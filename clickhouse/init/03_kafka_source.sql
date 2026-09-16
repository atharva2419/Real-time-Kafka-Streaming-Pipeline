-- The cold path's ingest: ClickHouse consumes the same Kafka topic as the
-- aggregator, as an independent consumer group.
--
-- This file must sort LAST. Consumption starts the moment the view below
-- exists, and any materialized view on wiki.edits created after that point
-- misses every row already ingested - on first boot, the entire backfill.
--
--   wiki-edits ──► wiki.edits_queue (Kafka engine)
--                        │
--            ┌───────────┴────────────┐
--   parsed cleanly                parse failed
--            ▼                        ▼
--   wiki.edits_mv              wiki.edits_errors_mv
--            ▼                        ▼
--      wiki.edits              wiki.edits_errors
--
-- Nothing ever SELECTs from edits_queue directly. Reading a Kafka engine table
-- consumes from it and advances the group's offsets, so a curious query in the
-- SQL console would silently steal rows from the pipeline. The materialized
-- views are the only readers.

-- The shape of a message as the producer writes it: seven JSON fields.
--
-- The Nullable columns mirror 01_raw.sql for the same reason: log events carry
-- a null id, and the producer nulls a timestamp it cannot read as a number.
CREATE TABLE IF NOT EXISTS wiki.edits_queue
(
    id           Nullable(UInt64),
    `type`       String,
    title        Nullable(String),
    wiki         String,
    `user`       Nullable(String),
    `timestamp`  Nullable(Int64),
    bot          Bool
)
ENGINE = Kafka
SETTINGS
    -- The internal listener. `localhost:9092` would be ClickHouse's own
    -- container, where no broker exists.
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'wiki-edits',
    -- Must not be the aggregator's group. Joining `wiki-aggregator` would make
    -- Kafka split the partitions between the two, and each path would silently
    -- see only part of the stream.
    kafka_group_name = 'clickhouse-cold-path',
    kafka_format = 'JSONEachRow',
    -- One consumer covers all six partitions at ~35 events/s. More would only
    -- add threads; the ceiling is one per partition.
    kafka_num_consumers = 1,
    -- A message that fails to parse becomes a row with _error and _raw_message
    -- set, instead of an exception that stalls the whole consumer.
    kafka_handle_error_mode = 'stream';

-- Clean rows into the events table.
CREATE MATERIALIZED VIEW IF NOT EXISTS wiki.edits_mv TO wiki.edits AS
SELECT
    -- The broker's timestamp. Nullable in the engine's schema, so fall back to
    -- now rather than reject the row.
    coalesce(_timestamp_ms, now64(3))  AS ingested_at,
    toDateTime(`timestamp`)            AS event_time,
    wiki,
    `type`,
    title,
    `user`,
    id,
    toUInt8(bot)                       AS bot,
    toUInt8(_partition)                AS kafka_partition,
    _offset                            AS kafka_offset
FROM wiki.edits_queue
WHERE length(_error) = 0;

-- Anything that failed to parse into the dead-letter table, with the raw bytes
-- and the parser's own message, so it can be read rather than guessed at.
--
-- The two WHERE clauses are exact complements: every message lands in exactly
-- one of the two tables.
CREATE MATERIALIZED VIEW IF NOT EXISTS wiki.edits_errors_mv TO wiki.edits_errors AS
SELECT
    now()                AS seen_at,
    _topic               AS topic,
    toUInt8(_partition)  AS kafka_partition,
    _offset              AS kafka_offset,
    _error               AS error,
    _raw_message         AS raw_message
FROM wiki.edits_queue
WHERE length(_error) > 0;
