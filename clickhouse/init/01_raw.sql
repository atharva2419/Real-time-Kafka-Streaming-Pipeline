-- Raw event storage for the cold path.
--
-- This file runs once, on first container start, from
-- /docker-entrypoint-initdb.d. The schema therefore lives in git rather than in
-- somebody's shell history. A wiped volume replays it.
--
-- The hot path (Redis) answers "what is happening now" over a 10-minute window.
-- This side keeps raw events for a week so they can be queried with SQL.

CREATE DATABASE IF NOT EXISTS wiki;

-- Every event the producer published, one row each.
--
-- Nullability is not decoration. Wikimedia log events (page moves, deletions,
-- user creation) carry a null `id`, and the producer nulls out any `timestamp`
-- that is not numeric so that event-time bucketing cannot silently misbucket a
-- string. A non-Nullable column for either would turn all of those into parse
-- failures, and they would land in the dead-letter table instead of here.
CREATE TABLE IF NOT EXISTS wiki.edits
(
    -- The Kafka message timestamp: always present, monotonic enough to
    -- partition and expire on. The payload's own clock is kept separately.
    ingested_at      DateTime64(3),
    event_time       Nullable(DateTime),

    wiki             LowCardinality(String),
    type             LowCardinality(String),
    title            Nullable(String),
    user             Nullable(String),
    id               Nullable(UInt64),
    bot              UInt8,

    -- Kept for deduplication, and because "which partition did this come from"
    -- is the first question when the two paths disagree.
    kafka_partition  UInt8,
    kafka_offset     UInt64
)
-- ClickHouse's Kafka engine is at-least-once: it inserts, then commits offsets,
-- so a crash or rebalance between the two redelivers messages. A replayed
-- message reproduces this sort key exactly, so ReplacingMergeTree collapses it
-- on merge. The key still leads with (wiki, ingested_at), which is what
-- analytical queries filter on - the dedup columns ride along at the end.
--
-- Dedup happens at merge time, so an exact count needs FINAL or a GROUP BY.
ENGINE = ReplacingMergeTree
PARTITION BY toDate(ingested_at)
ORDER BY (wiki, ingested_at, kafka_partition, kafka_offset)
TTL toDateTime(ingested_at) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- Messages the Kafka engine could not parse, captured through the _error and
-- _raw_message virtual columns (see 02_kafka_source.sql).
--
-- The producer already validates, so this table staying empty is the point of
-- it: an assertion that runs continuously rather than a place to look after
-- something has gone wrong.
CREATE TABLE IF NOT EXISTS wiki.edits_errors
(
    seen_at          DateTime DEFAULT now(),
    topic            LowCardinality(String),
    kafka_partition  UInt8,
    kafka_offset     UInt64,
    error            String,
    raw_message      String
)
ENGINE = MergeTree
PARTITION BY toDate(seen_at)
ORDER BY (seen_at, kafka_partition, kafka_offset)
TTL seen_at + INTERVAL 30 DAY;
