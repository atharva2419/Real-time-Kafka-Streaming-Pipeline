-- Pre-aggregated rollups over wiki.edits, so a chart over a day or a month
-- reads thousands of rows instead of millions.
--
--   wiki.edits ──► wiki.edits_1m_mv ──► wiki.edits_1m ──► wiki.edits_1d_mv ──► wiki.edits_1d
--   raw, 7 days       per minute × wiki, 90 days              per day × wiki, kept
--
-- ORDERING MATTERS. A materialized view only sees inserts made after it exists,
-- and 03_kafka_source.sql starts consuming the moment it runs - on first boot
-- that is a backfill of everything Kafka still holds. If these views came
-- later, every row in that backfill would miss the rollups for good. This file
-- therefore sorts before the ingest source.
--
-- The 1d view is chained off the 1m table, not off raw events: a view fires on
-- inserts into its source table, and inserts into edits_1m are what the 1m view
-- produces.
--
-- One limit worth stating plainly. The raw table deduplicates Kafka's
-- at-least-once redeliveries at merge time, but a view sees every inserted
-- block before any merge happens, so a redelivered message is counted twice
-- here and cannot be un-counted later - a sum has no memory of what it summed.
-- Editor counts are unaffected (uniq of the same user twice is still one).
-- Measured duplicates so far: zero. The hot-versus-cold comparison is what
-- would show it if that changed.

-- Per minute, per wiki.
--
-- Plain sums use SimpleAggregateFunction: a merge just adds them, with no state
-- to store. Distinct editors need a real aggregate state, because you cannot
-- add two distinct counts together - a user active in two minutes would be
-- counted twice. uniqState keeps a compact sketch that merges correctly.
CREATE TABLE IF NOT EXISTS wiki.edits_1m
(
    minute     DateTime,
    wiki       LowCardinality(String),
    edits      SimpleAggregateFunction(sum, UInt64),
    bot_edits  SimpleAggregateFunction(sum, UInt64),
    new_pages  SimpleAggregateFunction(sum, UInt64),
    editors    AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, wiki)
TTL minute + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS wiki.edits_1m_mv TO wiki.edits_1m AS
SELECT
    toStartOfMinute(ingested_at)  AS minute,
    wiki,
    count()                       AS edits,
    countIf(bot = 1)              AS bot_edits,
    countIf(`type` = 'new')       AS new_pages,
    uniqState(`user`)             AS editors
FROM wiki.edits
GROUP BY minute, wiki;

-- Per day, per wiki, kept indefinitely: a year of it is a few hundred thousand
-- rows at most.
CREATE TABLE IF NOT EXISTS wiki.edits_1d
(
    day        Date,
    wiki       LowCardinality(String),
    edits      SimpleAggregateFunction(sum, UInt64),
    bot_edits  SimpleAggregateFunction(sum, UInt64),
    new_pages  SimpleAggregateFunction(sum, UInt64),
    editors    AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, wiki);

-- Reads the block just inserted into edits_1m. Those rows are unmerged
-- per-minute states, so they are summed and their sketches merged again
-- (uniqMergeState) rather than recomputed from raw events.
CREATE MATERIALIZED VIEW IF NOT EXISTS wiki.edits_1d_mv TO wiki.edits_1d AS
SELECT
    toDate(minute)             AS day,
    wiki,
    sum(edits)                 AS edits,
    sum(bot_edits)             AS bot_edits,
    sum(new_pages)             AS new_pages,
    uniqMergeState(editors)    AS editors
FROM wiki.edits_1m
GROUP BY day, wiki;
