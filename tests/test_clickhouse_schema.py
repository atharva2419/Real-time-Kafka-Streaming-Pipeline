"""
Contract tests for the ClickHouse schema.

The init SQL runs once, on an empty volume, with no migration tool watching it.
These pin the decisions that are invisible until they are wrong: a non-Nullable
column that sends every log event to the dead-letter table, a dedup key that
does not actually dedup, a missing TTL that lets the disk fill for weeks.
"""

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
INIT = ROOT / "clickhouse" / "init"
COMPOSE = ROOT / "docker-compose.yml"


def sql(name: str) -> str:
    """One init file with comments stripped, so prose can't satisfy a match."""
    text = (INIT / name).read_text(encoding="utf-8")
    return re.sub(r"--[^\n]*", "", text)


def statement(name: str, table: str) -> str:
    """The CREATE TABLE body for one table, up to its terminating semicolon."""
    body = sql(name)
    start = body.index(f"CREATE TABLE IF NOT EXISTS {table}")
    return body[start: body.index(";", start)]


def clickhouse_service() -> str:
    text = COMPOSE.read_text(encoding="utf-8")
    match = re.search(r"^  clickhouse:\n(.*?)(?=^  [\w-]+:\n|^\S)", text, re.S | re.M)
    assert match, "clickhouse service not found in docker-compose.yml"
    return match.group(1)


# ---------------------------------------------------------------------------
# How the container is wired, which is where the first two hours went
# ---------------------------------------------------------------------------

def test_config_files_are_mounted_individually_not_as_directories():
    """
    Regression: mounting ./clickhouse/config.d over the server's config.d hides
    the image's own docker_related_config.xml - the file that sets listen_host
    to the wildcard. ClickHouse then binds to localhost inside the container and
    nothing can reach it, not the gateway and not the host, while the container
    still reports healthy because clickhouse-client connects locally.
    """
    service = clickhouse_service()
    assert "/etc/clickhouse-server/config.d:ro" not in service
    assert "/etc/clickhouse-server/users.d:ro" not in service
    assert "config.d/prometheus.xml:/etc/clickhouse-server/config.d/prometheus.xml:ro" in service


def test_the_entrypoint_user_rewrite_is_skipped():
    """
    Without this the image's entrypoint writes users.d/default-user.xml to
    restrict `default` to localhost inside the container, which would cut off
    the API and the test suite - and it cannot write there at all while that
    directory is read-only, so the container just crash-loops.
    """
    assert 'CLICKHOUSE_SKIP_USER_SETUP: "1"' in clickhouse_service()


def test_init_creates_the_database():
    assert "CREATE DATABASE IF NOT EXISTS wiki" in sql("01_raw.sql")


def test_init_is_rerunnable():
    """The volume outlives a container; every statement has to tolerate that."""
    for path in sorted(INIT.glob("*.sql")):
        body = sql(path.name)
        creates = re.findall(
            r"CREATE (?:TABLE|DATABASE|MATERIALIZED VIEW)(?: IF NOT EXISTS)?", body
        )
        assert creates, f"no CREATE statements in {path.name}"
        for create in creates:
            assert "IF NOT EXISTS" in create, f"{path.name}: {create}"


# ---------------------------------------------------------------------------
# The raw table
# ---------------------------------------------------------------------------

def test_nullable_fields_match_what_the_producer_actually_sends():
    """
    Wikimedia log events carry a null id, and the producer nulls a timestamp it
    cannot read as a number. A non-Nullable column for either would send all of
    those to the dead-letter table instead of the events table - which is how
    2.7% of the stream once went missing on the producer side.
    """
    edits = statement("01_raw.sql", "wiki.edits")
    for column in ("event_time", "title", "user", "id"):
        assert re.search(rf"\b{column}\s+Nullable\(", edits), f"{column} must be Nullable"


def test_required_fields_are_not_nullable():
    """`type` and `wiki` are what everything groups by; the producer guarantees them."""
    edits = statement("01_raw.sql", "wiki.edits")
    for column in ("wiki", "type"):
        assert re.search(rf"\b{column}\s+LowCardinality\(String\)", edits), column


def test_dedup_key_ends_with_the_kafka_coordinates():
    """
    ClickHouse's Kafka engine is at-least-once, so a redelivered message has to
    collapse. ReplacingMergeTree dedups on the whole sorting key, and only
    (partition, offset) makes a replay identical to the original.
    """
    edits = statement("01_raw.sql", "wiki.edits")
    assert "ReplacingMergeTree" in edits

    order_by = re.search(r"ORDER BY \(([^)]+)\)", edits)
    assert order_by, "wiki.edits has no ORDER BY"
    columns = [c.strip() for c in order_by.group(1).split(",")]
    assert columns[-2:] == ["kafka_partition", "kafka_offset"], columns
    # Queries filter on wiki and time, so the key has to lead with them.
    assert columns[0] == "wiki"


def test_raw_events_expire():
    """Without a TTL this grows without bound; with it, disk use is flat."""
    edits = statement("01_raw.sql", "wiki.edits")
    assert re.search(r"TTL .*INTERVAL 7 DAY", edits)


def test_partitioned_by_day_so_ttl_drops_whole_partitions():
    edits = statement("01_raw.sql", "wiki.edits")
    assert "PARTITION BY toDate(ingested_at)" in edits


def test_ingest_time_is_the_kafka_clock_not_the_payload_clock():
    """
    Partitioning and expiry run on a clock that is always present and roughly
    monotonic. The payload's own timestamp lags and can be null, so it is stored
    but never used for either.
    """
    edits = statement("01_raw.sql", "wiki.edits")
    assert re.search(r"ingested_at\s+DateTime64\(3\)", edits)
    assert "PARTITION BY toDate(ingested_at)" in edits
    assert "TTL toDateTime(ingested_at)" in edits


# ---------------------------------------------------------------------------
# The dead-letter table
# ---------------------------------------------------------------------------

def test_dead_letter_table_keeps_the_raw_message_and_the_error():
    errors = statement("01_raw.sql", "wiki.edits_errors")
    for column in ("raw_message", "error", "kafka_partition", "kafka_offset"):
        assert column in errors, column


def test_dead_letter_table_expires_too():
    errors = statement("01_raw.sql", "wiki.edits_errors")
    assert re.search(r"TTL .*INTERVAL 30 DAY", errors)


# ---------------------------------------------------------------------------
# Ingest: the Kafka engine table and its materialized views
# ---------------------------------------------------------------------------

def view(name: str, view_name: str) -> str:
    body = sql(name)
    start = body.index(f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}")
    return body[start: body.index(";", start)]


def setting(queue: str, key: str) -> str:
    match = re.search(rf"{key}\s*=\s*'?([^',\s]+)'?", queue)
    assert match, f"{key} not set"
    return match.group(1)


def test_clickhouse_does_not_join_the_aggregators_consumer_group():
    """
    The most damaging misconfiguration available here, and it fails silently.
    Sharing `wiki-aggregator` would make Kafka split the six partitions between
    the two consumers, and each path would quietly see only part of the stream.
    Measured with separate groups: both own all six partitions.
    """
    from pipeline import config

    queue = statement("03_kafka_source.sql", "wiki.edits_queue")
    assert setting(queue, "kafka_group_name") != config.CONSUMER_GROUP


def test_reads_the_topic_the_producer_writes():
    from pipeline import config

    queue = statement("03_kafka_source.sql", "wiki.edits_queue")
    assert setting(queue, "kafka_topic_list") == config.KAFKA_TOPIC


def test_broker_address_is_the_internal_listener():
    """localhost:9092 inside the ClickHouse container is ClickHouse itself."""
    queue = statement("03_kafka_source.sql", "wiki.edits_queue")
    assert setting(queue, "kafka_broker_list") == "kafka:29092"


def test_parse_errors_are_streamed_not_thrown():
    """
    The default throws on a bad message and stalls the consumer. `stream` turns
    it into a row carrying _error and _raw_message instead.
    """
    queue = statement("03_kafka_source.sql", "wiki.edits_queue")
    assert setting(queue, "kafka_handle_error_mode") == "stream"


def test_queue_nullability_matches_the_raw_table():
    """
    A field that is Nullable in wiki.edits but not in the queue would still fail
    at parse time - measured: 16,813 null-id log events landed only because both
    agree.
    """
    queue = statement("03_kafka_source.sql", "wiki.edits_queue")
    for column in ("id", "title", "user", "timestamp"):
        assert re.search(rf"`?{column}`?\s+Nullable\(", queue), f"{column} must be Nullable"


def test_every_message_lands_in_exactly_one_table():
    """The two views' filters are complements, so nothing is dropped or doubled."""
    clean = view("03_kafka_source.sql", "wiki.edits_mv")
    errors = view("03_kafka_source.sql", "wiki.edits_errors_mv")
    assert "TO wiki.edits AS" in clean and "WHERE length(_error) = 0" in clean
    assert "TO wiki.edits_errors AS" in errors and "WHERE length(_error) > 0" in errors


def test_only_materialized_views_read_the_queue():
    """
    Reading a Kafka engine table consumes from it and advances the offsets, so
    any other SELECT would silently steal rows from the pipeline.
    """
    for path in sorted(INIT.glob("*.sql")):
        for stmt in sql(path.name).split(";"):
            if "FROM wiki.edits_queue" in stmt:
                assert "CREATE MATERIALIZED VIEW" in stmt, f"{path.name} reads the queue directly"


def test_ingest_time_comes_from_the_broker():
    clean = view("03_kafka_source.sql", "wiki.edits_mv")
    assert "_timestamp_ms" in clean
    assert "_partition" in clean and "_offset" in clean


# ---------------------------------------------------------------------------
# Rollups
# ---------------------------------------------------------------------------

def test_ingest_source_is_created_after_every_view_on_raw_events():
    """
    A materialized view only sees inserts made after it exists, and the Kafka
    source starts consuming the moment it is created - on first boot, a backfill
    of everything Kafka holds. Any view on wiki.edits created later misses all
    of it for good. Init files run in name order, so the ingest file must sort
    after every file that defines such a view.
    """
    files = sorted(INIT.glob("*.sql"))
    ingest = [f for f in files if "ENGINE = Kafka" in sql(f.name)]
    assert len(ingest) == 1, ingest

    views_on_raw = [
        f for f in files
        if re.search(r"CREATE MATERIALIZED VIEW[^;]*FROM wiki\.edits\b(?!_)", sql(f.name))
    ]
    assert views_on_raw, "no views on wiki.edits found"
    for f in views_on_raw:
        assert f.name < ingest[0].name, f"{f.name} would miss the backfill from {ingest[0].name}"


def test_daily_rollup_is_chained_off_the_minute_rollup():
    """A view fires on inserts into its own source; the 1d view must read edits_1m."""
    daily = view("02_rollups.sql", "wiki.edits_1d_mv")
    assert "FROM wiki.edits_1m" in daily
    assert "uniqMergeState(editors)" in daily


def test_minute_rollup_keeps_a_mergeable_sketch_of_editors():
    minute = view("02_rollups.sql", "wiki.edits_1m_mv")
    assert "uniqState(`user`)" in minute


def test_editor_state_types_match_across_both_rollups():
    """uniqMergeState must produce exactly the type the daily table declares."""
    for table in ("wiki.edits_1m", "wiki.edits_1d"):
        body = statement("02_rollups.sql", table)
        assert "editors    AggregateFunction(uniq, Nullable(String))" in body, table


def test_minute_rollup_expires_and_daily_rollup_is_kept():
    assert re.search(r"TTL .*INTERVAL 90 DAY", statement("02_rollups.sql", "wiki.edits_1m"))
    assert "TTL" not in statement("02_rollups.sql", "wiki.edits_1d")


def test_rollup_keys_are_time_then_wiki():
    """AggregatingMergeTree merges on the sorting key, so the key is the grain."""
    assert "ORDER BY (minute, wiki)" in statement("02_rollups.sql", "wiki.edits_1m")
    assert "ORDER BY (day, wiki)" in statement("02_rollups.sql", "wiki.edits_1d")
