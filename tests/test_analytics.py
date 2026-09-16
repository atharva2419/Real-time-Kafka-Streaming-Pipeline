"""
Tests for the analytics queries.

The first half needs no database: it pins which requests are refused and which
table each one reads. The second half runs the queries against a real
ClickHouse, in a throwaway database loaded from the real schema.
"""

import asyncio

import pytest

from pipeline import analytics, config
from pipeline.analytics import AnalyticsError, ColdPath, ColdPathUnavailable
from tests import _clickhouse as ch

DB = "wiki_test_analytics"


# ===========================================================================
# Validation - no database needed
# ===========================================================================

@pytest.mark.parametrize("bad", ["5y", "", "24 h", "1w"])
def test_unknown_range_is_refused_with_the_allowed_values(bad):
    with pytest.raises(AnalyticsError) as exc:
        analytics.timeline_query(bad, "1h", "wiki")
    assert "allowed: 1h, 6h, 24h, 7d, 30d, 90d" in str(exc.value)


def test_unknown_bucket_is_refused():
    with pytest.raises(AnalyticsError, match="unknown bucket"):
        analytics.timeline_query("24h", "10s", "wiki")


def test_bucket_wider_than_range_is_refused():
    with pytest.raises(AnalyticsError, match="wider than range"):
        analytics.timeline_query("1h", "1d", "wiki")


def test_too_many_points_is_refused_with_a_suggestion():
    with pytest.raises(AnalyticsError) as exc:
        analytics.timeline_query("30d", "1m", "wiki")
    assert "43200 points" in str(exc.value)
    assert "coarser bucket" in str(exc.value)


def test_a_full_day_at_one_minute_is_exactly_the_limit():
    analytics.timeline_query("24h", "1m", "wiki")  # 1440 points, allowed


@pytest.mark.parametrize(
    "range_,bucket,table",
    [("24h", "1m", "edits_1m"), ("7d", "1h", "edits_1m"), ("90d", "1d", "edits_1d")],
)
def test_daily_buckets_read_the_daily_rollup(range_, bucket, table):
    query = analytics.timeline_query(range_, bucket, "wiki")
    assert f"{{db:Identifier}}.{table}" in query.sql


def test_top_editors_refuses_ranges_past_raw_retention():
    """Raw events are kept 7 days; a 30-day answer would be silently partial."""
    with pytest.raises(AnalyticsError, match="kept for 7 days"):
        analytics.top_editors_query("30d", 10, "wiki")


def test_top_wikis_can_look_back_as_far_as_the_minute_rollup():
    analytics.top_wikis_query("90d", 10, "wiki")


@pytest.mark.parametrize("bad", [0, -1, 51, 1000])
def test_limit_is_bounded(bad):
    with pytest.raises(AnalyticsError, match="limit must be between 1 and 50"):
        analytics.top_wikis_query("24h", bad, "wiki")


@pytest.mark.parametrize(
    "build",
    [
        lambda db: analytics.timeline_query("24h", "1h", db),
        lambda db: analytics.top_wikis_query("24h", 10, db),
        lambda db: analytics.top_editors_query("24h", 10, db),
        lambda db: analytics.freshness_query(db),
    ],
)
def test_the_database_is_bound_never_formatted_into_sql(build):
    query = build("not_a_real_db")
    assert "{db:Identifier}" in query.sql
    assert "not_a_real_db" not in query.sql
    assert query.parameters["db"] == "not_a_real_db"


def test_user_supplied_values_travel_as_parameters():
    query = analytics.top_editors_query("6h", 7, "wiki")
    assert query.parameters == {"db": "wiki", "secs": 21_600, "limit": 7}
    assert "21600" not in query.sql and " 7" not in query.sql


# ===========================================================================
# Against a real ClickHouse
# ===========================================================================

@pytest.fixture(scope="module")
def clickhouse():
    ch.require_clickhouse()
    ch.create_schema(DB)
    yield
    ch.drop(DB)


@pytest.fixture
def db(clickhouse):
    ch.truncate(DB)
    yield


def cold() -> ColdPath:
    return ColdPath(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=DB,
    )


def run(query: analytics.Query) -> list[dict]:
    async def go():
        path = cold()
        try:
            return await path.rows(query)
        finally:
            await path.close()
    return asyncio.run(go())


def insert(rows: list[dict]) -> None:
    """Rows placed relative to now, so the rolling-window filters include them."""
    values = []
    for i, row in enumerate(rows):
        user = "NULL" if row.get("user") is None else f"'{row['user']}'"
        values.append(
            f"(now64(3) - toIntervalSecond({row.get('ago', 0)}), NULL, "
            f"'{row.get('wiki', 'enwiki')}', '{row.get('type', 'edit')}', NULL, {user}, "
            f"{i}, {int(row.get('bot', False))}, {row.get('partition', 0)}, "
            f"{row.get('offset', i)})"
        )
    ch.q(
        f"INSERT INTO {DB}.edits (ingested_at, event_time, wiki, type, title, user, id, "
        "bot, kafka_partition, kafka_offset) VALUES " + ", ".join(values)
    )


def test_timeline_totals_what_was_inserted(db):
    insert([{"ago": 30, "bot": True}, {"ago": 90}, {"ago": 150, "bot": True}])

    points = run(analytics.timeline_query("1h", "1m", DB))

    assert sum(p["edits"] for p in points) == 3
    assert sum(p["bot_edits"] for p in points) == 2
    assert [p["t"] for p in points] == sorted(p["t"] for p in points)
    assert all(p["t"] % 60 == 0 for p in points)


def test_timeline_excludes_events_outside_the_range(db):
    insert([{"ago": 60}, {"ago": 2 * 3600}])

    inside = run(analytics.timeline_query("1h", "5m", DB))
    assert sum(p["edits"] for p in inside) == 1


def test_daily_timeline_comes_through_the_chained_rollup(db):
    insert([{"ago": 10, "user": "alice"}, {"ago": 20, "user": "alice"}, {"ago": 30, "user": "bob"}])

    points = run(analytics.timeline_query("7d", "1d", DB))

    assert sum(p["edits"] for p in points) == 3
    assert max(p["editors"] for p in points) == 2


def test_top_wikis_are_ordered_and_limited(db):
    insert(
        [{"ago": 5, "wiki": "enwiki"}] * 3
        + [{"ago": 5, "wiki": "dewiki"}] * 5
        + [{"ago": 5, "wiki": "frwiki"}]
    )

    wikis = run(analytics.top_wikis_query("1h", 2, DB))

    assert [(w["wiki"], w["edits"]) for w in wikis] == [("dewiki", 5), ("enwiki", 3)]


def test_top_editors_do_not_double_count_a_redelivered_message(db):
    """
    The contrast with the rollups: the same Kafka message inserted twice is
    counted twice by edits_1m, but once here, because this counts distinct
    (partition, offset) pairs rather than rows.
    """
    insert([
        {"ago": 5, "user": "alice", "partition": 3, "offset": 42},
        {"ago": 5, "user": "alice", "partition": 3, "offset": 42},  # redelivery
        {"ago": 5, "user": "alice", "partition": 3, "offset": 43},
        {"ago": 5, "user": "bob", "partition": 1, "offset": 7},
    ])

    editors = run(analytics.top_editors_query("1h", 10, DB))
    assert [(e["user"], e["edits"]) for e in editors] == [("alice", 2), ("bob", 1)]

    rollup = ch.q(f"SELECT sum(edits) FROM {DB}.edits_1m")
    assert rollup == "4"


def test_anonymous_edits_are_left_out_of_top_editors(db):
    insert([{"ago": 5, "user": None}] * 3 + [{"ago": 5, "user": "alice"}])

    editors = run(analytics.top_editors_query("1h", 10, DB))
    assert [e["user"] for e in editors] == ["alice"]


def test_freshness_reports_recent_data(db):
    insert([{"ago": 3}])
    age = run(analytics.freshness_query(DB))[0]["age_seconds"]
    assert 0 <= age < 30


def test_freshness_is_null_when_there_is_nothing(db):
    assert run(analytics.freshness_query(DB))[0]["age_seconds"] is None


# ===========================================================================
# Unavailability
# ===========================================================================

def test_an_unreachable_clickhouse_is_reported_not_crashed():
    """A dead port must surface as ColdPathUnavailable, which the API maps to 503."""
    async def go():
        path = ColdPath(host="localhost", port=8999, username="default", password="",
                        database="wiki", connect_timeout=1)
        try:
            await path.rows(analytics.freshness_query("wiki"))
        finally:
            await path.close()

    with pytest.raises(ColdPathUnavailable, match="cannot reach ClickHouse"):
        asyncio.run(go())


# ===========================================================================
# Failures after a connection exists
# ===========================================================================

class FakeClient:
    """Stands in for a connected client whose next query fails a given way."""

    def __init__(self, error: Exception) -> None:
        self.error = error
        self.closed = False

    async def query(self, sql, parameters=None):
        raise self.error

    async def close(self):
        self.closed = True


def connected_path(error: Exception) -> tuple[ColdPath, FakeClient]:
    path = ColdPath(host="unused", port=0, username="", password="", database="wiki")
    client = FakeClient(error)
    path._client = client
    return path, client


def test_connection_lost_mid_query_is_unavailable_and_reconnects_next_time():
    """
    Regression for an ordering bug. clickhouse-connect follows DB-API, where
    OperationalError subclasses DatabaseError, so a handler that re-raised
    DatabaseError first turned every mid-query outage into a 500. The dead-port
    test cannot catch this: it fails while connecting, before any query runs.
    """
    from clickhouse_connect.driver.exceptions import OperationalError

    path, client = connected_path(OperationalError("connection reset"))

    with pytest.raises(ColdPathUnavailable, match="unreachable"):
        asyncio.run(path.rows(analytics.freshness_query("wiki")))

    assert client.closed, "a failed client must be closed"
    assert path._client is None, "and forgotten, so the next request reconnects"


def test_a_rejected_query_is_a_bug_not_an_outage():
    """The server answered and said no: that must stay loud, not become a 503."""
    from clickhouse_connect.driver.exceptions import DatabaseError

    path, client = connected_path(DatabaseError("Unknown column"))

    with pytest.raises(DatabaseError):
        asyncio.run(path.rows(analytics.freshness_query("wiki")))

    assert not client.closed, "a healthy connection is kept after a query error"
