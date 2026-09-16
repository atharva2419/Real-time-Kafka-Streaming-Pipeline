"""
Rollup tests against a real ClickHouse.

The schema files are loaded into a throwaway database with synthetic rows, so
these exercise ClickHouse's actual materialized-view and aggregate-state
behaviour rather than a model of it. 03_kafka_source.sql is deliberately never
loaded: it would start a real consumer against the real topic.

Skipped automatically when ClickHouse is not reachable: `docker compose up -d clickhouse`.
"""

import pathlib
import re

import pytest
import requests

from pipeline import config

INIT = pathlib.Path(__file__).resolve().parents[1] / "clickhouse" / "init"
DB = "wiki_test"
CH = f"http://{config.CLICKHOUSE_HOST}:{config.CLICKHOUSE_PORT}/"


def q(sql: str) -> str:
    response = requests.post(CH, data=sql.encode(), timeout=30)
    if response.status_code != 200:
        raise AssertionError(f"ClickHouse error for:\n{sql}\n\n{response.text}")
    return response.text.strip()


def statements(filename: str) -> list[str]:
    """One init file, retargeted at the test database, split into statements."""
    text = re.sub(r"--[^\n]*", "", (INIT / filename).read_text(encoding="utf-8"))
    text = re.sub(r"DATABASE IF NOT EXISTS wiki\b", f"DATABASE IF NOT EXISTS {DB}", text)
    text = re.sub(r"\bwiki\.", f"{DB}.", text)
    return [s.strip() for s in text.split(";") if s.strip()]


@pytest.fixture(scope="module")
def clickhouse():
    try:
        requests.get(CH + "ping", timeout=1).raise_for_status()
    except requests.RequestException as exc:
        pytest.skip(
            f"ClickHouse not reachable at {CH} ({type(exc).__name__}) - "
            "run `docker compose up -d clickhouse`",
            allow_module_level=True,
        )

    q(f"DROP DATABASE IF EXISTS {DB} SYNC")
    for filename in ("01_raw.sql", "02_rollups.sql"):
        for stmt in statements(filename):
            q(stmt)
    yield
    q(f"DROP DATABASE IF EXISTS {DB} SYNC")


@pytest.fixture
def db(clickhouse):
    for table in ("edits", "edits_1m", "edits_1d"):
        q(f"TRUNCATE TABLE {DB}.{table}")
    yield


def insert(rows: list[dict]) -> None:
    """One INSERT per call, so each call is one block - like one Kafka flush."""
    values = []
    for i, row in enumerate(rows):
        user = "NULL" if row.get("user") is None else f"'{row['user']}'"
        template = (
            "('{at}', NULL, '{wiki}', '{type}', NULL, {user}, {id}, {bot}, {part}, {offset})"
        )
        values.append(
            template.format(
                at=row["at"],
                wiki=row.get("wiki", "enwiki"),
                type=row.get("type", "edit"),
                user=user,
                id=row.get("id", i),
                bot=int(row.get("bot", False)),
                part=row.get("partition", 0),
                offset=row.get("offset", i),
            )
        )
    q(
        f"INSERT INTO {DB}.edits (ingested_at, event_time, wiki, type, title, user, id, "
        f"bot, kafka_partition, kafka_offset) VALUES " + ", ".join(values)
    )


def one(sql: str) -> list[str]:
    return q(sql).split("\t")


# ---------------------------------------------------------------------------
# The minute rollup
# ---------------------------------------------------------------------------

def test_minute_rollup_counts_edits_bots_and_new_pages(db):
    insert([
        {"at": "2026-09-01 10:00:05", "bot": True},
        {"at": "2026-09-01 10:00:20", "bot": False, "type": "new"},
        {"at": "2026-09-01 10:00:59", "bot": True},
        {"at": "2026-09-01 10:01:00", "bot": False},
    ])

    assert one(
        f"SELECT sum(edits), sum(bot_edits), sum(new_pages) FROM {DB}.edits_1m "
        "WHERE minute = '2026-09-01 10:00:00'"
    ) == ["3", "2", "1"]
    assert one(
        f"SELECT sum(edits) FROM {DB}.edits_1m WHERE minute = '2026-09-01 10:01:00'"
    ) == ["1"]


def test_minute_rollup_keeps_wikis_apart(db):
    insert([
        {"at": "2026-09-01 10:00:05", "wiki": "enwiki"},
        {"at": "2026-09-01 10:00:06", "wiki": "enwiki"},
        {"at": "2026-09-01 10:00:07", "wiki": "dewiki"},
    ])

    rows = q(
        f"SELECT wiki, sum(edits) FROM {DB}.edits_1m GROUP BY wiki ORDER BY wiki"
    ).splitlines()
    assert rows == ["dewiki\t1", "enwiki\t2"]


def test_separate_blocks_for_one_minute_stay_unmerged_until_grouped(db):
    """
    Two Kafka flushes landing in the same minute produce two physical rows for
    one key, and they stay two until a background merge. A query that forgets
    GROUP BY would double-count; the grouped sum never does. Measured on the
    live table: 29,743 physical rows for 29,686 keys.
    """
    q(f"SYSTEM STOP MERGES {DB}.edits_1m")
    try:
        insert([{"at": "2026-09-01 10:00:05"}])
        insert([{"at": "2026-09-01 10:00:30", "offset": 99}])

        assert q(f"SELECT count() FROM {DB}.edits_1m") == "2"
        assert q(f"SELECT sum(edits) FROM {DB}.edits_1m GROUP BY minute, wiki") == "2"
    finally:
        q(f"SYSTEM START MERGES {DB}.edits_1m")


# ---------------------------------------------------------------------------
# Distinct editors: why the rollup carries a sketch and not a number
# ---------------------------------------------------------------------------

def test_an_editor_active_in_two_minutes_is_counted_once_for_the_day(db):
    """
    You cannot add two distinct counts: alice in 10:00 and alice in 10:01 would
    sum to two editors. uniqState keeps a sketch per minute, and merging the
    sketches counts her once.
    """
    insert([
        {"at": "2026-09-01 10:00:05", "user": "alice"},
        {"at": "2026-09-01 10:00:10", "user": "bob"},
        {"at": "2026-09-01 10:01:05", "user": "alice"},
    ])

    per_minute = q(
        f"SELECT uniqMerge(editors) FROM {DB}.edits_1m GROUP BY minute ORDER BY minute"
    ).splitlines()
    assert per_minute == ["2", "1"]

    naive_sum = sum(int(n) for n in per_minute)
    merged = q(f"SELECT uniqMerge(editors) FROM {DB}.edits_1m")
    assert naive_sum == 3
    assert merged == "2"


def test_anonymous_edits_are_not_counted_as_an_editor(db):
    insert([
        {"at": "2026-09-01 10:00:05", "user": None},
        {"at": "2026-09-01 10:00:06", "user": None},
        {"at": "2026-09-01 10:00:07", "user": "alice"},
    ])

    assert one(
        f"SELECT sum(edits), uniqMerge(editors) FROM {DB}.edits_1m"
    ) == ["3", "1"]


# ---------------------------------------------------------------------------
# The chained daily rollup
# ---------------------------------------------------------------------------

def test_daily_rollup_is_fed_by_the_minute_rollup(db):
    insert([
        {"at": "2026-09-01 10:00:05", "bot": True, "user": "alice"},
        {"at": "2026-09-01 23:59:59", "type": "new", "user": "alice"},
        {"at": "2026-09-02 00:00:01", "user": "bob"},
    ])

    days = q(
        f"SELECT day, sum(edits), sum(bot_edits), sum(new_pages), uniqMerge(editors) "
        f"FROM {DB}.edits_1d GROUP BY day ORDER BY day"
    ).splitlines()
    assert days == ["2026-09-01\t2\t1\t1\t1", "2026-09-02\t1\t0\t0\t1"]


def test_daily_totals_equal_minute_totals(db):
    insert([
        {"at": f"2026-09-01 10:{m:02d}:00", "user": f"u{m % 4}", "offset": m}
        for m in range(60)
    ])

    minute = one(f"SELECT sum(edits), uniqMerge(editors) FROM {DB}.edits_1m")
    daily = one(f"SELECT sum(edits), uniqMerge(editors) FROM {DB}.edits_1d")
    assert minute == daily == ["60", "4"]


# ---------------------------------------------------------------------------
# The known limit, pinned so it cannot be forgotten
# ---------------------------------------------------------------------------

def test_a_redelivered_message_is_deduplicated_in_raw_but_counted_twice_in_rollups(db):
    """
    Documents a real limitation rather than hiding it. The Kafka engine is
    at-least-once; a redelivery reproduces the same (partition, offset), and the
    raw ReplacingMergeTree collapses it on merge. But a materialized view sees
    each inserted block before any merge, so the rollups count it twice, and a
    sum cannot be un-summed. Editor counts survive (uniq of one user is one).

    Measured duplicates on the live stream so far: zero. If this test ever
    starts failing because the rollups deduplicate, update the docs too.
    """
    message = {"at": "2026-09-01 10:00:05", "user": "alice", "partition": 3, "offset": 42}
    insert([message])
    insert([message])  # the same Kafka message, delivered again

    assert q(f"SELECT count() FROM {DB}.edits FINAL") == "1"
    assert one(f"SELECT sum(edits), uniqMerge(editors) FROM {DB}.edits_1m") == ["2", "1"]
