"""
Sink tests against a real Redis.

These run the actual Lua script rather than a fake, because the property under
test - that a rewritten window overwrites in place - depends on Redis's own
ZADD/HSET semantics and on the trim staying consistent across two keys. A fake
would only be testing my model of Redis.

Skipped automatically when Redis is not reachable: `docker compose up -d redis`.
"""

import json

import pytest

redis = pytest.importorskip("redis")

from pipeline import config  # noqa: E402
from pipeline.consumer.sink import RedisSink, history_point  # noqa: E402
from pipeline.consumer.window import TumblingWindow  # noqa: E402

KEYS = (config.HISTORY_KEY, config.HISTORY_DATA_KEY, config.LATEST_KEY)


@pytest.fixture(scope="session")
def redis_url():
    """
    Probe Redis once per session with a short timeout.

    Per-test probing meant a developer without Redis running waited out one
    connect timeout per test - about a minute for this module alone.
    """
    r = redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=1,
    )
    try:
        r.ping()
    except redis.RedisError as exc:
        pytest.skip(
            f"Redis not reachable on {config.REDIS_HOST}:{config.REDIS_PORT} "
            f"({type(exc).__name__}) - run `docker compose up -d redis`",
            allow_module_level=True,
        )
    finally:
        r.close()
    return (config.REDIS_HOST, config.REDIS_PORT)


@pytest.fixture
def client(redis_url):
    host, port = redis_url
    r = redis.Redis(host=host, port=port, decode_responses=True)
    for key in KEYS:
        r.delete(key)
    yield r
    for key in KEYS:
        r.delete(key)
    r.close()


@pytest.fixture
def sink(client):
    return RedisSink(client)


def window(start, total, bot=0):
    w = TumblingWindow(start=start, size=5)
    for i in range(total):
        w.add({"wiki": "enwiki", "user": "alice", "bot": i < bot, "type": "edit"})
    return w.snapshot()


def history(client):
    """Read back the history the way the API does, oldest first."""
    members = client.zrange(config.HISTORY_KEY, 0, -1)
    if not members:
        return []
    return [json.loads(p) for p in client.hmget(config.HISTORY_DATA_KEY, members)]


# ---------------------------------------------------------------------------
# Basic writes
# ---------------------------------------------------------------------------

def test_windows_are_stored_in_time_order(sink, client):
    sink.write([window(100, 3), window(105, 7), window(110, 1)])
    assert [p["t"] for p in history(client)] == [100, 105, 110]
    assert [p["total"] for p in history(client)] == [3, 7, 1]


def test_latest_key_holds_the_newest_window(sink, client):
    sink.write([window(100, 3), window(105, 7)])
    latest = json.loads(client.get(config.LATEST_KEY))
    assert latest["window_start"] == 105
    assert latest["total_edits"] == 7


def test_latest_key_carries_a_ttl(sink, client):
    """Without a TTL a dead aggregator leaves the dashboard showing stale data."""
    sink.write([window(100, 3)])
    assert 0 < client.ttl(config.LATEST_KEY) <= config.LATEST_TTL_SECONDS


def test_empty_batch_writes_nothing(sink, client):
    assert sink.write([]) is None
    assert client.exists(config.HISTORY_KEY) == 0


def test_write_returns_history_length(sink):
    assert sink.write([window(100, 1), window(105, 1)]) == 2


# ---------------------------------------------------------------------------
# Idempotency - the crash-recovery contract
# ---------------------------------------------------------------------------

def test_identical_replay_changes_nothing(sink, client):
    batch = [window(100, 3), window(105, 7)]
    sink.write(batch)
    before = history(client)
    sink.write(batch)
    assert history(client) == before


def test_partial_window_is_corrected_not_duplicated(sink, client):
    """
    Regression, found by SIGKILLing the aggregator mid-window.

    A window written partially before the crash is recomputed with its full
    total on replay. Keying history on payload content left both versions in
    place - two points for the same instant. Keying on window_start overwrites.
    """
    sink.write([window(215, 7)])        # partial write, then crash
    sink.write([window(215, 144)])      # recomputed after replay

    points = history(client)
    assert len(points) == 1
    assert points[0]["total"] == 144
    assert client.zcard(config.HISTORY_KEY) == 1
    assert client.hlen(config.HISTORY_DATA_KEY) == 1


def test_overlapping_replay_batch_is_merged(sink, client):
    sink.write([window(100, 1), window(105, 2)])
    sink.write([window(105, 9), window(110, 3)])   # 105 replayed with a new total

    points = history(client)
    assert [p["t"] for p in points] == [100, 105, 110]
    assert [p["total"] for p in points] == [1, 9, 3]


def test_out_of_order_batches_still_sort_by_time(sink, client):
    sink.write([window(110, 1)])
    sink.write([window(100, 1)])
    assert [p["t"] for p in history(client)] == [100, 110]


# ---------------------------------------------------------------------------
# Trimming - both keys must stay in step
# ---------------------------------------------------------------------------

def test_history_is_capped(sink, client):
    sink.write([window(100 + 5 * i, 1) for i in range(config.HISTORY_MAX + 30)])
    assert client.zcard(config.HISTORY_KEY) == config.HISTORY_MAX


def test_trim_leaves_no_orphaned_payloads(sink, client):
    """The hash must be trimmed with the zset, or it grows without bound."""
    sink.write([window(100 + 5 * i, 1) for i in range(config.HISTORY_MAX + 30)])
    assert client.hlen(config.HISTORY_DATA_KEY) == client.zcard(config.HISTORY_KEY)


def test_trim_keeps_the_newest_windows(sink, client):
    sink.write([window(100 + 5 * i, i) for i in range(config.HISTORY_MAX + 10)])
    points = history(client)
    assert len(points) == config.HISTORY_MAX
    assert points[-1]["t"] == 100 + 5 * (config.HISTORY_MAX + 9)
    assert points == sorted(points, key=lambda p: p["t"])


def test_trim_across_many_small_batches(sink, client):
    for i in range(config.HISTORY_MAX + 20):
        sink.write([window(100 + 5 * i, 1)])
    assert client.zcard(config.HISTORY_KEY) == config.HISTORY_MAX
    assert client.hlen(config.HISTORY_DATA_KEY) == config.HISTORY_MAX


# ---------------------------------------------------------------------------
# Payload shape
# ---------------------------------------------------------------------------

def test_history_point_is_compact():
    """Every dashboard client pulls the full history on connect."""
    point = json.loads(history_point(window(100, 3, bot=2)))
    assert set(point) == {"t", "total", "rate", "bot"}
    assert point["total"] == 3
    assert point["bot"] == 2
    assert point["rate"] == 0.6


def test_history_point_is_deterministic():
    assert history_point(window(100, 5)) == history_point(window(100, 5))
