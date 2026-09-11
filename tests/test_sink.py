"""
Sink tests against a real Redis.

These run the actual Lua script rather than a fake, because the properties
under test - a rewrite overwriting in place, two replicas *not* overwriting
each other, and the trim keeping the index and the per-window hashes in step -
depend on Redis's own semantics. A fake would only test my model of Redis.

Skipped automatically when Redis is not reachable: `docker compose up -d redis`.

They run against a separate Redis database (15) rather than the one the pipeline
uses, because the fixtures delete every window key they touch. Sharing db 0 with
a running stack meant a test run silently wiped the live dashboard's history.
"""

import json

import pytest

redis = pytest.importorskip("redis")

# Not db 0: that is where a locally running pipeline keeps its windows.
TEST_DB = 15

from pipeline import config  # noqa: E402
from pipeline.consumer.sink import RedisSink, window_key  # noqa: E402
from pipeline.consumer.window import TumblingWindow  # noqa: E402
from pipeline.merge import merge_partials  # noqa: E402


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
        db=TEST_DB,
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


def _clear(r):
    keys = list(r.scan_iter(f"{config.WINDOW_KEY_PREFIX}*"))
    if keys:
        r.delete(*keys)
    r.delete(config.HISTORY_KEY)


@pytest.fixture
def client(redis_url):
    host, port = redis_url
    r = redis.Redis(host=host, port=port, db=TEST_DB, decode_responses=True)
    _clear(r)
    yield r
    _clear(r)
    r.close()


@pytest.fixture
def sink(client):
    return RedisSink(client)


def window(start, total, bot=0, wiki="enwiki"):
    w = TumblingWindow(start=start, size=5)
    for i in range(total):
        w.add({"wiki": wiki, "user": "alice", "bot": i < bot, "type": "edit"})
    return w.snapshot()


def slices(client, start):
    """Every partition slice stored for one window."""
    raw = client.hgetall(window_key(start))
    return {int(field): json.loads(value) for field, value in raw.items()}


def merged(client, start):
    stored = slices(client, start)
    return merge_partials(list(stored.values()))


def index(client):
    return [float(m) for m in client.zrange(config.HISTORY_KEY, 0, -1)]


# ---------------------------------------------------------------------------
# Basic writes
# ---------------------------------------------------------------------------

def test_window_is_indexed_and_stored_under_its_partition(sink, client):
    sink.write([(3, window(100, 7))])

    assert index(client) == [100]
    assert list(slices(client, 100)) == [3]
    assert slices(client, 100)[3]["total_edits"] == 7


def test_windows_are_indexed_in_time_order(sink, client):
    sink.write([(0, window(110, 1)), (0, window(100, 2)), (0, window(105, 3))])
    assert index(client) == [100, 105, 110]


def test_each_window_gets_its_own_hash(sink, client):
    sink.write([(0, window(100, 1)), (0, window(105, 1))])
    assert client.exists(window_key(100)) == 1
    assert client.exists(window_key(105)) == 1


def test_window_hash_carries_a_ttl(sink, client):
    """A safety net for windows the trim never reaches because writing stopped."""
    sink.write([(0, window(100, 1))])
    assert 0 < client.ttl(window_key(100)) <= config.WINDOW_TTL_SECONDS


def test_empty_batch_writes_nothing(sink, client):
    assert sink.write([]) is None
    assert client.exists(config.HISTORY_KEY) == 0


def test_write_returns_history_length(sink):
    assert sink.write([(0, window(100, 1)), (0, window(105, 1))]) == 2


# ---------------------------------------------------------------------------
# The bug this layout exists to fix
# ---------------------------------------------------------------------------

def test_two_replicas_do_not_overwrite_each_other(sink, client):
    """
    Regression for the measured two-replica bug: a replica owning P0-P2 counted
    13 edits for a window and one owning P3-P5 counted 104. Keyed on
    window_start alone, Redis kept 104 and the true total of 117 was lost.
    """
    sink.write([(0, window(100, 13))])     # replica A, its partitions
    sink.write([(3, window(100, 104))])    # replica B, at the same instant

    assert sorted(slices(client, 100)) == [0, 3]
    assert merged(client, 100)["total_edits"] == 117
    assert index(client) == [100]


def test_replicas_interleaved_across_several_windows(sink, client):
    for start in (100, 105, 110):
        sink.write([(0, window(start, 2))])
        sink.write([(4, window(start, 8))])

    assert index(client) == [100, 105, 110]
    for start in (100, 105, 110):
        assert merged(client, start)["total_edits"] == 10


def test_per_wiki_counts_stay_exact_across_replicas(sink, client):
    sink.write([(0, window(100, 3, wiki="enwiki"))])
    sink.write([(1, window(100, 5, wiki="dewiki"))])

    assert merged(client, 100)["edits_per_wiki"] == {"dewiki": 5, "enwiki": 3}


# ---------------------------------------------------------------------------
# Idempotency - the crash-recovery contract
# ---------------------------------------------------------------------------

def test_identical_replay_changes_nothing(sink, client):
    batch = [(0, window(100, 3)), (0, window(105, 7))]
    sink.write(batch)
    before = {start: slices(client, start) for start in (100, 105)}
    sink.write(batch)

    assert {start: slices(client, start) for start in (100, 105)} == before


def test_partial_window_is_corrected_not_duplicated(sink, client):
    """
    Regression, found by SIGKILLing the aggregator mid-window: a window written
    partially before the crash is recomputed in full on replay and must
    overwrite its own slice.
    """
    sink.write([(2, window(215, 7))])      # partial write, then crash
    sink.write([(2, window(215, 144))])    # recomputed after replay

    assert list(slices(client, 215)) == [2]
    assert merged(client, 215)["total_edits"] == 144
    assert index(client) == [215]


def test_replay_of_one_partition_leaves_the_others_alone(sink, client):
    sink.write([(0, window(100, 13)), (3, window(100, 104))])
    sink.write([(0, window(100, 20))])     # only partition 0 replays

    stored = slices(client, 100)
    assert stored[0]["total_edits"] == 20
    assert stored[3]["total_edits"] == 104
    assert merged(client, 100)["total_edits"] == 124


# ---------------------------------------------------------------------------
# Trimming - the index and the window hashes must stay in step
# ---------------------------------------------------------------------------

def test_history_is_capped(sink, client):
    sink.write([(0, window(100 + 5 * i, 1)) for i in range(config.HISTORY_MAX + 30)])
    assert client.zcard(config.HISTORY_KEY) == config.HISTORY_MAX


def test_trim_deletes_the_window_hashes_it_drops(sink, client):
    """Trimming only the index would leak one hash per dropped window."""
    sink.write([(0, window(100 + 5 * i, 1)) for i in range(config.HISTORY_MAX + 30)])

    stored = list(client.scan_iter(f"{config.WINDOW_KEY_PREFIX}*"))
    assert len(stored) == config.HISTORY_MAX
    assert client.exists(window_key(100)) == 0


def test_trim_keeps_the_newest_windows(sink, client):
    sink.write([(0, window(100 + 5 * i, 1)) for i in range(config.HISTORY_MAX + 10)])

    kept = index(client)
    assert len(kept) == config.HISTORY_MAX
    assert kept[-1] == 100 + 5 * (config.HISTORY_MAX + 9)
    assert kept == sorted(kept)


def test_trim_across_many_small_batches(sink, client):
    for i in range(config.HISTORY_MAX + 20):
        sink.write([(0, window(100 + 5 * i, 1))])

    assert client.zcard(config.HISTORY_KEY) == config.HISTORY_MAX
    assert len(list(client.scan_iter(f"{config.WINDOW_KEY_PREFIX}*"))) == config.HISTORY_MAX
