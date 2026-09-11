"""Central configuration, loaded from the environment (see .env.example)."""

import os

from dotenv import load_dotenv

load_dotenv()


def _str(name: str, default: str) -> str:
    return os.getenv(name, default)


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    # Narrowed with `is None` rather than `not in (None, "")` so type checkers
    # can follow it; an unset and an empty variable both mean "use the default".
    if raw is None or raw == "":
        return default
    return int(raw)


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


# --- Kafka ---------------------------------------------------------------
KAFKA_BOOTSTRAP = _str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = _str("KAFKA_TOPIC", "wiki-edits")
KAFKA_PARTITIONS = _int("KAFKA_PARTITIONS", 6)
CONSUMER_GROUP = _str("KAFKA_CONSUMER_GROUP", "wiki-aggregator")

# --- Redis ---------------------------------------------------------------
REDIS_HOST = _str("REDIS_HOST", "localhost")
REDIS_PORT = _int("REDIS_PORT", 6379)

# Index of known windows: ZSET, member = score = window_start.
HISTORY_KEY = "wiki:history"
# One HASH per window, field = partition, value = that partition's slice.
# Partition-scoped so that aggregator replicas never overwrite each other.
WINDOW_KEY_PREFIX = "wiki:win:"
HISTORY_MAX = _int("HISTORY_MAX", 120)
# A window older than the retained history is dead weight; the TTL is a safety
# net for windows the trim never reaches because writing stopped.
WINDOW_TTL_SECONDS = _int("WINDOW_TTL_SECONDS", 0)
# /healthz reports degraded when the newest window is older than this.
STALE_AFTER_SECONDS = _int("STALE_AFTER_SECONDS", 30)

# --- Windowing -----------------------------------------------------------
WINDOW_SECONDS = _int("WINDOW_SECONDS", 5)
if WINDOW_TTL_SECONDS <= 0:
    WINDOW_TTL_SECONDS = HISTORY_MAX * WINDOW_SECONDS * 2
# How long to keep a closed window open for stragglers before emitting it.
WINDOW_GRACE_SECONDS = _float("WINDOW_GRACE_SECONDS", 1.0)
# "arrival" uses the time the event was polled from Kafka; "event" uses the
# producer-supplied timestamp field. See docs/DESIGN.md.
WINDOW_TIME_SOURCE = _str("WINDOW_TIME_SOURCE", "arrival")
# Safety valve: if the clock jumps (laptop sleep, NTP step) don't emit
# thousands of empty windows to catch up.
MAX_GAP_WINDOWS = _int("MAX_GAP_WINDOWS", 12)

# --- Producer ------------------------------------------------------------
WIKIMEDIA_SSE_URL = _str(
    "WIKIMEDIA_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
)
USER_AGENT = _str(
    "USER_AGENT",
    "KafkaWikiPipeline/2.0 (https://github.com/atharva2419; educational project)",
)
PRINT_EVERY = _int("PRINT_EVERY", 500)

# --- API -----------------------------------------------------------------
API_HOST = _str("API_HOST", "0.0.0.0")
API_PORT = _int("API_PORT", 8000)
WS_PUSH_INTERVAL = _float("WS_PUSH_INTERVAL", 1.0)
