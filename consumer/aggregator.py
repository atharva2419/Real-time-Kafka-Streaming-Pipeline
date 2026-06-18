import json
import sys
import time

import redis
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from window import TumblingWindow

KAFKA_TOPIC = "wiki-edits"
KAFKA_BOOTSTRAP = "localhost:9092"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
WINDOW_KEY = "wiki:latest_window"
HISTORY_KEY = "wiki:history"
HISTORY_MAX = 60
TTL_SECONDS = 30


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def write_window(r: redis.Redis, summary: dict) -> None:
    pipe = r.pipeline()
    # Overwrite the "latest window" with a 30-second TTL so stale data
    # disappears automatically if the aggregator stops.
    pipe.set(WINDOW_KEY, json.dumps(summary), ex=TTL_SECONDS)
    # Append total_edits to the time-series history list, then trim to the
    # most recent HISTORY_MAX entries (60 windows × 5 s = 5 minutes of history).
    pipe.rpush(HISTORY_KEY, summary["total_edits"])
    pipe.ltrim(HISTORY_KEY, -HISTORY_MAX, -1)
    pipe.execute()


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

def print_summary(summary: dict) -> None:
    ts = time.strftime("%H:%M:%S", time.localtime(summary["window_start"]))
    total = summary["total_edits"]

    top_wikis = sorted(
        summary["edits_per_wiki"].items(), key=lambda x: x[1], reverse=True
    )[:4]
    wikis_str = ", ".join(f"{w}: {c:,}" for w, c in top_wikis)

    bv = summary["bot_vs_human"]
    total_bh = bv["bot"] + bv["human"]
    bot_pct = (bv["bot"] / total_bh * 100) if total_bh else 0

    print(f"[{ts}] {total:,} edits | {wikis_str} | bots: {bot_pct:.0f}%")


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        # No group_id → each aggregator instance gets its own independent
        # offset cursor (no consumer-group coordination needed here).
        group_id="wiki-aggregator",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        # Poll returns as soon as 1 message is available, so the window
        # close check runs frequently even at low throughput.
        fetch_max_wait_ms=500,
    )


def main() -> None:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    consumer = make_consumer()
    window = TumblingWindow(size_seconds=5)

    print(f"Aggregator started — consuming '{KAFKA_TOPIC}', window=5s")

    try:
        while True:
            # poll() with a short timeout so we check the window clock even
            # during quiet periods (low edit rate on some wikis at night).
            records = consumer.poll(timeout_ms=500)

            for _, messages in records.items():
                for msg in messages:
                    window.add(msg.value)

            if window.is_closed():
                summary = window.flush()
                write_window(r, summary)
                print_summary(summary)

    except KafkaError as exc:
        print(f"Kafka error: {exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("Aggregator stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
