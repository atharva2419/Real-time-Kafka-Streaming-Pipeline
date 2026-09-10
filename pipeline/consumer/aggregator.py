"""
Kafka -> tumbling windows -> Redis.

Delivery semantics: at-least-once from Kafka, idempotent at the sink, which
adds up to effectively-once window output. See offsets.py and docs/DESIGN.md.
"""

import argparse
import json
import logging
import signal
import sys
import time

import redis
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from pipeline import config
from pipeline.consumer.offsets import OffsetTracker
from pipeline.consumer.sink import RedisSink
from pipeline.consumer.window import WindowManager

log = logging.getLogger("aggregator")

POLL_TIMEOUT_MS = 500


# ---------------------------------------------------------------------------
# Offset commits
# ---------------------------------------------------------------------------

def build_commit(
    offsets: dict[tuple[str, int], int],
) -> dict[TopicPartition, OffsetAndMetadata]:
    """
    Convert OffsetTracker output into the shape KafkaConsumer.commit() wants.

    kafka-python asserts on the value type, so raw ints fail at runtime rather
    than at import. The struct gained a `leader_epoch` field in 2.2 with no
    default, so its arity is checked rather than assumed.
    """
    fields = len(OffsetAndMetadata._fields)
    return {
        TopicPartition(topic, partition): (
            OffsetAndMetadata(offset, "", -1) if fields == 3
            else OffsetAndMetadata(offset, "")
        )
        for (topic, partition), offset in offsets.items()
    }


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

def format_summary(summary: dict) -> str:
    ts = time.strftime("%H:%M:%S", time.localtime(summary["window_start"]))
    top = ", ".join(
        f"{wiki}: {count:,}"
        for wiki, count in list(summary["edits_per_wiki"].items())[:4]
    )
    counts = summary["bot_vs_human"]
    seen = counts["bot"] + counts["human"]
    bot_pct = (counts["bot"] / seen * 100) if seen else 0.0
    return (
        f"[{ts}] {summary['total_edits']:>5,} edits "
        f"({summary['edits_per_second']:>6.1f}/s) | {top or '-'} | bots: {bot_pct:.0f}%"
    )


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        group_id=config.CONSUMER_GROUP,
        # Members of this group share the topic's partitions, so instances can
        # be scaled horizontally. Offsets are committed by hand, only once the
        # window a message landed in has been written to Redis.
        enable_auto_commit=False,
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        max_poll_records=2000,
        consumer_timeout_ms=-1,
    )


def event_timestamp(event: dict, arrival: float, source: str) -> float:
    if source == "event":
        ts = event.get("timestamp")
        if isinstance(ts, int | float) and ts > 0:
            return float(ts)
    return arrival


class Aggregator:
    def __init__(self, consumer: KafkaConsumer, r: redis.Redis) -> None:
        self.consumer = consumer
        self.sink = RedisSink(r)
        self.windows = WindowManager(
            size_seconds=config.WINDOW_SECONDS,
            grace_seconds=config.WINDOW_GRACE_SECONDS,
            max_gap_windows=config.MAX_GAP_WINDOWS,
        )
        self.offsets = OffsetTracker()
        self.time_source = config.WINDOW_TIME_SOURCE
        self._running = True

    def stop(self, *_args) -> None:
        self._running = False

    def run(self) -> None:
        log.info(
            "aggregator started - topic=%s group=%s window=%ss grace=%ss time_source=%s",
            config.KAFKA_TOPIC,
            config.CONSUMER_GROUP,
            config.WINDOW_SECONDS,
            config.WINDOW_GRACE_SECONDS,
            self.time_source,
        )

        while self._running:
            records = self.consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            arrival = time.time()

            for tp, messages in records.items():
                for msg in messages:
                    ts = event_timestamp(msg.value, arrival, self.time_source)
                    if self.windows.add(msg.value, ts):
                        self.offsets.record(
                            tp.topic, tp.partition, msg.offset, self.windows.align(ts)
                        )

            self._flush(self._clock(arrival))

        self._shutdown()

    def _clock(self, now: float) -> float | None:
        """
        The clock that decides when a window closes.

        arrival mode: wall clock, so windows always close on schedule.
        event mode:   the watermark, so a window stays open until the stream
                      itself has moved past it. A stalled upstream therefore
                      stalls the windows rather than emitting false zeros.
        """
        if self.time_source == "arrival":
            return now
        return self.windows.watermark

    def _flush(self, now: float | None) -> None:
        if now is None:
            return
        summaries = self.windows.pop_closed(now)
        if not summaries:
            return

        self.sink.write(summaries)

        # Only now are these offsets safe to commit.
        commits = self.offsets.release(summaries[-1]["window_start"])
        if commits:
            self.consumer.commit(build_commit(commits))

        for summary in summaries:
            log.info(format_summary(summary))

    def _shutdown(self) -> None:
        log.info(
            "shutting down - %d open windows discarded, %d late events seen",
            self.windows.open_windows,
            self.windows.late_events,
        )
        self.consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Wikimedia edit-stream aggregator")
    parser.add_argument(
        "--time-source",
        choices=("arrival", "event"),
        help="override WINDOW_TIME_SOURCE",
    )
    args = parser.parse_args()
    if args.time_source:
        config.WINDOW_TIME_SOURCE = args.time_source

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    r = redis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True
    )
    try:
        r.ping()
    except redis.RedisError as exc:
        log.error("cannot reach Redis at %s:%s - %s", config.REDIS_HOST, config.REDIS_PORT, exc)
        sys.exit(1)

    try:
        aggregator = Aggregator(make_consumer(), r)
    except KafkaError as exc:
        log.error("cannot reach Kafka at %s - %s", config.KAFKA_BOOTSTRAP, exc)
        sys.exit(1)

    signal.signal(signal.SIGINT, aggregator.stop)
    signal.signal(signal.SIGTERM, aggregator.stop)
    aggregator.run()


if __name__ == "__main__":
    main()
