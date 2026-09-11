"""
Kafka -> tumbling windows -> Redis.

Window state is kept **per partition**, not per process. That is what lets
several aggregators share the topic: each one writes only the fields of the
partitions it owns, and readers sum the fields back together. It also gives
event-time mode a watermark per partition instead of one global maximum, so a
busy partition can no longer race ahead and make a quiet partition's events
look late.

Delivery semantics: at-least-once from Kafka, idempotent at the sink, which
adds up to effectively-once window output. See offsets.py and docs/DESIGN.md.
"""

import argparse
import json
import logging
import signal
import sys
import time
from collections import defaultdict

import redis
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from pipeline import config, metrics
from pipeline.consumer.offsets import OffsetTracker
from pipeline.consumer.sink import RedisSink
from pipeline.consumer.window import WindowManager
from pipeline.merge import merge_partials

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
        self.offsets = OffsetTracker()
        self.time_source = config.WINDOW_TIME_SOURCE
        self._managers: dict[int, WindowManager] = {}
        self._running = True

    def stop(self, *_args) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Per-partition window state
    # ------------------------------------------------------------------

    def _manager(self, partition: int) -> WindowManager:
        manager = self._managers.get(partition)
        if manager is None:
            manager = WindowManager(
                size_seconds=config.WINDOW_SECONDS,
                grace_seconds=config.WINDOW_GRACE_SECONDS,
                max_gap_windows=config.MAX_GAP_WINDOWS,
            )
            self._managers[partition] = manager
        return manager

    def _clock(self, manager: WindowManager, now: float) -> float | None:
        """
        The clock that decides when this partition's windows close.

        arrival mode: wall clock, so windows always close on schedule.
        event mode:   this partition's own watermark, so a window stays open
                      until that partition's stream has moved past it.
        """
        if self.time_source == "arrival":
            return now
        return manager.watermark

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

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
                manager = self._manager(tp.partition)
                label = str(tp.partition)
                metrics.events_consumed.labels(partition=label).inc(len(messages))
                for msg in messages:
                    ts = event_timestamp(msg.value, arrival, self.time_source)
                    if manager.add(msg.value, ts):
                        self.offsets.record(
                            tp.topic, tp.partition, msg.offset, manager.align(ts)
                        )
                    else:
                        metrics.late_events.labels(partition=label).inc()

            self._flush(arrival)
            self._observe(arrival)

        self._shutdown()

    def _flush(self, now: float) -> None:
        entries: list[tuple[int, dict]] = []
        released_through: dict[int, float] = {}

        for partition, manager in self._managers.items():
            clock = self._clock(manager, now)
            if clock is None:
                continue
            summaries = manager.pop_closed(clock)
            if not summaries:
                continue
            entries.extend((partition, summary) for summary in summaries)
            released_through[partition] = summaries[-1]["window_start"]

        if not entries:
            return

        # ① Durable first: if this raises, nothing below runs and the offsets
        #    stay uncommitted, so Kafka replays these messages.
        try:
            with metrics.sink_write_seconds.time():
                retained = self.sink.write(entries)
        except Exception:
            metrics.sink_errors.inc()
            raise
        if retained is not None:
            metrics.history_windows.set(retained)

        emitted_at = time.time()
        for partition, summary in entries:
            metrics.windows_emitted.labels(partition=str(partition)).inc()
            # Measured against wall clock, so in event-time mode this includes
            # however far behind real time the stream itself is running.
            metrics.window_emit_delay.observe(emitted_at - summary["window_end"])

        # ② Only now are these offsets safe to commit, per partition.
        commits: dict[tuple[str, int], int] = {}
        for partition, through in released_through.items():
            offset = self.offsets.release_partition(
                config.KAFKA_TOPIC, partition, through
            )
            if offset is not None:
                commits[(config.KAFKA_TOPIC, partition)] = offset

        # ③ Commit.
        if commits:
            self.consumer.commit(build_commit(commits))

        self._log(entries)

    def _observe(self, now: float) -> None:
        """Refresh the gauges that describe in-memory state."""
        metrics.assigned_partitions.set(len(self._managers))
        metrics.open_windows.set(
            sum(m.open_windows for m in self._managers.values())
        )
        for partition, manager in self._managers.items():
            watermark = manager.watermark
            if watermark is not None:
                metrics.watermark_lag_seconds.labels(partition=str(partition)).set(
                    now - watermark
                )

    def _log(self, entries: list[tuple[int, dict]]) -> None:
        """Log this instance's own contribution, merged across its partitions."""
        by_window: dict[float, list[dict]] = defaultdict(list)
        for _partition, summary in entries:
            by_window[summary["window_start"]].append(summary)

        for start in sorted(by_window):
            merged = merge_partials(by_window[start])
            if merged is not None:
                log.info(format_summary(merged))

    def _shutdown(self) -> None:
        log.info(
            "shutting down - %d partitions, %d open windows discarded, %d late events",
            len(self._managers),
            sum(m.open_windows for m in self._managers.values()),
            sum(m.late_events for m in self._managers.values()),
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

    metrics.serve(config.AGGREGATOR_METRICS_PORT)

    r = redis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True
    )
    try:
        r.ping()
    except redis.RedisError as exc:
        log.error(
            "cannot reach Redis at %s:%s - %s", config.REDIS_HOST, config.REDIS_PORT, exc
        )
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
