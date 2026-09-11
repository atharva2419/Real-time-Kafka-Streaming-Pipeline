"""
Wikimedia recentchange SSE -> Kafka.

Keyed by wiki, so all edits for a given wiki land on the same partition and
preserve their relative order. That also lets the aggregator scale out: adding
a consumer to the group moves whole wikis between instances rather than
splitting one wiki's stream across two window states.
"""

import json
import logging
import signal
import sys
import time

import requests
import sseclient
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from pipeline import config, metrics

log = logging.getLogger("producer")

RECONNECT_DELAY = 5
# Only the fields the aggregator actually groups by. `id` is deliberately
# NOT required: Wikimedia log events (page moves, deletions, user creation)
# carry a null id, and requiring it silently discarded ~2.7% of the stream
# and undercounted the "log" bucket in the edit-type breakdown.
REQUIRED_FIELDS = ("type", "wiki")
# SSEClient consumes an iterator of byte chunks. Iterating a Response
# directly would work, but only via Response.__iter__'s hardcoded 128-byte
# chunks; iter_content yields as data arrives, so a larger size cuts
# per-chunk overhead without adding latency.
SSE_CHUNK_BYTES = 8192

_running = True


def _stop(*_args) -> None:
    global _running
    _running = False


def _on_send_error(exc: Exception) -> None:
    """
    Sends are asynchronous, so a batch that fails after the producer's own
    retries would otherwise vanish: the future's exception is never looked at,
    nothing is logged, and the event is simply gone. This is the only place
    that loss becomes visible.
    """
    metrics.produce_errors.inc()
    log.error("kafka send failed after retries: %s", exc)


# ---------------------------------------------------------------------------
# Kafka setup
# ---------------------------------------------------------------------------

def ensure_topic() -> None:
    """
    Create the topic with an explicit partition count.

    Without this, auto-creation gives a single partition, which silently caps
    the consumer group at one useful member.
    """
    try:
        admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP)
    except KafkaError as exc:
        log.warning("could not reach Kafka admin API (%s) - relying on auto-create", exc)
        return

    try:
        admin.create_topics(
            [
                NewTopic(
                    name=config.KAFKA_TOPIC,
                    num_partitions=config.KAFKA_PARTITIONS,
                    replication_factor=1,
                )
            ]
        )
        log.info(
            "created topic '%s' with %d partitions",
            config.KAFKA_TOPIC,
            config.KAFKA_PARTITIONS,
        )
    except TopicAlreadyExistsError:
        log.info("topic '%s' already exists", config.KAFKA_TOPIC)
    except KafkaError as exc:
        log.warning("topic creation failed: %s", exc)
    finally:
        admin.close()


# typeshed's kafka-python stubs declare serializers as Callable[[object], bytes],
# so they must accept any object: a `key: str` parameter is rejected, and a
# lambda's argument is typed `object`, which has no .encode(). Narrow instead.
def serialize_key(key: object) -> bytes:
    if not isinstance(key, str):
        raise TypeError(f"partition key must be str, got {type(key).__name__}")
    return key.encode("utf-8")


def serialize_value(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=serialize_value,
        key_serializer=serialize_key,
        # Durability over raw throughput: wait for all in-sync replicas, and
        # let the broker de-duplicate retried batches rather than producing
        # the same edit twice after a transient network blip.
        acks="all",
        enable_idempotence=True,
        retries=5,
        # Batch for ~50ms; at ~40 events/sec this cuts request count sharply
        # for a latency cost far below the 5s window size.
        linger_ms=50,
        compression_type="gzip",
    )


# ---------------------------------------------------------------------------
# Event handling
# ---------------------------------------------------------------------------

def extract_event(raw: object) -> dict | None:
    """Project the payload down to the fields we aggregate, or None if unusable."""
    if not isinstance(raw, dict):
        return None
    if any(raw.get(field) in (None, "") for field in REQUIRED_FIELDS):
        return None

    timestamp = raw.get("timestamp")
    if not isinstance(timestamp, int | float):
        timestamp = None

    return {
        "id": raw.get("id"),
        "type": raw.get("type"),
        "title": raw.get("title"),
        "wiki": raw.get("wiki"),
        "user": raw.get("user"),
        "timestamp": timestamp,
        "bot": bool(raw.get("bot", False)),
    }


class Stats:
    def __init__(self) -> None:
        self.sent = 0
        self.dropped = 0
        self._mark = time.time()
        self._mark_sent = 0

    def tick(self) -> str | None:
        if self.sent == 0 or self.sent % config.PRINT_EVERY != 0:
            return None
        now = time.time()
        elapsed = now - self._mark
        rate = (self.sent - self._mark_sent) / elapsed if elapsed > 0 else 0.0
        self._mark, self._mark_sent = now, self.sent
        return f"sent {self.sent:,} events ({rate:.0f}/sec), dropped {self.dropped:,}"


def stream_events(producer: KafkaProducer, stats: Stats) -> None:
    headers = {"Accept": "text/event-stream", "User-Agent": config.USER_AGENT}
    response = requests.get(
        config.WIKIMEDIA_SSE_URL, stream=True, headers=headers, timeout=30
    )
    response.raise_for_status()

    stream = response.iter_content(chunk_size=SSE_CHUNK_BYTES)
    # The stub types event_source as Generator, but SSEClient only iterates
    # it, so an Iterator satisfies the actual contract.
    for sse in sseclient.SSEClient(stream).events():  # type: ignore[arg-type]
        if not _running:
            return
        if not sse.data or not sse.data.strip():
            continue
        try:
            raw = json.loads(sse.data)
        except json.JSONDecodeError:
            stats.dropped += 1
            metrics.events_dropped.labels(reason="unparseable").inc()
            continue

        event = extract_event(raw)
        if event is None:
            stats.dropped += 1
            metrics.events_dropped.labels(reason="invalid").inc()
            continue

        producer.send(
            config.KAFKA_TOPIC, key=event["wiki"], value=event
        ).add_errback(_on_send_error)
        stats.sent += 1
        metrics.events_produced.inc()

        line = stats.tick()
        if line:
            log.info(line)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    metrics.serve(config.PRODUCER_METRICS_PORT)
    ensure_topic()

    try:
        producer = make_producer()
    except KafkaError as exc:
        log.error("cannot reach Kafka at %s - %s", config.KAFKA_BOOTSTRAP, exc)
        sys.exit(1)

    log.info("streaming %s -> topic '%s'", config.WIKIMEDIA_SSE_URL, config.KAFKA_TOPIC)
    stats = Stats()

    while _running:
        try:
            stream_events(producer, stats)
        except requests.exceptions.RequestException as exc:
            metrics.sse_reconnects.labels(reason="sse").inc()
            log.warning("SSE stream error: %s - reconnecting in %ds", exc, RECONNECT_DELAY)
            time.sleep(RECONNECT_DELAY)
        except KafkaError as exc:
            metrics.sse_reconnects.labels(reason="kafka").inc()
            log.warning("Kafka error: %s - reconnecting in %ds", exc, RECONNECT_DELAY)
            time.sleep(RECONNECT_DELAY)
            producer = make_producer()

    log.info("draining producer buffer")
    producer.flush(timeout=10)
    producer.close(timeout=10)
    log.info("producer stopped - sent %s, dropped %s", f"{stats.sent:,}", f"{stats.dropped:,}")


if __name__ == "__main__":
    main()
