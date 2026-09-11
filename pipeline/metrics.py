"""
Prometheus metrics for every process in the pipeline.

Why a separate plane at all, when there is already a dashboard? Because the
dashboard reads Redis through the aggregator's output, so when the pipeline
breaks the dashboard goes blank - exactly when you most need to see what is
happening. Prometheus scrapes the processes directly and keeps reporting when
the data path is dead.

Two conventions worth knowing if you extend this:

  Counters over gauges.
      Windows are 5 s and scrapes are 5 s, so anything exposed as a gauge that
      changes per window can be missed between scrapes. Counters plus rate()
      lose nothing.

  Label cardinality is a budget.
      `partition` has six values and is safe. `wiki` has hundreds and would
      multiply every series by that - per-wiki counts belong in Redis, where
      they already are.

prometheus_client appends the `_total` suffix to counters itself, so the names
declared here deliberately omit it.
"""

import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = logging.getLogger("metrics")

# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

events_produced = Counter(
    "wiki_events_produced",
    "Events published to Kafka.",
)
events_dropped = Counter(
    "wiki_events_dropped",
    "Events discarded before publishing.",
    ["reason"],
)
produce_errors = Counter(
    "wiki_produce_errors",
    "Sends that failed after the producer's own retries. Should always be 0.",
)
sse_reconnects = Counter(
    "wiki_sse_reconnects",
    "Reconnections to the upstream event stream.",
    ["reason"],
)

# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------

events_consumed = Counter(
    "wiki_events_consumed",
    "Messages consumed from Kafka.",
    ["partition"],
)
windows_emitted = Counter(
    "wiki_windows_emitted",
    "Windows closed and written to Redis.",
    ["partition"],
)
late_events = Counter(
    "wiki_late_events",
    "Events dropped because their window had already been emitted.",
    ["partition"],
)
window_emit_delay = Histogram(
    "wiki_window_emit_delay_seconds",
    "Delay between a window ending and being written to Redis.",
    # The freshness target is ~2.5 s; buckets are placed to make that visible.
    buckets=(0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 5.0, 10.0, 30.0),
)
sink_write_seconds = Histogram(
    "wiki_sink_write_seconds",
    "Duration of the Lua write to Redis.",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0),
)
sink_errors = Counter(
    "wiki_sink_errors",
    "Redis writes that raised. Offsets are not committed when this happens.",
)
open_windows = Gauge(
    "wiki_open_windows",
    "Windows currently held in memory across all owned partitions.",
)
assigned_partitions = Gauge(
    "wiki_assigned_partitions",
    "Partitions this instance holds window state for.",
)
watermark_lag_seconds = Gauge(
    "wiki_watermark_lag_seconds",
    "Wall clock minus this partition's watermark. Near zero in arrival mode.",
    ["partition"],
)
history_windows = Gauge(
    "wiki_history_windows",
    "Windows currently retained in Redis.",
)

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

ws_clients = Gauge(
    "wiki_api_ws_clients",
    "Open WebSocket connections.",
)
redis_read_seconds = Histogram(
    "wiki_api_redis_read_seconds",
    "Duration of a Redis read served by the API.",
    ["op"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5),
)


# ---------------------------------------------------------------------------
# Exposition
# ---------------------------------------------------------------------------

def serve(port: int) -> None:
    """
    Expose /metrics on a background thread.

    The producer and aggregator are not HTTP servers, so this is how Prometheus
    reaches them; the API serves its own /metrics route instead.
    """
    start_http_server(port)
    log.info("metrics exposed on :%d/metrics", port)
