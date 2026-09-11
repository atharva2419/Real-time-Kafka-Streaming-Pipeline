# Real-time Kafka Streaming Pipeline

Ingests Wikimedia's public edit firehose (~35 events/sec), aggregates it into
5-second tumbling windows, and serves the result to a live WebSocket dashboard.

The interesting part is not the topology — it is the delivery semantics. The
aggregator withholds Kafka offsets until the window a message landed in has been
durably written, and each window is stored in Redis as one field per Kafka
partition, so a `SIGKILL` mid-stream replays without losing or duplicating
anything and two aggregators can share the work without overwriting each other.
Every claim here was verified by running it — including three that turned out to
be wrong, which is what **[docs/DESIGN.md](docs/DESIGN.md)** is mostly about.

---

## Architecture

```
Wikimedia SSE firehose
  (stream.wikimedia.org)
         │  text/event-stream
         ▼
  pipeline/producer/produce.py
  · validates + projects to 7 fields
  · keyed by wiki -> per-wiki ordering
  · idempotent producer, acks=all
         │
         ▼
  Kafka (KRaft, 6 partitions)  ──────┐
         │                            │
         ▼                            ▼
  pipeline/consumer/aggregator.py   [more group members:
  · window state per partition        each writes only the
  · epoch-aligned tumbling windows    partitions it owns]
  · arrival-time or watermark-driven
    event-time bucketing
  · commits offsets only after the
    window is written
         │
         ▼
       Redis
  · wiki:history      zset index of window starts
  · wiki:win:<start>  hash, one field per partition
         │
         ▼
  pipeline/api/server.py  (FastAPI)
  · /ws pushes each new window
         │
         ▼
  Live dashboard at localhost:8000
```

---

## Quick start

```bash
docker compose up -d --build
```

That is the whole thing — Kafka, Redis, producer, aggregator and API. Health
checks gate startup order, so nothing races the broker. Open
**http://localhost:8000**.

```bash
curl localhost:8000/healthz          # {"status":"ok","latest_window":...}
curl localhost:8000/api/latest       # full window summary
curl "localhost:8000/api/history?limit=20"
docker compose logs -f aggregator
docker compose down -v
```

### Running it without Docker

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
cp .env.example .env

docker compose up -d kafka redis     # infrastructure only

python -m pipeline.producer.produce
python -m pipeline.consumer.aggregator
python -m pipeline.api.server
```

`kafka-python` 2.0.x cannot be imported on Python 3.12; `requirements.txt` pins
`>=2.2` for that reason.

---

## What it actually prints

Real output, not illustrative:

```
22:17:56 INFO  [22:17:50]    67 edits (  13.4/s) | wikidatawiki: 18, enwiki: 17, elwiktionary: 8 | bots: 33%
22:18:01 INFO  [22:17:55]   198 edits (  39.6/s) | commonswiki: 67, wikidatawiki: 42, enwiki: 29 | bots: 48%
22:18:06 INFO  [22:18:00]   176 edits (  35.2/s) | wikidatawiki: 77, commonswiki: 32, enwiki: 13 | bots: 44%
22:18:11 INFO  [22:18:05]   180 edits (  36.0/s) | wikidatawiki: 60, commonswiki: 46, enwiki: 20 | bots: 42%
```

The firehose runs at roughly **30–50 edits/sec**, so a 5-second window holds
~150–250 edits. `wikidatawiki` and `commonswiki` are about half the volume, and
bots are typically 40–50%.

---

## Testing

```bash
pytest                    # 117 tests
pytest --cov              # 100% on window.py, offsets.py, sink.py, merge.py
ruff check .
mypy                      # clean across pipeline/ and tests/
```

The sink tests run against a **real Redis** rather than a fake, because the
property under test — that a rewritten window overwrites in place — depends on
Redis's own semantics and on a Lua script keeping two keys in step. A fake would
only test my model of Redis. They skip automatically if Redis is not up.

CI runs lint and tests on Python 3.11 and 3.12, then builds the stack and blocks
until `/healthz` reports a live window, so a broken pipeline fails the build
rather than only a broken unit.

### Reproducing the crash test

```bash
docker compose exec redis redis-cli FLUSHALL
docker compose kill -s SIGKILL aggregator
sleep 15
docker compose start aggregator
# then inspect wiki:history for gaps and duplicates
```

In `event` mode the history comes back fully contiguous with zero duplicates. In
the default `arrival` mode nothing is lost either, but the replayed backlog
lands in the window open at restart — a hole plus one inflated window. Why, and
which you want, is [§3 of the design notes](docs/DESIGN.md).

### Scaling the aggregator

```bash
docker compose up -d --scale aggregator=2
docker compose logs aggregator | grep edits    # each replica logs its own share
curl localhost:8000/api/latest                 # the API serves the merged window
```

Kafka splits the six partitions across the group, and each replica writes only
the Redis fields for the partitions it owns, so their work composes rather than
collides. Measured with two replicas: 20 + 199 = 219, 11 + 228 = 239,
12 + 231 = 243 — the merged totals match the replicas' own logs exactly.

This did not use to work. Both replicas wrote the same key and the last one won,
silently undercounting every window; [§5.3 of the design
notes](docs/DESIGN.md) has the numbers and the fix.

---

## Configuration

Every setting is an environment variable; see [.env.example](.env.example).

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka:29092` inside compose |
| `KAFKA_PARTITIONS` | `6` | Created explicitly; auto-creation gives 1 |
| `WINDOW_SECONDS` | `5` | Windows are epoch-aligned |
| `WINDOW_GRACE_SECONDS` | `1.0` | How long a closed window waits for stragglers |
| `WINDOW_TIME_SOURCE` | `arrival` | `arrival` or `event` — see design notes |
| `HISTORY_MAX` | `120` | Windows retained (10 minutes at 5s) |
| `STALE_AFTER_SECONDS` | `30` | `/healthz` degrades when the newest window is older |

---

## Project layout

```
pipeline/
├── config.py              env-driven configuration
├── merge.py               summing per-partition window slices
├── producer/produce.py    SSE -> Kafka, keyed by wiki
├── consumer/
│   ├── window.py          TumblingWindow + WindowManager (watermarks, gaps)
│   ├── offsets.py         commit-after-write bookkeeping
│   ├── sink.py            idempotent, partition-scoped Redis writer (Lua)
│   └── aggregator.py      the consume loop, window state per partition
└── api/
    ├── server.py          FastAPI: REST + WebSocket
    └── static/index.html  dashboard
tests/                     117 tests
docs/DESIGN.md             semantics, trade-offs, measurements
docker/Dockerfile          one image, three entrypoints
```

---

## Known limitations

Kept honest and current in [§6 of the design notes](docs/DESIGN.md). The
short version: no metrics export yet (Prometheus + Grafana is the next
addition), top-5 editors is approximate when several aggregators share the
topic (totals and per-wiki counts stay exact), there is no rebalance listener,
the watermark has no idle timeout, and Redis history is a rolling 10-minute
window with no durable store behind it.

## Roadmap

- Prometheus metrics — consumer lag, flush latency, `late_events` — and Grafana
- A rebalance listener wired to `OffsetTracker.forget()`
- Kafka offsets stored in the sink's Lua script, for true exactly-once
- Load-generator mode replaying a captured file at N× speed, with a measured
  throughput/latency table
- Schema Registry + Avro, with a compatibility check in CI
- A dead-letter topic for malformed events
- A durable sink (ClickHouse or TimescaleDB) for history beyond the rolling window
