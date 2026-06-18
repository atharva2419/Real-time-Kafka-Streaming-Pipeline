# Real-time Kafka Streaming Pipeline

A real-time data streaming pipeline that ingests Wikimedia's public edit stream, aggregates metrics using tumbling windows, and streams results to a live WebSocket dashboard.

**Resume bullet:** *Built a real-time streaming pipeline with Kafka, Python consumers, window aggregation, and a live WebSocket dashboard.*

---

## Architecture

```
Wikimedia SSE Stream
  (stream.wikimedia.org)
         │
         │  HTTP chunked / text-event-stream
         ▼
  producer/produce.py
  (sseclient + kafka-python)
         │
         │  JSON messages → topic: "wiki-edits"
         ▼
  Apache Kafka (Docker)
         │
         ├──────────────────────────┐
         │                          │
         ▼                          ▼
  consumer/aggregator.py      [future consumers]
  (5s tumbling windows)
         │
         │  SET wiki:latest_window
         │  RPUSH wiki:history
         ▼
       Redis
         │
         ▼
  FastAPI (WebSocket)          [coming soon]
         │
         ▼
  React Dashboard              [coming soon]
  (live chart)
```

---

## Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 7.6.0 (Confluent) |
| Stream coordination | Apache ZooKeeper 7.6.0 |
| Cache / state store | Redis 7 (Alpine) |
| Producer | Python · `kafka-python` · `sseclient-py` · `requests` |
| Consumer / aggregator | Python · `kafka-python` · `redis-py` |
| API server | FastAPI · Uvicorn |
| Frontend | React |
| Infrastructure | Docker Compose |
| Observability | Prometheus *(coming soon)* |

---

## Project Structure

```
.
├── docker-compose.yml          # Zookeeper, Kafka, Redis
├── requirements.txt
├── producer/
│   └── produce.py              # SSE → Kafka producer
└── consumer/
    ├── window.py               # TumblingWindow class
    └── aggregator.py           # Kafka consumer + Redis writer
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.11+
- pip

---

## Setup

**1. Clone the repository**

```bash
git clone <repo-url>
cd "Real-time Kafka Streaming Pipeline"
```

**2. Install Python dependencies**

```bash
pip install -r requirements.txt
```

**3. Start infrastructure (Kafka + ZooKeeper + Redis)**

```bash
docker compose up -d
```

Wait ~10 seconds for Kafka to finish initialising before running the producer.

---

## Running

Open three terminal windows.

**Terminal 1 — Producer** (ingest Wikimedia edits → Kafka)

```bash
python producer/produce.py
```

Expected output:
```
Connected to Kafka at localhost:9092, publishing to 'wiki-edits'
Produced 100 events (47/sec)
Produced 200 events (51/sec)
```

**Terminal 2 — Aggregator** (Kafka → 5s tumbling windows → Redis)

```bash
python consumer/aggregator.py
```

Expected output:
```
Aggregator started — consuming 'wiki-edits', window=5s
[14:32:05] 1,847 edits | enwiki: 412, dewiki: 201, frwiki: 98, eswiki: 74 | bots: 62%
[14:32:10] 1,903 edits | enwiki: 389, dewiki: 188, frwiki: 112, ruwiki: 67 | bots: 58%
```

**Terminal 3 — Verify Redis output**

```bash
docker exec -it <redis-container-name> redis-cli
GET wiki:latest_window
LRANGE wiki:history 0 -1
```

---

## How It Works

### Producer (`producer/produce.py`)

Connects to Wikimedia's free public SSE endpoint — no authentication required. Each server-sent event is a JSON payload describing a wiki edit (page title, editor username, wiki site, bot flag, etc.). The producer strips the payload down to 7 fields and publishes to the `wiki-edits` Kafka topic. A background thread inside `kafka-python` batches and flushes messages to the broker asynchronously, so the hot loop is never blocked by network I/O. The outer `while True` reconnects automatically if the SSE stream or Kafka connection drops.

### Tumbling Window (`consumer/window.py`)

A `TumblingWindow` accumulates five counters over a fixed 5-second interval: total edits, edits per wiki, bot vs human ratio, event type distribution, and per-user edit counts. When `flush()` is called it snapshots the accumulators, advances the start pointer by exactly 5 seconds (preserving alignment even if flush is called slightly late), resets state, and returns a JSON-serialisable dict. Arrival time is used for window boundaries rather than event timestamps because Wikimedia's timestamps can lag wall-clock time by several seconds.

### Aggregator (`consumer/aggregator.py`)

Polls Kafka every 500 ms, feeds each message into the window, and after every poll checks whether the window has expired. On close it writes two things to Redis atomically via a pipeline: the full window summary JSON (30-second TTL) and the edit count appended to a time-series list capped at 60 entries (5 minutes of history).

---

## Data Schema

**Kafka message (`wiki-edits` topic)**

```json
{
  "id": 12345678,
  "type": "edit",
  "title": "Python (programming language)",
  "wiki": "enwiki",
  "user": "SomeEditor",
  "timestamp": 1718700000,
  "bot": false
}
```

**Redis `wiki:latest_window`**

```json
{
  "window_start": 1718700005.123,
  "window_end": 1718700010.123,
  "total_edits": 1847,
  "edits_per_wiki": { "enwiki": 412, "dewiki": 201, "frwiki": 98 },
  "bot_vs_human": { "bot": 1143, "human": 704 },
  "edit_types": { "edit": 1200, "new": 400, "log": 200, "categorize": 47 },
  "top_editors": [
    { "user": "CleanupBot", "count": 34 },
    { "user": "WikiGnome", "count": 12 }
  ]
}
```

**Redis `wiki:history`** — list of integers, last 60 window totals (newest at right)

---

## Stopping

```bash
# Stop the producer and aggregator with Ctrl+C in each terminal

# Tear down Docker services
docker compose down
```

To also delete all Kafka data and Redis state:

```bash
docker compose down -v
```

---

## What's Coming

- `api/server.py` — FastAPI WebSocket server that streams Redis data to clients
- `dashboard/` — React app with a live chart (Chart.js or Recharts)
- Prometheus metrics endpoint on the aggregator
- Docker Compose profiles to run the full stack with one command
