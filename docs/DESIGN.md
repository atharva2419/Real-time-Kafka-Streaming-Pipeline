# Design notes

Why this pipeline is built the way it is, and what it does when things break.

Every claim in §5 was measured on a live run against the Wikimedia firehose, not
reasoned about. Two of them started out wrong; §5.1 records what the
measurements changed.

---

## 1. Delivery semantics

**Guarantee: at-least-once delivery from Kafka, idempotent writes at the sink.**

Kafka alone cannot give end-to-end exactly-once here, because the sink is Redis
rather than another Kafka topic — there is no transaction spanning both. The
guarantee is assembled from two halves instead.

### Half one: never commit an offset early

The obvious implementation — commit offsets after writing to Redis — is wrong.
By the time window `N` is written, `poll()` has usually already returned
messages belonging to window `N+1`, which are still sitting in an open in-memory
window. Committing the consumer's *current position* would mark those as
processed, and a crash would lose them with no error anywhere.

[`OffsetTracker`](../pipeline/consumer/offsets.py) records which window each
message contributed to and releases an offset only once that window has been
written:

```
record(tp, offset=41, window_start=100)
record(tp, offset=42, window_start=105)

release(through=100)  ->  {tp: 42}   # only offset 41 is safe
release(through=105)  ->  {tp: 43}
```

A crash therefore replays every message whose window was still open. Verified:
a `SIGKILL` with 15s of downtime replayed the full backlog, with no missing
events.

### Half two: make the replay harmless

Replayed messages must not double-count. Two properties make this safe:

1. **Windows are aligned to the Unix epoch**, not to process start time, so a
   restarted aggregator computes the same boundaries as the one that died.
2. **A window's contents are a deterministic function of its input events.**

The sink then addresses both keys by `window_start` *alone*, never by payload
content, so a rewrite overwrites in place. See §5.1 — an earlier version keyed
history on the payload, which was not good enough.

### What replay reconstructs depends on the time source

This is the part that is easy to state too strongly. Replay recovers the
**events**; whether it recovers the **windows** depends on §3:

- In `event` mode the replayed events carry their original timestamps, so they
  land back in their original windows and the gap is genuinely repaired.
- In `arrival` mode they are stamped with the time they were re-read, so the
  backlog collapses into whichever window is open at restart. Nothing is lost,
  but the history has a hole and one inflated window.

Measured numbers for both are in §5.

---

## 2. Windowing

### Alignment

Windows of size `N` always start at a multiple of `N` past the epoch. This is
what makes the sink idempotent, and it means two aggregators started seconds
apart produce directly comparable output. The previous implementation opened its
first window at `time.time()` on startup and advanced by `+size` from there, so
every instance had its own arbitrary phase.

### Catch-up

`pop_closed(now)` emits **every** window whose end has passed, oldest first,
including empty ones for intervals with no events. Two bugs this fixes:

- A stall used to collapse all events that arrived during it into a single
  window stamped with a stale start. Bucketing now happens at `add()` time
  rather than at flush time, so each event keeps its own bucket.
- Quiet periods used to produce holes in the time series. They now produce
  explicit zeros, so the dashboard shows a flat line rather than interpolating
  across a gap.

A `max_gap_windows` cap stops a clock jump — a laptop waking from sleep, an NTP
step — from emitting hours of empty windows.

### Grace period

A window is held open for `WINDOW_GRACE_SECONDS` past its end before emission,
so a straggler still lands in the right bucket. Events arriving after that are
counted in `late_events` and dropped. The counter is the point: it makes the
cost of the time-source choice measurable rather than theoretical.

---

## 3. Time source: arrival vs event time

Wikimedia payloads carry a `timestamp`, but it records when the edit hit the
wiki server, and lags wall-clock by seconds — replication lag, clock skew,
batched flushes. Both modes are implemented; `WINDOW_TIME_SOURCE` selects one.

| | `arrival` (default) | `event` |
|---|---|---|
| Bucketed by | poll time | payload timestamp |
| Window closed by | wall clock | watermark |
| Windows close | always on schedule | only as the stream advances |
| Attribution | wrong for delayed events | correct |
| Upstream stall | emits zeros — the stall is visible | windows stall too |
| Crash replay | collapses into the present | rebuilds the original windows |

### Event mode must close on a watermark, not on wall clock

The first version of event mode bucketed by event time but still decided
*closure* by wall clock. Because Wikimedia timestamps lag several seconds, every
window was closed while events belonging to it were still arriving, and all of
them were then dropped as late. A live run showed whole windows missing from the
history.

Event mode now closes on the **watermark** — the highest timestamp observed so
far — so a window closes only once the stream itself has moved past it. That is
the same idea a real stream processor exposes, in its simplest form: a fixed
grace period rather than a watermark that adapts to observed lateness.

The cost is the honest one: if the upstream stalls, the watermark stops
advancing and so do the windows. That is precisely why `arrival` is the default
for a *monitoring* dashboard, where a stalled upstream should read as a dip
rather than as a frozen chart.

---

## 4. Partitioning and scale-out

The producer keys each message by `wiki`. Consequences:

- All edits for one wiki land on one partition and keep their relative order.
- Adding a consumer to the group moves *whole wikis* between instances, so one
  wiki's stream is never split across two independent window states.
- The topic is created explicitly with `KAFKA_PARTITIONS` (default 6). Relying
  on auto-creation gave a single partition, which silently capped the consumer
  group at one useful member no matter how many were started.

The key distribution is skewed — `wikidatawiki` and `commonswiki` alone are
roughly half the firehose — so partitions will not be evenly loaded. Balancing
would mean giving up per-wiki ordering, which is not worth it here.

---

## 5. Failure behaviour

Measured against the live stream at ~35 events/sec, 5s windows.

| Failure | Observed behaviour |
|---|---|
| Aggregator `SIGKILL`ed, `arrival` mode | No events lost, no duplicate points. The 15s backlog replayed into the window open at restart: a 20s hole in the history followed by one window of **742 edits against a median of 184**. |
| Aggregator `SIGKILL`ed, `event` mode | History **fully contiguous, zero duplicates, no spike** — the backlog was reattributed to its original windows. zset and payload hash stayed in step at 19/19 entries. |
| SSE stream drops | Producer reconnects after 5s. Appears as low-count windows, not missing ones. |
| Redis is down | `write` raises, offsets are not released, nothing is committed — so the data replays once Redis returns. The process currently exits rather than retrying; see §6. |
| Aggregator stops entirely | `wiki:latest_window` has a 30s TTL, so it disappears and `/healthz` reports `degraded` rather than the dashboard silently showing stale numbers. |
| Producer outruns the consumer | Consumer lag grows, visible only through Kafka's own tooling today. See §6.1. |
| Clock jump / laptop sleep | Bounded by `max_gap_windows`; resynchronises instead of replaying the gap. |
| Malformed SSE payload | Dropped and counted. Live rate is ~0.8% — events missing `id`, `type` or `wiki`. |

### 5.1 What the measurements changed

Two claims in this document were wrong before they were tested, and both were
found by killing the process rather than by reading the code.

1. **"A crash recomputes the affected windows"** was stated unconditionally. It
   is only true in `event` mode. In `arrival` mode the replayed backlog is
   stamped with the restart time, producing the 742-edit window above. §1 and §3
   now say so.

2. **The sink was not actually idempotent.** History was a sorted set whose
   *member* was the payload, which deduplicated only when a replay produced
   byte-identical output. A window written partially before the crash and
   recomputed completely afterwards is not byte-identical, so a `SIGKILL` left
   two points for the same instant — observed as `t=…215` appearing twice with
   totals 7 and 144.

   Both keys are now addressed by `window_start` alone: the zset member is the
   window start, and the payload lives in a parallel hash, so a rewrite
   overwrites. The write and its trim run as one Lua script, since trimming the
   two keys separately would leave orphaned payloads behind. Pinned by
   [`test_partial_window_is_corrected_not_duplicated`](../tests/test_sink.py).

---

## 6. Known limitations

Ordered by how much they matter.

1. **No metrics export.** Consumer lag, window flush latency and `late_events`
   are computed but only logged. A Prometheus endpoint plus a Grafana board is
   the single highest-signal addition left.
2. **Partial window state is not checkpointed.** A crash recomputes open windows
   from the replayed log, which works, but a rebalance that moves a partition
   mid-window discards that partition's contribution to the open window on the
   old owner.
3. **No consumer-group rebalance listener.** `OffsetTracker.forget()` exists but
   is not wired to a partition-revoked callback, so a rebalance can leave stale
   pending offsets in memory. They are never committed for a partition the
   instance no longer owns, so this leaks memory rather than corrupting data.
4. **The watermark never regresses and has no idle timeout.** A single event
   with a far-future timestamp would advance it permanently and cause every
   subsequent event to be dropped as late. A real implementation bounds this.
5. **Redis is not durable.** History is capped at `HISTORY_MAX` windows with no
   long-term store, so nothing outside that rolling window can be queried.
6. **No schema enforcement.** Messages are ad-hoc JSON. A Schema Registry with
   Avro plus a compatibility check in CI would catch producer/consumer drift.
7. **Malformed events are counted and dropped**, not routed to a dead-letter
   topic, so they cannot be inspected after the fact.
8. **Single broker, replication factor 1.** Fine for a laptop, not a statement
   about production topology.
