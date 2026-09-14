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

The sink then addresses a window by **(`window_start`, partition)**, never by
payload content, so a rewrite overwrites in place. Both halves of that key are
load-bearing: §5.1 and §5.3 are the two bugs that proved it.

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

The producer keys each message by `wiki`, and the topic is created explicitly
with `KAFKA_PARTITIONS` (default 6). Auto-creation gave a single partition, which
silently capped the consumer group at one useful member.

**What the key actually buys.** Not ordering: the aggregation is counting, and
counting does not care what order events arrive in. The real payoff is that a
wiki's events land on exactly one partition, so the per-partition
`edits_per_wiki` maps have *disjoint key sets* and merge back together exactly.
An earlier version of this document argued the key was about ordering; that was
never the reason it mattered.

**Window state is per partition.** Each aggregator keeps a `WindowManager` per
assigned partition and writes one Redis field per partition (see §5.3), so
instances in the group compose instead of colliding. Two consequences:

- Event-time mode advances a watermark per partition rather than one global
  maximum, so a busy partition can no longer make a quiet one's events look late.
- Totals, per-wiki counts, bot ratios and edit types merge exactly.
  `top_editors` does not: each slice carries only its own top 5, so a user
  spread across several wikis can be missed. Exact top-k would mean storing
  every user's count per window; at scale the honest answer is Space-Saving or a
  Count-Min Sketch with a stated error bound.

**The load is skewed, and that is acceptable.** Measured over 141,855 messages:
P3 holds 45.3% and P5 40.2%, the other four between 1.7% and 6.4%. Consumer
parallelism is therefore bounded by the hottest partition, not by the partition
count. Spreading a hot wiki would mean salting the key (`wikidatawiki#0..3`) and
giving up the disjointness that makes per-wiki merging exact.

---

## 5. Failure behaviour

Measured against the live stream at ~35 events/sec, 5s windows.

| Failure | Observed behaviour |
|---|---|
| Aggregator `SIGKILL`ed, `arrival` mode | No events lost, no duplicate points. The 15s backlog replayed into the window open at restart: a 20s hole in the history followed by one window of **742 edits against a median of 184**. |
| Aggregator `SIGKILL`ed, `event` mode | History **fully contiguous, zero duplicates, no spike** — the backlog was reattributed to its original windows. Index and window hashes stayed in step at 19/19 entries. |
| One of two replicas `SIGKILL`ed | Survivor took over all six partitions. History **contiguous, zero duplicates, index and hashes in step at 17/17**, with one 557-edit window where the backlog replayed (arrival mode, as above). |
| SSE stream drops | Producer reconnects after 5s. Appears as low-count windows, not missing ones. |
| Redis is down | `write` raises, offsets are not released, nothing is committed — so the data replays once Redis returns. The process currently exits rather than retrying; see §6. |
| Aggregator stops entirely | No new windows are indexed, so `/healthz` compares the newest window against the wall clock and reports `degraded` rather than the dashboard silently showing stale numbers. |
| Producer outruns the consumer | Consumer lag grows, visible per partition on the Grafana board and alertable above 2000 messages (§7). |
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

### 5.2 Over-strict validation was silently dropping 2.7% of the stream

Adding producer-side validation introduced a bug that the original unvalidated
code did not have. `REQUIRED_FIELDS` included `id`, so any event without one was
discarded — and a 900-event sample of the live firehose showed **every single
dropped event was a `type: "log"` entry with a null `id`**: page moves,
deletions, user creations. All legitimate activity, and none of it needs an `id`,
because the aggregator groups by wiki, type, user and bot flag.

The effect was not just lost volume. The `log` bucket in the edit-type breakdown
on the dashboard was systematically undercounted, which is the kind of error that
looks like a plausible data distribution rather than a bug.

`REQUIRED_FIELDS` is now `("type", "wiki")` — the two fields the aggregator
actually groups by. Pinned by
[`test_log_events_without_an_id_are_kept`](../tests/test_producer.py).

The general lesson: a validation rule is a claim about which data is worthless,
and it deserves to be checked against real traffic rather than assumed.

### 5.3 "Scale out by adding consumers" was false

This document and the README both said extra group members would scale the
aggregator horizontally. Running it with two replicas showed otherwise:

| Window 23:45:30 | replica A (P0-P2) | replica B (P3-P5) | Redis stored | True |
|---|---|---|---|---|
| total edits | 13 | 104 | **104** | 117 |

Each replica only ever sees its own partitions, so what it computes for a window
is a *slice*. Keyed on `window_start` alone, both wrote the same key and the last
write won: an 11% undercount here, and 89% had the other replica landed last. The
general lesson is worth keeping: **overwrite-by-key idempotency and horizontal
partitioning conflict whenever the key omits a dimension the writers are split
on.**

The fix adds that dimension. A window is a hash keyed by `window_start` with one
field per partition, indexed by a sorted set of window starts:

```
wiki:history        ZSET  member = score = window_start
wiki:win:<start>    HASH  field = partition -> that partition's slice
```

A replay overwrites exactly the field it wrote before, a replica can only touch
fields for partitions it owns, and a rebalance just changes who writes a field.
The index also makes duplicate history points *structurally* impossible, because
a sorted-set member is unique. Readers sum the fields back together
([`pipeline/merge.py`](../pipeline/merge.py)).

Verified live with two replicas: 20 + 199 = 219, 11 + 228 = 239, 12 + 231 = 243.
The API's merged totals match the replicas' own logs exactly, and every breakdown
in `/api/latest` sums to the total. Pinned by
[`test_two_replicas_do_not_overwrite_each_other`](../tests/test_sink.py).

---

## 6. Known limitations

Ordered by how much they matter.

1. **Alerts are defined but not routed.** `observability/alerts.yml` is
   evaluated by Prometheus and visible on its `/alerts` page, but there is no
   Alertmanager, so nothing pages anyone.
2. **`top_editors` is approximate across replicas.** Each partition slice
   carries only its own top 5, so a user spread thinly across several wikis can
   be missed. Totals, per-wiki counts, bot ratio and edit types stay exact, and
   with a single aggregator (the default) there is one slice and top-k is exact
   too.
3. **No consumer-group rebalance listener.** `OffsetTracker.forget()` exists but
   is not wired to a partition-revoked callback, so a rebalance leaves stale
   window state and pending offsets behind for partitions the instance no longer
   owns. Nothing is committed for them, so this leaks memory rather than
   corrupting data, but the open window on the old owner is dropped instead of
   flushed and the new owner rebuilds it from the replayed log.
4. **The watermark never regresses and has no idle timeout.** A single event
   with a far-future timestamp would advance it permanently and cause every
   subsequent event to be dropped as late. A real implementation bounds this.
5. **Redis is not durable.** History is capped at `HISTORY_MAX` windows with no
   long-term store, so nothing outside that rolling window can be queried.
6. **Reads merge on every request.** A connecting dashboard pulls `HISTORY_MAX`
   windows, one pipelined `HGETALL` each. Fine at this size; the fix if it ever
   mattered is a reducer writing a merged rollup.
7. **The Lua script builds the window keys itself**, so they are not declared in
   `KEYS`. Correct on a single Redis; Redis Cluster would need hash tags.
8. **No schema enforcement.** Messages are ad-hoc JSON. A Schema Registry with
   Avro plus a compatibility check in CI would catch producer/consumer drift.
9. **Malformed events are counted and dropped**, not routed to a dead-letter
   topic, so they cannot be inspected after the fact.
10. **Single broker, replication factor 1.** Fine for a laptop, not a statement
    about production topology.

---

## 7. Observability

Metrics live on a plane separate from the data path: Prometheus *pulls* each
process's `/metrics` on a timer, so nothing in the write or read path depends on
it. If Prometheus stops, the pipeline does not notice.

That separation matters more than it sounds. The app dashboard reads Redis
through the aggregator's own output, so it goes blank exactly when the pipeline
breaks - the moment you most need to see what is happening. Scraping the
processes directly keeps reporting through an outage.

### What is measured

| Signal | Type | Answers |
|---|---|---|
| `wiki_events_produced_total`, `wiki_events_consumed_total` | counters | Is the aggregator keeping up with the producer? |
| `kafka_consumergroup_lag` (from `kafka-exporter`) | gauge | How far behind is each partition? |
| `wiki_window_emit_delay_seconds` | histogram | Is the freshness target holding? |
| `wiki_late_events_total`, `wiki_events_dropped_total{reason}` | counters | What do the time-source and validation choices cost? |
| `wiki_produce_errors_total` | counter | Ingest loss that used to be silent (§6.1) |
| `wiki_sink_write_seconds`, `wiki_sink_errors_total` | histogram, counter | Redis health |
| `wiki_assigned_partitions`, `wiki_open_windows` | gauges | Is every partition owned? What would a crash recompute? |
| `wiki_watermark_lag_seconds{partition}` | gauge | Per-partition liveness; in event mode, how far behind real time the stream is |

### Three decisions worth defending

**Lag is read from the broker, not from the app.** The aggregator could compute
its own lag, but that metric disappears the moment the aggregator dies -
precisely when it matters. `kafka-exporter` reads it from the broker and keeps
reporting through the outage.

**Counters, not gauges, for anything event-driven.** Windows are 5 s and the
scrape interval is 5 s, so a gauge that changes once per window can be missed
between scrapes. `rate()` over a counter is exact regardless of scrape timing.

**`partition` is the only per-series label.** It has six values. `wiki` has
hundreds and would multiply every series by that - the classic cardinality
mistake. Per-wiki counts belong in Redis, where they already are.

### Measured baselines

Worth writing down, because "normal" is not obvious here:

| | |
|---|---|
| Produced vs consumed | ~28/s each, tracking within noise |
| Consumer lag | 90-110 messages total, **not zero by design** |
| Window emit delay | p99 **1.5 s** against the 2.5 s target |
| Redis write | p99 ~2.5 ms |
| Drops, late events, produce errors | 0 |

Consumer lag never reaches zero because offsets are not committed until the
window they fed has been written (§1), so about one window per partition is
always outstanding. An alert on "lag > 0" would fire forever; the rule uses 2000.

The emit-delay figure is the first measurement of a number that had only been
derived: grace (1 s) + poll (≤0.5 s) + write. Measured p99 is 1.5 s.

### One front door, separate processes

Grafana and Prometheus sit behind an nginx gateway at `/grafana/` and
`/prometheus/`, so the whole stack is one address. They are not folded into the
app, because a monitoring view must outlive the thing it monitors, and the
gateway only routes. Stopping each piece in turn:

| Stopped | `/` | `/grafana/` | `/prometheus/` |
|---|---|---|---|
| the API | 502 | 200 | 200 |
| the obs profile | 200 | 503 + instructions | 503 + instructions |

Three details that fail silently if they are wrong, each pinned by a test:

- **Upstreams resolve per request.** A literal `proxy_pass http://grafana:3000`
  is resolved at startup, so the gateway would refuse to boot whenever the obs
  profile is off. A resolver plus variables defers the lookup.
- **Headers are set at server level only.** nginx does not merge
  `proxy_set_header`: a location that sets one header drops all the inherited
  ones, including `Host`, which breaks Grafana's origin check.
- **The two sub-path strategies are paired.** Grafana serves from `/grafana/`
  itself, so nginx passes the prefix through; Prometheus serves from its root with
  `--web.external-url`, so nginx strips it. Change one side without the other and
  the UI loads with every asset path broken.

One claim did not survive measurement. The config originally justified a long
WebSocket read timeout by saying nginx's 60 s default would drop the dashboard
socket during an outage. It would not: uvicorn pings every 20 s, which counts as
traffic. With the timeout at its default, the socket stayed open through 90 s of
application silence. The longer timeout stays as insurance for a server that
does not ping, and the comment now says so.

### A rule that could not fire

The first stall rule was `sum(rate(wiki_windows_emitted_total[1m])) == 0`, which
reads correctly and is wrong. A dead process publishes no series at all, so the
expression returns an *empty vector* and there is nothing for `== 0` to match.
Freezing the aggregator for 250 seconds left the alert `inactive` the entire
time - it could not detect the one failure it was written for.

`up == 0` is what catches a process that is down, frozen or unreachable, and it
now leads the rule set as `TargetDown`. The stall rules keep their `rate() == 0`
form, with an `absent()` arm, for the other shape of failure: alive and
scrapeable, but no longer doing its job.

Measured on the fixed rules, with the aggregator paused:

| Elapsed | Lag | State |
|---|---|---|
| t+20s | 949 | `TargetDown` pending |
| t+80s | 3,522 | `TargetDown` firing |
| t+140s | 6,176 | `PipelineStalled` firing |
| t+200s | 8,637 | `ConsumerLagHigh` firing |

The general lesson is the same one §5 keeps repeating: an alert is a claim about
a failure, and until the failure has been staged, it is an untested claim.

### What is missing

No Alertmanager, so the eight rules are a working expression and a description
rather than a page. No tracing - at this size, per-window structured logs carry
more than spans would. And the API runs a single uvicorn worker, so the metrics
registry needs no multiprocess mode; that assumption breaks if it is ever scaled
with workers instead of replicas.
