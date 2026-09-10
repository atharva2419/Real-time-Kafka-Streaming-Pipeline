"""
Tumbling-window aggregation.

Two pieces:

  TumblingWindow  A pure accumulator for one fixed [start, end) interval.
                  It holds no clock, so it is trivially testable.

  WindowManager   Assigns events to windows by timestamp, holds windows open
                  for a grace period, and emits them in order once closed.

Window alignment
----------------
Windows are aligned to the Unix epoch: a window of size N always starts at a
multiple of N. Two aggregators started seconds apart therefore produce
*identical* window boundaries, which is what makes the Redis sink idempotent
(see aggregator.py) and lets replicas be compared directly.

Time source
-----------
Wikimedia payloads carry a `timestamp` field, but it reflects when the edit was
recorded on the wiki server and can lag wall-clock by several seconds
(replication lag, clock skew, batched flushes). Two modes are supported:

  "arrival" (default)  Bucket by the time the event was polled from Kafka.
                       Windows always close on schedule; a badly delayed event
                       lands in the wrong bucket.

  "event"              Bucket by the payload timestamp. Correct attribution,
                       but a stalled upstream stalls the windows, and events
                       arriving more than `grace` seconds after their window
                       closed are counted as late and dropped.

Neither is universally right; the trade-off is the point. `late_events` is
tracked in both modes so the cost of the choice is measurable.
"""

import math
from collections import Counter

TOP_EDITORS = 5


class TumblingWindow:
    """Accumulators for a single fixed [start, start + size) interval."""

    def __init__(self, start: float, size: int) -> None:
        self.start = start
        self.size = size
        self.total = 0
        self.bots = 0
        self.humans = 0
        self.wikis: Counter = Counter()
        self.types: Counter = Counter()
        self.users: Counter = Counter()

    @property
    def end(self) -> float:
        return self.start + self.size

    def add(self, event: dict) -> None:
        self.total += 1
        self.wikis[event.get("wiki") or "unknown"] += 1
        if event.get("bot"):
            self.bots += 1
        else:
            self.humans += 1
        self.types[event.get("type") or "unknown"] += 1
        self.users[event.get("user") or "anonymous"] += 1

    def snapshot(self) -> dict:
        """A JSON-serialisable summary. Deterministic for a given set of events."""
        return {
            "window_start": self.start,
            "window_end": self.end,
            "total_edits": self.total,
            "edits_per_second": round(self.total / self.size, 2),
            "edits_per_wiki": dict(self.wikis.most_common()),
            "bot_vs_human": {"bot": self.bots, "human": self.humans},
            "edit_types": dict(self.types.most_common()),
            "top_editors": [
                {"user": user, "count": count}
                for user, count in self.users.most_common(TOP_EDITORS)
            ],
        }


class WindowManager:
    """
    Routes events into aligned tumbling windows and emits them in order.

    Usage:
        mgr = WindowManager(size_seconds=5, grace_seconds=1.0)
        mgr.add(event, ts=arrival_time)
        for summary in mgr.pop_closed(now=time.time()):
            ...
    """

    def __init__(
        self,
        size_seconds: int = 5,
        grace_seconds: float = 1.0,
        max_gap_windows: int = 12,
    ) -> None:
        if size_seconds <= 0:
            raise ValueError("size_seconds must be positive")
        self.size = size_seconds
        self.grace = grace_seconds
        self.max_gap_windows = max_gap_windows

        self._windows: dict[float, TumblingWindow] = {}
        # Start of the most recently emitted window; None until the first event.
        self._emitted_through: float | None = None
        # Highest timestamp observed so far - the watermark in event-time mode.
        self._max_ts: float | None = None
        self.late_events = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def align(self, ts: float) -> float:
        """Snap a timestamp down to its epoch-aligned window start."""
        return math.floor(ts / self.size) * self.size

    def add(self, event: dict, ts: float) -> bool:
        """
        Route one event into its window.

        Returns True if it was counted, False if it belonged to a window that
        has already been emitted (a late event, which is dropped).
        """
        start = self.align(ts)
        if self._max_ts is None or ts > self._max_ts:
            self._max_ts = ts

        if self._emitted_through is not None and start <= self._emitted_through:
            self.late_events += 1
            return False

        if self._emitted_through is None:
            # First event defines the origin: the previous window counts as
            # already emitted, so we never back-fill history before startup.
            self._emitted_through = start - self.size

        window = self._windows.get(start)
        if window is None:
            window = TumblingWindow(start, self.size)
            self._windows[start] = window
        window.add(event)
        return True

    def pop_closed(self, now: float) -> list[dict]:
        """
        Emit every window whose end (plus grace) has passed, oldest first.

        Intervals with no events are emitted as empty windows so the output is
        a gap-free time series - a dashboard should show a flat line during a
        quiet period, not a hole.
        """
        if self._emitted_through is None:
            return []

        newest_closed = self.align(now - self.grace) - self.size
        if newest_closed <= self._emitted_through:
            return []

        gap = int((newest_closed - self._emitted_through) / self.size)
        if gap > self.max_gap_windows:
            # Clock jumped or we were stalled for a long time. Emit the most
            # recent max_gap_windows and resynchronise rather than flooding
            # the sink with a long tail of empty windows.
            self._emitted_through = newest_closed - self.max_gap_windows * self.size
            self._drop_before(self._emitted_through)

        out: list[dict] = []
        start = self._emitted_through + self.size
        while start <= newest_closed:
            window = self._windows.pop(start, None) or TumblingWindow(start, self.size)
            out.append(window.snapshot())
            start += self.size

        self._emitted_through = newest_closed
        return out

    @property
    def watermark(self) -> float | None:
        """
        Highest timestamp seen so far; None before the first event.

        In event-time mode this is the clock that closes windows. Using wall
        clock instead would close a window while events belonging to it were
        still arriving - Wikimedia timestamps lag by several seconds - and
        every one of those would then be dropped as late.
        """
        return self._max_ts

    @property
    def open_windows(self) -> int:
        return len(self._windows)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _drop_before(self, start: float) -> None:
        for key in [k for k in self._windows if k <= start]:
            self.late_events += self._windows.pop(key).total
