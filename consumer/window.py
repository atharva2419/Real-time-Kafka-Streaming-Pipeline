"""
TumblingWindow — fixed-size, non-overlapping time window aggregator.

Design note on timestamps:
  Wikimedia event payloads carry a Unix "timestamp" field, but it reflects
  the time the edit was recorded on the wiki server and can lag wall-clock
  time by several seconds (replication lag, clock skew, batched flushes).
  Using event timestamps for window assignment would cause frequent
  late-arrival issues in a 5-second window.

  This implementation uses *arrival time* (time.time() at the moment the
  message is polled from Kafka) for window boundary decisions.  This keeps
  the logic simple and deterministic at the cost of occasionally placing a
  very-late event in the wrong window — acceptable for a monitoring dashboard.
"""

import time
from collections import Counter, defaultdict


class TumblingWindow:
    """
    A single tumbling window of fixed duration.

    Lifecycle:
      1. Instantiate — window opens at the current wall-clock time.
      2. Call add(event) for each incoming message.
      3. Call is_closed() to test whether the window has expired.
      4. Call flush() to get the summary dict and advance to the next window.
         flush() resets all accumulators and moves start forward by size_seconds,
         preserving alignment (no drift even if flush is called slightly late).
    """

    def __init__(self, size_seconds: int = 5) -> None:
        self.size = size_seconds
        self._start: float = time.time()
        self._reset()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, event: dict) -> None:
        self._total += 1
        self._wikis[event.get("wiki") or "unknown"] += 1
        if event.get("bot"):
            self._bots += 1
        else:
            self._humans += 1
        self._types[event.get("type") or "unknown"] += 1
        self._users[event.get("user") or "anonymous"] += 1

    def is_closed(self, now: float | None = None) -> bool:
        """Return True if wall-clock time has passed the window's end boundary."""
        return (now if now is not None else time.time()) >= self._start + self.size

    def flush(self) -> dict:
        """
        Snapshot the current window, advance the start pointer, reset state.
        The returned dict is JSON-serialisable.
        """
        top5 = [
            {"user": user, "count": cnt}
            for user, cnt in self._users.most_common(5)
        ]

        summary = {
            "window_start": self._start,
            "window_end": self._start + self.size,
            "total_edits": self._total,
            "edits_per_wiki": dict(self._wikis),
            "bot_vs_human": {"bot": self._bots, "human": self._humans},
            "edit_types": dict(self._types),
            "top_editors": top5,
        }

        # Advance by exactly size_seconds — keeps windows aligned even if
        # flush() is called a few milliseconds late.
        self._start += self.size
        self._reset()
        return summary

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _reset(self) -> None:
        self._total: int = 0
        self._wikis: Counter = Counter()
        self._bots: int = 0
        self._humans: int = 0
        self._types: Counter = Counter()
        self._users: Counter = Counter()
