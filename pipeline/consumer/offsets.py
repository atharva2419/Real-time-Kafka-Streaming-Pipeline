"""
Offset bookkeeping for commit-after-write.

The naive version of "commit after the sink write" is wrong: by the time
window N is written, poll() has usually already handed us messages belonging
to window N+1. Committing the consumer's current position would mark those as
processed even though they are still sitting in an open in-memory window, so a
crash would lose them silently.

OffsetTracker records which window each message contributed to, and will only
release an offset once the window containing it has been durably written.

Combined with an idempotent sink (windows are keyed by their aligned start, and
a window's contents are a deterministic function of its input events), this
gives effectively-once output on top of at-least-once delivery: a crash replays
the tail of the log, the same windows are recomputed, and the sink overwrites
rather than duplicates.
"""

from collections import defaultdict

# (topic, partition) -> {window_start: highest offset seen for that window}
_Pending = dict[tuple[str, int], dict[float, int]]


class OffsetTracker:
    def __init__(self) -> None:
        self._pending: _Pending = defaultdict(dict)

    def record(self, topic: str, partition: int, offset: int, window_start: float) -> None:
        """Note that `offset` contributed to the window starting at `window_start`."""
        per_window = self._pending[(topic, partition)]
        if offset > per_window.get(window_start, -1):
            per_window[window_start] = offset

    def release(self, through_window_start: float) -> dict[tuple[str, int], int]:
        """
        Return the offsets that are now safe to commit, given that every window
        up to and including `through_window_start` has been written.

        The returned value is the *next* offset to read per partition, which is
        the committed-offset convention Kafka expects. Released entries are
        dropped, so calling twice is harmless.
        """
        commits: dict[tuple[str, int], int] = {}

        for tp, per_window in self._pending.items():
            done = [w for w in per_window if w <= through_window_start]
            if not done:
                continue
            highest = max(per_window[w] for w in done)
            for w in done:
                del per_window[w]
            commits[tp] = highest + 1

        return commits

    def forget(self, topic: str, partition: int) -> None:
        """Drop state for a partition we no longer own (consumer rebalance)."""
        self._pending.pop((topic, partition), None)

    @property
    def pending_partitions(self) -> int:
        return sum(1 for v in self._pending.values() if v)
