"""
Offset bookkeeping for commit-after-write.

The naive version of "commit after the sink write" is wrong: by the time window
N is written, poll() has usually already handed us messages belonging to window
N+1. Committing the consumer's current position would mark those as processed
even though they are still sitting in an open in-memory window, so a crash
would lose them silently.

OffsetTracker records which window each message contributed to, and releases an
offset only once the window containing it has been durably written.

Releases are per partition because each partition has its own window state: a
quiet partition can still be filling window N while a busy one has already
emitted N+2, and neither should hold the other's offsets back.

Combined with a sink keyed by (window_start, partition), this gives
effectively-once output on top of at-least-once delivery: a crash replays the
tail of the log, the same windows are recomputed, and the sink overwrites the
same fields rather than duplicating them.
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

    def release_partition(
        self, topic: str, partition: int, through_window_start: float
    ) -> int | None:
        """
        Return the offset that is now safe to commit for one partition, given
        that its windows up to and including `through_window_start` have been
        written. None if nothing is releasable.

        The returned value is the *next* offset to read, which is the
        committed-offset convention Kafka expects. Released entries are
        dropped, so calling twice is harmless.
        """
        per_window = self._pending.get((topic, partition))
        if not per_window:
            return None

        done = [w for w in per_window if w <= through_window_start]
        if not done:
            return None

        highest = max(per_window[w] for w in done)
        for window in done:
            del per_window[window]
        return highest + 1

    def forget(self, topic: str, partition: int) -> None:
        """Drop state for a partition we no longer own (consumer rebalance)."""
        self._pending.pop((topic, partition), None)

    @property
    def pending_partitions(self) -> int:
        return sum(1 for v in self._pending.values() if v)
