"""
Tests for commit-after-write bookkeeping.

The property under test: an offset is never released until the window that the
message contributed to has been written. Violating that turns a crash into
silent data loss, which is the failure mode the old auto-commit setup had.

Releases are per partition, because each partition now carries its own window
state and closes its windows on its own clock.
"""

from pipeline.consumer.offsets import OffsetTracker

TOPIC = "wiki-edits"


def test_offset_is_withheld_until_its_window_is_written():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=10, window_start=100)
    t.record(TOPIC, 0, offset=11, window_start=105)

    # Window 100 written: offset 10 is safe, 11 is not.
    assert t.release_partition(TOPIC, 0, 100) == 11
    # Window 105 written: now 11 is safe.
    assert t.release_partition(TOPIC, 0, 105) == 12


def test_commit_is_the_next_offset_to_read():
    """Kafka's commit convention is last-processed + 1."""
    t = OffsetTracker()
    t.record(TOPIC, 3, offset=41, window_start=10)
    assert t.release_partition(TOPIC, 3, 10) == 42


def test_highest_offset_per_window_wins():
    t = OffsetTracker()
    for offset in (5, 6, 7):
        t.record(TOPIC, 0, offset=offset, window_start=100)
    assert t.release_partition(TOPIC, 0, 100) == 8


def test_out_of_order_offsets_do_not_lower_the_watermark():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=9, window_start=100)
    t.record(TOPIC, 0, offset=4, window_start=100)
    assert t.release_partition(TOPIC, 0, 100) == 10


def test_release_spanning_several_windows_takes_the_max():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=1, window_start=100)
    t.record(TOPIC, 0, offset=2, window_start=105)
    t.record(TOPIC, 0, offset=3, window_start=110)

    assert t.release_partition(TOPIC, 0, 110) == 4


def test_release_is_idempotent():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=1, window_start=100)

    assert t.release_partition(TOPIC, 0, 100) == 2
    assert t.release_partition(TOPIC, 0, 100) is None
    assert t.release_partition(TOPIC, 0, 200) is None


def test_nothing_to_release_yields_no_commit():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=1, window_start=105)
    assert t.release_partition(TOPIC, 0, 100) is None


def test_unknown_partition_releases_nothing():
    assert OffsetTracker().release_partition(TOPIC, 7, 100) is None


# ---------------------------------------------------------------------------
# Partition independence - a slow partition must not hold back a fast one
# ---------------------------------------------------------------------------

def test_partitions_are_released_independently():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=100, window_start=50)
    t.record(TOPIC, 1, offset=7, window_start=50)

    assert t.release_partition(TOPIC, 0, 50) == 101
    # Partition 1 still has its offset pending until its own window closes.
    assert t.release_partition(TOPIC, 1, 50) == 8


def test_releasing_one_partition_leaves_the_others_pending():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=1, window_start=100)
    t.record(TOPIC, 1, offset=1, window_start=100)

    t.release_partition(TOPIC, 0, 100)
    assert t.pending_partitions == 1
    assert t.release_partition(TOPIC, 1, 100) == 2


def test_a_partition_ahead_of_another_releases_only_its_own():
    """A busy partition can be two windows ahead of a quiet one."""
    t = OffsetTracker()
    t.record(TOPIC, 3, offset=900, window_start=110)
    t.record(TOPIC, 4, offset=12, window_start=100)

    assert t.release_partition(TOPIC, 3, 110) == 901
    assert t.release_partition(TOPIC, 4, 110) == 13


def test_forget_drops_a_revoked_partition():
    """After a rebalance we must not commit offsets for a partition we lost."""
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=1, window_start=100)
    t.record(TOPIC, 1, offset=1, window_start=100)

    t.forget(TOPIC, 0)
    assert t.release_partition(TOPIC, 0, 100) is None
    assert t.release_partition(TOPIC, 1, 100) == 2


def test_pending_partitions_reflects_outstanding_work():
    t = OffsetTracker()
    assert t.pending_partitions == 0
    t.record(TOPIC, 0, offset=1, window_start=100)
    t.record(TOPIC, 1, offset=1, window_start=100)
    assert t.pending_partitions == 2
    t.release_partition(TOPIC, 0, 100)
    t.release_partition(TOPIC, 1, 100)
    assert t.pending_partitions == 0
