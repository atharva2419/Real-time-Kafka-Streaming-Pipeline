"""
Tests for commit-after-write bookkeeping.

The property under test: an offset is never released until the window that the
message contributed to has been written. Violating that turns a crash into
silent data loss, which is the failure mode the old auto-commit setup had.
"""

from pipeline.consumer.offsets import OffsetTracker

TOPIC = "wiki-edits"


def test_offset_is_withheld_until_its_window_is_written():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=10, window_start=100)
    t.record(TOPIC, 0, offset=11, window_start=105)

    # Window 100 written: offset 10 is safe, 11 is not.
    assert t.release(through_window_start=100) == {(TOPIC, 0): 11}
    # Window 105 written: now 11 is safe.
    assert t.release(through_window_start=105) == {(TOPIC, 0): 12}


def test_commit_is_the_next_offset_to_read():
    """Kafka's commit convention is last-processed + 1."""
    t = OffsetTracker()
    t.record(TOPIC, 3, offset=41, window_start=10)
    assert t.release(10) == {(TOPIC, 3): 42}


def test_highest_offset_per_window_wins():
    t = OffsetTracker()
    for offset in (5, 6, 7):
        t.record(TOPIC, 0, offset=offset, window_start=100)
    assert t.release(100) == {(TOPIC, 0): 8}


def test_out_of_order_offsets_do_not_lower_the_watermark():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=9, window_start=100)
    t.record(TOPIC, 0, offset=4, window_start=100)
    assert t.release(100) == {(TOPIC, 0): 10}


def test_partitions_are_tracked_independently():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=100, window_start=50)
    t.record(TOPIC, 1, offset=7, window_start=50)
    t.record(TOPIC, 2, offset=3, window_start=55)

    assert t.release(50) == {(TOPIC, 0): 101, (TOPIC, 1): 8}


def test_release_spanning_several_windows_takes_the_max():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=1, window_start=100)
    t.record(TOPIC, 0, offset=2, window_start=105)
    t.record(TOPIC, 0, offset=3, window_start=110)

    assert t.release(110) == {(TOPIC, 0): 4}


def test_release_is_idempotent():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=1, window_start=100)

    assert t.release(100) == {(TOPIC, 0): 2}
    assert t.release(100) == {}
    assert t.release(200) == {}


def test_nothing_to_release_yields_no_commit():
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=1, window_start=105)
    assert t.release(100) == {}


def test_forget_drops_a_revoked_partition():
    """After a rebalance we must not commit offsets for a partition we lost."""
    t = OffsetTracker()
    t.record(TOPIC, 0, offset=1, window_start=100)
    t.record(TOPIC, 1, offset=1, window_start=100)

    t.forget(TOPIC, 0)
    assert t.release(100) == {(TOPIC, 1): 2}


def test_pending_partitions_reflects_outstanding_work():
    t = OffsetTracker()
    assert t.pending_partitions == 0
    t.record(TOPIC, 0, offset=1, window_start=100)
    t.record(TOPIC, 1, offset=1, window_start=100)
    assert t.pending_partitions == 2
    t.release(100)
    assert t.pending_partitions == 0
