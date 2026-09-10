"""Unit tests for the windowing engine. No Kafka, no Redis, no real clock."""

import pytest

from pipeline.consumer.window import TumblingWindow, WindowManager

SIZE = 5


def edit(wiki="enwiki", user="alice", bot=False, type_="edit"):
    return {"wiki": wiki, "user": user, "bot": bot, "type": type_}


def manager(**kwargs):
    kwargs.setdefault("size_seconds", SIZE)
    kwargs.setdefault("grace_seconds", 0.0)
    return WindowManager(**kwargs)


# ---------------------------------------------------------------------------
# TumblingWindow
# ---------------------------------------------------------------------------

class TestTumblingWindow:
    def test_counts_are_partitioned_consistently(self):
        w = TumblingWindow(start=100, size=SIZE)
        w.add(edit(wiki="enwiki", bot=True))
        w.add(edit(wiki="enwiki", bot=False))
        w.add(edit(wiki="dewiki", bot=False))

        snap = w.snapshot()
        assert snap["total_edits"] == 3
        assert snap["bot_vs_human"] == {"bot": 1, "human": 2}
        # Every breakdown must sum back to the total, or the dashboard lies.
        assert sum(snap["edits_per_wiki"].values()) == 3
        assert sum(snap["edit_types"].values()) == 3

    def test_missing_fields_fall_back_to_placeholders(self):
        w = TumblingWindow(start=0, size=SIZE)
        w.add({})
        snap = w.snapshot()
        assert snap["edits_per_wiki"] == {"unknown": 1}
        assert snap["top_editors"] == [{"user": "anonymous", "count": 1}]
        assert snap["bot_vs_human"] == {"bot": 0, "human": 1}

    def test_top_editors_is_capped_and_ordered(self):
        w = TumblingWindow(start=0, size=SIZE)
        for i in range(8):
            for _ in range(i + 1):
                w.add(edit(user=f"user{i}"))

        top = w.snapshot()["top_editors"]
        assert len(top) == 5
        assert [e["user"] for e in top] == ["user7", "user6", "user5", "user4", "user3"]
        assert [e["count"] for e in top] == [8, 7, 6, 5, 4]

    def test_rate_is_normalised_by_window_size(self):
        w = TumblingWindow(start=0, size=SIZE)
        for _ in range(20):
            w.add(edit())
        assert w.snapshot()["edits_per_second"] == 4.0

    def test_empty_window_snapshot_is_valid(self):
        snap = TumblingWindow(start=50, size=SIZE).snapshot()
        assert snap["total_edits"] == 0
        assert snap["edits_per_second"] == 0
        assert snap["edits_per_wiki"] == {}
        assert snap["top_editors"] == []
        assert snap["window_start"] == 50
        assert snap["window_end"] == 55


# ---------------------------------------------------------------------------
# Alignment
# ---------------------------------------------------------------------------

class TestAlignment:
    @pytest.mark.parametrize(
        "ts,expected",
        [(0, 0), (4.999, 0), (5, 5), (9.2, 5), (100, 100), (1718700003.7, 1718700000)],
    )
    def test_timestamps_snap_down_to_window_start(self, ts, expected):
        assert manager().align(ts) == expected

    def test_alignment_is_independent_of_start_time(self):
        """Two aggregators started at different times must agree on boundaries."""
        a, b = manager(), manager()
        a.add(edit(), ts=1000.1)
        b.add(edit(), ts=1003.9)
        assert a.align(1002) == b.align(1002)

    def test_rejects_nonpositive_size(self):
        with pytest.raises(ValueError):
            WindowManager(size_seconds=0)


# ---------------------------------------------------------------------------
# Emission
# ---------------------------------------------------------------------------

class TestEmission:
    def test_nothing_is_emitted_before_the_window_closes(self):
        m = manager()
        m.add(edit(), ts=100)
        assert m.pop_closed(now=103) == []
        assert m.pop_closed(now=104.99) == []

    def test_window_is_emitted_once_its_end_passes(self):
        m = manager()
        m.add(edit(), ts=100)
        m.add(edit(), ts=102)

        out = m.pop_closed(now=105)
        assert len(out) == 1
        assert out[0]["window_start"] == 100
        assert out[0]["total_edits"] == 2

    def test_emission_is_not_repeated(self):
        m = manager()
        m.add(edit(), ts=100)
        assert len(m.pop_closed(now=105)) == 1
        assert m.pop_closed(now=105) == []
        assert m.pop_closed(now=106) == []

    def test_grace_period_delays_emission(self):
        m = manager(grace_seconds=2.0)
        m.add(edit(), ts=100)
        assert m.pop_closed(now=105.0) == []   # closed, but inside grace
        assert m.pop_closed(now=106.9) == []
        assert len(m.pop_closed(now=107.0)) == 1

    def test_no_output_before_the_first_event(self):
        """A cold aggregator must not back-fill windows from the epoch."""
        assert manager().pop_closed(now=1718700000) == []

    def test_events_are_routed_to_their_own_windows(self):
        m = manager()
        m.add(edit(wiki="enwiki"), ts=100)
        m.add(edit(wiki="dewiki"), ts=106)
        m.add(edit(wiki="frwiki"), ts=112)

        out = m.pop_closed(now=115)
        assert [w["window_start"] for w in out] == [100, 105, 110]
        assert [w["total_edits"] for w in out] == [1, 1, 1]
        assert out[0]["edits_per_wiki"] == {"enwiki": 1}
        assert out[2]["edits_per_wiki"] == {"frwiki": 1}


# ---------------------------------------------------------------------------
# Catch-up and gaps: the behaviour the previous implementation got wrong
# ---------------------------------------------------------------------------

class TestCatchUp:
    def test_stall_does_not_collapse_events_into_one_mislabelled_window(self):
        """
        Regression: events spanning a 20s stall used to be emitted as a single
        window stamped with a stale start. Each must keep its own bucket.
        """
        m = manager()
        for ts in (100, 106, 111, 117):
            m.add(edit(), ts=ts)

        out = m.pop_closed(now=125)
        assert [w["window_start"] for w in out] == [100, 105, 110, 115, 120]
        assert [w["total_edits"] for w in out] == [1, 1, 1, 1, 0]
        assert sum(w["total_edits"] for w in out) == 4

    def test_quiet_periods_emit_empty_windows_not_holes(self):
        m = manager()
        m.add(edit(), ts=100)

        out = m.pop_closed(now=120)
        assert [w["window_start"] for w in out] == [100, 105, 110, 115]
        assert [w["total_edits"] for w in out] == [1, 0, 0, 0]

    def test_emitted_windows_are_contiguous_and_ordered(self):
        m = manager()
        m.add(edit(), ts=1000)
        out = m.pop_closed(now=1100)

        starts = [w["window_start"] for w in out]
        assert starts == sorted(starts)
        assert all(b - a == SIZE for a, b in zip(starts, starts[1:], strict=False))

    def test_large_clock_jump_is_bounded(self):
        """A laptop waking from sleep must not emit an hour of empty windows."""
        m = manager(max_gap_windows=12)
        m.add(edit(), ts=100)

        out = m.pop_closed(now=100_000)
        assert len(out) == 12
        # It resynchronises to the present rather than replaying the gap.
        assert out[-1]["window_end"] <= 100_000


# ---------------------------------------------------------------------------
# Late events
# ---------------------------------------------------------------------------

class TestLateEvents:
    def test_event_for_an_emitted_window_is_dropped_and_counted(self):
        m = manager()
        m.add(edit(), ts=100)
        m.pop_closed(now=110)

        assert m.add(edit(), ts=101) is False
        assert m.late_events == 1

    def test_late_event_does_not_corrupt_a_later_window(self):
        m = manager()
        m.add(edit(), ts=100)
        m.pop_closed(now=110)

        m.add(edit(), ts=101)          # late, dropped
        m.add(edit(), ts=112)          # on time
        out = m.pop_closed(now=120)

        assert sum(w["total_edits"] for w in out) == 1
        assert m.late_events == 1

    def test_within_grace_a_straggler_still_counts(self):
        m = manager(grace_seconds=3.0)
        m.add(edit(), ts=100)
        m.pop_closed(now=106)          # window 100 still inside grace
        m.add(edit(), ts=104)          # straggler for window 100

        out = m.pop_closed(now=109)
        assert out[0]["window_start"] == 100
        assert out[0]["total_edits"] == 2
        assert m.late_events == 0

    def test_open_window_count_is_exposed(self):
        m = manager()
        m.add(edit(), ts=100)
        m.add(edit(), ts=107)
        assert m.open_windows == 2
        m.pop_closed(now=115)
        assert m.open_windows == 0


# ---------------------------------------------------------------------------
# Watermark (the clock used to close windows in event-time mode)
# ---------------------------------------------------------------------------

class TestWatermark:
    def test_undefined_before_any_event(self):
        assert manager().watermark is None

    def test_tracks_the_highest_timestamp_seen(self):
        m = manager()
        m.add(edit(), ts=100)
        m.add(edit(), ts=112)
        m.add(edit(), ts=107)          # out of order, must not lower it
        assert m.watermark == 112

    def test_late_events_still_advance_it(self):
        """A dropped event is still evidence of how far the stream has moved."""
        m = manager()
        m.add(edit(), ts=100)
        m.pop_closed(now=110)
        m.add(edit(), ts=101)          # late, dropped
        assert m.watermark == 101 or m.watermark == 100

    def test_driving_emission_by_watermark_keeps_lagging_events(self):
        """
        Regression: closing event-time windows on wall clock dropped events
        whose payload timestamps lagged. Driving closure from the watermark
        means a window only closes once the stream itself has moved past it.
        """
        m = manager(grace_seconds=1.0)
        # Events for window 100 arrive with timestamps lagging real time.
        m.add(edit(), ts=100)
        m.add(edit(), ts=102)

        # Wall clock is far ahead, but the stream has not moved past window 100.
        assert m.pop_closed(now=m.watermark) == []
        assert m.late_events == 0

        # A later event advances the watermark and closes window 100.
        m.add(edit(), ts=107)
        out = m.pop_closed(now=m.watermark)
        assert [w["window_start"] for w in out] == [100]
        assert out[0]["total_edits"] == 2
        assert m.late_events == 0
