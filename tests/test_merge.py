"""
Tests for merging per-partition window slices.

This is the read half of the fix for the two-replica overwrite bug: the sink
stores one slice per partition, and everything that reads a window sums those
slices back together here.
"""

from pipeline.consumer.window import TumblingWindow
from pipeline.merge import history_point, merge_partials


def slice_of(start=100, size=5, events=()):
    w = TumblingWindow(start=start, size=size)
    for event in events:
        w.add(event)
    return w.snapshot()


def edit(wiki="enwiki", user="alice", bot=False, type_="edit"):
    return {"wiki": wiki, "user": user, "bot": bot, "type": type_}


def merged_of(partials) -> dict:
    """merge_partials() for inputs a test knows are non-empty, narrowed for typing."""
    merged = merge_partials(partials)
    assert merged is not None
    return merged


def point_of(partials) -> dict:
    point = history_point(partials)
    assert point is not None
    return point


# ---------------------------------------------------------------------------
# Sums
# ---------------------------------------------------------------------------

def test_totals_are_summed_across_slices():
    a = slice_of(events=[edit()] * 13)
    b = slice_of(events=[edit()] * 104)

    merged = merged_of([a, b])
    assert merged["total_edits"] == 117


def test_bot_and_human_counts_are_summed():
    a = slice_of(events=[edit(bot=True), edit(bot=False)])
    b = slice_of(events=[edit(bot=True)] * 3)

    assert merged_of([a, b])["bot_vs_human"] == {"bot": 4, "human": 1}


def test_edit_types_are_summed():
    a = slice_of(events=[edit(type_="edit"), edit(type_="log")])
    b = slice_of(events=[edit(type_="log")] * 2)

    assert merged_of([a, b])["edit_types"] == {"log": 3, "edit": 1}


def test_rate_is_recomputed_from_the_merged_total():
    """Not the sum of the slices' rates - that would be right only by luck."""
    a = slice_of(events=[edit()] * 10)
    b = slice_of(events=[edit()] * 15)

    merged = merged_of([a, b])
    assert merged["total_edits"] == 25
    assert merged["edits_per_second"] == 5.0


def test_window_bounds_are_preserved():
    merged = merged_of([slice_of(start=100), slice_of(start=100)])
    assert merged["window_start"] == 100
    assert merged["window_end"] == 105


def test_slice_count_is_reported():
    merged = merged_of([slice_of(), slice_of(), slice_of()])
    assert merged["partitions"] == 3


def test_no_slices_merges_to_nothing():
    assert merge_partials([]) is None


def test_single_slice_passes_counts_through():
    events = [edit(wiki="dewiki"), edit(wiki="dewiki", bot=True)]
    merged = merged_of([slice_of(events=events)])

    assert merged["total_edits"] == 2
    assert merged["edits_per_wiki"] == {"dewiki": 2}
    assert merged["bot_vs_human"] == {"bot": 1, "human": 1}


# ---------------------------------------------------------------------------
# Per-wiki counts: exact, because a wiki lives on exactly one partition
# ---------------------------------------------------------------------------

def test_disjoint_wiki_counts_merge_exactly():
    """
    The Kafka key is the wiki, so a wiki's events all land on one partition and
    the slices never overlap. This is the real payoff of that key choice.
    """
    a = slice_of(events=[edit(wiki="enwiki")] * 3)
    b = slice_of(events=[edit(wiki="dewiki")] * 5)

    merged = merged_of([a, b])
    assert merged["edits_per_wiki"] == {"dewiki": 5, "enwiki": 3}
    assert sum(merged["edits_per_wiki"].values()) == merged["total_edits"]


def test_wiki_counts_are_ordered_by_size():
    a = slice_of(events=[edit(wiki="enwiki")] * 2)
    b = slice_of(events=[edit(wiki="commonswiki")] * 9)

    assert list(merged_of([a, b])["edits_per_wiki"]) == ["commonswiki", "enwiki"]


def test_overlapping_wikis_still_sum():
    """Not expected with wiki keying, but the merge must not silently drop one."""
    a = slice_of(events=[edit(wiki="enwiki")] * 2)
    b = slice_of(events=[edit(wiki="enwiki")] * 3)

    assert merged_of([a, b])["edits_per_wiki"] == {"enwiki": 5}


# ---------------------------------------------------------------------------
# Top editors: approximate across slices, and that is documented behaviour
# ---------------------------------------------------------------------------

def test_editor_counts_are_summed_across_slices():
    a = slice_of(events=[edit(user="bot1")] * 4)
    b = slice_of(events=[edit(user="bot1")] * 6)

    assert merged_of([a, b])["top_editors"] == [{"user": "bot1", "count": 10}]


def test_top_editors_is_capped_at_five():
    events = [edit(user=f"u{i}") for i in range(9) for _ in range(i + 1)]
    merged = merged_of([slice_of(events=events), slice_of(events=events)])

    assert len(merged["top_editors"]) == 5
    assert [e["user"] for e in merged["top_editors"]] == ["u8", "u7", "u6", "u5", "u4"]


def test_ties_break_on_name_so_the_merge_is_deterministic():
    a = slice_of(events=[edit(user="b")])
    b = slice_of(events=[edit(user="a")])

    top = merged_of([a, b])["top_editors"]
    assert [e["user"] for e in top] == ["a", "b"]


def test_editor_below_every_slices_cut_is_missed():
    """
    Documents the known inexactness: each slice carries only its own top 5, so
    a user sitting just below the cut on every partition disappears. Exact
    merging would mean storing every user's count in every window.
    """
    spread = [edit(user="spread")] * 3
    a = slice_of(events=[edit(user=f"a{i}") for i in range(5) for _ in range(9)] + spread)
    b = slice_of(events=[edit(user=f"b{i}") for i in range(5) for _ in range(9)] + spread)

    merged = merged_of([a, b])
    assert "spread" not in [e["user"] for e in merged["top_editors"]]
    # The total still counts every one of its edits; only the top-k is lossy.
    assert merged["total_edits"] == 96


# ---------------------------------------------------------------------------
# History points
# ---------------------------------------------------------------------------

def test_history_point_sums_totals_and_bots():
    a = slice_of(events=[edit(bot=True)] * 2 + [edit()])
    b = slice_of(events=[edit(bot=True)] * 4)

    point = point_of([a, b])
    assert point == {"t": 100, "total": 7, "rate": 1.4, "bot": 6}


def test_history_point_matches_the_full_merge():
    a = slice_of(events=[edit()] * 13)
    b = slice_of(events=[edit(bot=True)] * 104)

    point = point_of([a, b])
    merged = merged_of([a, b])
    assert point["total"] == merged["total_edits"]
    assert point["rate"] == merged["edits_per_second"]
    assert point["bot"] == merged["bot_vs_human"]["bot"]


def test_history_point_of_nothing_is_none():
    assert history_point([]) is None


def test_empty_window_is_still_a_point():
    point = point_of([slice_of()])
    assert point == {"t": 100, "total": 0, "rate": 0, "bot": 0}
