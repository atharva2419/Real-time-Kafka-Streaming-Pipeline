"""
Merging the per-partition slices of a window.

Each aggregator instance owns a subset of the topic's partitions, so a window is
stored in Redis as one field per partition rather than one blended value (see
consumer/sink.py). Anything that reads a window - the API, or an instance
logging its own contribution - sums those slices back together here.

What merges exactly, and what does not:

  total_edits, bot_vs_human, edit_types
      Exact. Plain sums over disjoint sets of events.

  edits_per_wiki
      Exact, and this is the one real payoff of keying Kafka messages by wiki:
      a wiki's events all land on one partition, so the per-partition maps have
      disjoint key sets and adding them loses nothing.

  top_editors
      Approximate across partitions. A user who edits several wikis is split
      across partitions, and each slice carries only its own top 5, so a user
      sitting just below the cut everywhere can be missed. Exact merging would
      mean storing every user's count in every window. With one aggregator
      (the default) there is a single slice and the result is exact.
"""

TOP_EDITORS = 5


def _window_size(partial: dict) -> float:
    return partial["window_end"] - partial["window_start"]


def merge_partials(partials: list[dict]) -> dict | None:
    """Sum per-partition slices of one window into a single summary."""
    if not partials:
        return None

    start = min(p["window_start"] for p in partials)
    size = _window_size(partials[0])

    total = 0
    bots = 0
    humans = 0
    wikis: dict[str, int] = {}
    types: dict[str, int] = {}
    users: dict[str, int] = {}

    for partial in partials:
        total += partial["total_edits"]
        bots += partial["bot_vs_human"]["bot"]
        humans += partial["bot_vs_human"]["human"]
        for wiki, count in partial["edits_per_wiki"].items():
            wikis[wiki] = wikis.get(wiki, 0) + count
        for kind, count in partial["edit_types"].items():
            types[kind] = types.get(kind, 0) + count
        for editor in partial["top_editors"]:
            users[editor["user"]] = users.get(editor["user"], 0) + editor["count"]

    top = sorted(users.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_EDITORS]

    return {
        "window_start": start,
        "window_end": start + size,
        "total_edits": total,
        "edits_per_second": round(total / size, 2) if size else 0,
        "edits_per_wiki": dict(sorted(wikis.items(), key=lambda kv: (-kv[1], kv[0]))),
        "bot_vs_human": {"bot": bots, "human": humans},
        "edit_types": dict(sorted(types.items(), key=lambda kv: (-kv[1], kv[0]))),
        "top_editors": [{"user": user, "count": count} for user, count in top],
        "partitions": len(partials),
    }


def history_point(partials: list[dict]) -> dict | None:
    """
    The compact record the dashboard charts.

    Sums only the fields a chart needs rather than going through
    merge_partials, because a dashboard pulls the whole history on connect.
    """
    if not partials:
        return None

    start = min(p["window_start"] for p in partials)
    size = _window_size(partials[0])
    total = sum(p["total_edits"] for p in partials)
    bots = sum(p["bot_vs_human"]["bot"] for p in partials)

    return {
        "t": start,
        "total": total,
        "rate": round(total / size, 2) if size else 0,
        "bot": bots,
    }
