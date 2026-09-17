"""
Contract tests between the dashboard page and the API.

The page carries its own copies of a few API facts - which ranges exist, how
many seconds a bucket is, which route to call. If those drift, the History tab
breaks quietly: a 400 behind a spinner, or gaps drawn in the wrong place. These
check the page's copies against the real thing.
"""

import json
import pathlib
import re

from pipeline import analytics
from pipeline.api import server

PAGE = pathlib.Path(__file__).resolve().parents[1] / "pipeline" / "api" / "static" / "index.html"


def page() -> str:
    return PAGE.read_text(encoding="utf-8")


def js_object(name: str) -> dict:
    """Read a flat JS object literal like `const NAME = { "a": "b", ... };`."""
    match = re.search(rf"const {name} = (\{{.*?\}});", page(), re.S)
    assert match, f"{name} not found in index.html"
    body = re.sub(r"(\d)_(\d)", r"\1\2", match.group(1))       # 60_000 -> 60000
    body = re.sub(r",\s*}", "}", body)                         # trailing comma
    body = re.sub(r"([{,]\s*)(\w+)\s*:", r'\1"\2":', body)     # bare keys
    return json.loads(body)


def js_const(name: str) -> str:
    match = re.search(rf'const {name} = "([^"]+)";', page())
    assert match, f"{name} not found in index.html"
    return match.group(1)


# ---------------------------------------------------------------------------
# The page only asks for things the API will answer
# ---------------------------------------------------------------------------

def test_every_analytics_route_the_page_calls_exists():
    called = set(re.findall(r"`(/api/analytics/[\w-]+)\?", page()))
    assert called, "no analytics calls found in index.html"

    registered = {getattr(route, "path", None) for route in server.app.routes}
    assert called <= registered, f"page calls routes that do not exist: {called - registered}"


def test_every_range_and_bucket_the_page_requests_is_valid():
    """Each pair is run through the real validator, so none can come back 400."""
    for range_, bucket in js_object("HISTORY_RANGES").items():
        analytics.timeline_query(range_, bucket, "wiki")


def test_the_page_offers_exactly_the_api_ranges():
    buttons = re.findall(r'<button data-range="(\w+)">', page())
    assert buttons == list(js_object("HISTORY_RANGES")) == list(analytics.RANGES)


def test_bucket_and_range_seconds_match_the_api():
    """The page uses these to lay points onto a grid; a mismatch misplaces gaps."""
    assert js_object("BUCKET_SECONDS") == analytics.BUCKETS
    assert js_object("RANGE_SECONDS") == analytics.RANGES


def test_editor_ranges_are_clamped_at_raw_retention():
    """Longer ranges ask for this much instead; it must be exactly what raw keeps."""
    clamp = js_const("EDITOR_MAX_RANGE")
    assert analytics.RANGES[clamp] == analytics.RAW_RETENTION
    analytics.top_editors_query(clamp, 10, "wiki")


# ---------------------------------------------------------------------------
# User-derived text
# ---------------------------------------------------------------------------

def test_table_labels_are_escaped_before_they_become_html():
    """
    Usernames are chosen by Wikipedia users. MediaWiki forbids < and > in them,
    but the page should not depend on an upstream's input rules to stay safe.
    """
    source = page()
    start = source.index("function renderTable")
    body = source[start: source.index("\n}\n", start)]
    assert "${esc(label)}" in body
    assert "${label}" not in body, "a raw label is interpolated into HTML"


# ---------------------------------------------------------------------------
# Switching views
# ---------------------------------------------------------------------------

def test_hidden_views_are_actually_hidden():
    """
    Regression, caught only by rendering the page. The views are toggled with
    the `hidden` attribute, but the browser's built-in [hidden] { display: none }
    loses to any author rule that sets display - and `main` sets display: grid.
    Both tabs rendered stacked, with the History tab highlighted over Live content.
    """
    source = page()
    toggled = re.findall(r'<(\w+) id="view-\w+"', source)
    assert toggled, "no view containers found"

    styles = source[source.index("<style>"): source.index("</style>")]
    for tag in set(toggled):
        if re.search(rf"\b{tag}\s*\{{[^}}]*display\s*:", styles):
            assert "[hidden] { display: none !important; }" in styles, (
                f"<{tag}> sets display, so `hidden` needs an !important override"
            )
