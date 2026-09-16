"""
Route-level tests for the analytics API, plus the timing regression.

None of these need Redis or ClickHouse running: validation happens before any
query, and unavailability is simulated by pointing the cold path at a dead port.
"""

import asyncio
import pathlib
import re
import time

import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry, Histogram

from pipeline import analytics, metrics
from pipeline.api import server

ROOT = pathlib.Path(__file__).resolve().parents[1]


@pytest.fixture
def client():
    with TestClient(server.app) as c:
        yield c


def dead_cold_path() -> analytics.ColdPath:
    return analytics.ColdPath(
        host="localhost", port=8999, username="default", password="",
        database="wiki", connect_timeout=1,
    )


# ---------------------------------------------------------------------------
# Refused requests are 400s with a reason
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "path",
    [
        "/api/analytics/timeline?range=5y",
        "/api/analytics/timeline?range=24h&bucket=10s",
        "/api/analytics/timeline?range=30d&bucket=1m",
        "/api/analytics/top-wikis?range=24h&limit=500",
        "/api/analytics/top-editors?range=30d",
    ],
)
def test_unanswerable_requests_are_400_with_a_reason(client, path):
    response = client.get(path)
    assert response.status_code == 400
    assert response.json()["error"]


# ---------------------------------------------------------------------------
# An unreachable ClickHouse is a 503, and only on the analytics routes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "path",
    ["/api/analytics/timeline", "/api/analytics/top-wikis", "/api/analytics/top-editors"],
)
def test_cold_path_outage_is_a_503_not_a_500(client, path):
    server.app.state.cold = dead_cold_path()
    before = metrics.cold_path_unavailable._value.get()

    response = client.get(path)

    assert response.status_code == 503
    assert response.json()["error"] == "cold path unavailable"
    assert metrics.cold_path_unavailable._value.get() == before + 1


def test_cold_path_health_is_reported_quickly_and_never_raises():
    """/healthz must not hang or 500 because the secondary store is down."""
    started = time.monotonic()
    state = asyncio.run(server.cold_path_state(dead_cold_path()))

    assert state == {"state": "unavailable"}
    assert time.monotonic() - started < 5


def test_cold_path_health_uses_state_not_status():
    """
    Callers detect health by grepping for "status":"ok". A nested "status" in
    the cold-path block could match even when the live path is degraded.
    """
    source = (ROOT / "pipeline" / "api" / "server.py").read_text(encoding="utf-8")
    start = source.index("async def cold_path_state")
    body = source[start: source.index("# ----", start)]
    assert '"state"' in body
    assert '"status"' not in body


# ---------------------------------------------------------------------------
# Timing coroutines: the regression
# ---------------------------------------------------------------------------

def test_the_timer_decorator_does_not_time_coroutines():
    """
    Pins the prometheus_client behaviour that made wiki_api_redis_read_seconds
    record ~1 microsecond for every read: on an async def, @histogram.time()
    times creating the coroutine object, not awaiting it.
    """
    registry = CollectorRegistry()
    decorated = Histogram("decorated_seconds", "probe", registry=registry)
    managed = Histogram("managed_seconds", "probe", registry=registry)

    @decorated.time()
    async def slow_decorated():
        await asyncio.sleep(0.1)

    async def slow_managed():
        with managed.time():
            await asyncio.sleep(0.1)

    asyncio.run(slow_decorated())
    asyncio.run(slow_managed())

    decorated_sum = registry.get_sample_value("decorated_seconds_sum")
    managed_sum = registry.get_sample_value("managed_seconds_sum")
    assert decorated_sum is not None and managed_sum is not None
    assert decorated_sum < 0.01
    assert managed_sum >= 0.09


def test_no_async_function_is_timed_with_the_decorator():
    pattern = re.compile(r"@[\w.()=\"']+\.time\(\)\s*\n\s*async def")
    offenders = [
        str(path.relative_to(ROOT))
        for path in (ROOT / "pipeline").rglob("*.py")
        if pattern.search(path.read_text(encoding="utf-8"))
    ]
    assert not offenders, f"decorator-timed coroutines record nothing: {offenders}"
