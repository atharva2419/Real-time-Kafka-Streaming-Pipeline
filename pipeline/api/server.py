"""
FastAPI read layer over both paths.

  hot path   Redis. The last ten minutes in 5-second windows, for the live view.
  cold path  ClickHouse. Days of history, for /api/analytics/*.

The API never touches Kafka, and the two paths fail independently. The
ClickHouse client connects lazily (see pipeline/analytics.py), so a cold-path
outage turns the analytics routes into 503s and leaves the live dashboard, the
WebSocket and /healthz's verdict alone.

Windows are stored in Redis as one field per Kafka partition (see
consumer/sink.py), so every hot-path read merges those fields back together via
pipeline/merge.py.
"""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import redis.asyncio as aioredis
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from pipeline import analytics, config, metrics
from pipeline.consumer.sink import window_key
from pipeline.merge import history_point, merge_partials

log = logging.getLogger("api")
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = aioredis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True
    )
    # Does not connect here. Connecting at startup would let a ClickHouse outage
    # stop the API from booting at all, taking the live dashboard with it.
    app.state.cold = analytics.ColdPath(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_DB,
        connect_timeout=config.CLICKHOUSE_CONNECT_TIMEOUT,
        query_timeout=config.CLICKHOUSE_QUERY_TIMEOUT,
    )
    yield
    await app.state.cold.close()
    await app.state.redis.aclose()


app = FastAPI(title="Wiki Stream API", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Hot path reads
# ---------------------------------------------------------------------------
#
# Timed with `with histogram.time():` inside the body, never `@histogram.time()`
# on the function. prometheus_client's decorator does not understand coroutines:
# on an async def it times only the creation of the coroutine object. Measured:
# a 200ms await recorded as 0.000001s.

def _slices(raw: dict[str, str]) -> list[dict]:
    """The per-partition slices of one window, newest write per partition."""
    return [json.loads(value) for value in raw.values()]


async def _index(r: aioredis.Redis, start: int, stop: int) -> list[str]:
    # redis-py shares one signature between its sync and async clients, so the
    # declared return type is a union the checker cannot resolve here.
    return cast(list[str], await r.zrange(config.HISTORY_KEY, start, stop))


async def read_latest(r: aioredis.Redis) -> dict | None:
    with metrics.redis_read_seconds.labels(op="latest").time():
        members = await _index(r, -1, -1)
        if not members:
            return None
        raw = cast(
            dict[str, str],
            await cast("Awaitable[dict]", r.hgetall(window_key(float(members[0])))),
        )
        if not raw:
            return None
        return merge_partials(_slices(raw))


async def read_history(r: aioredis.Redis, limit: int) -> list[dict]:
    """
    Read the newest `limit` windows, oldest first.

    One HGETALL per window, pipelined into a single round trip. A dashboard
    pulls the whole history on connect, so this is the hot read; if it ever
    mattered, the fix is a reducer writing a merged rollup rather than merging
    on every read.
    """
    with metrics.redis_read_seconds.labels(op="history").time():
        members = await _index(r, -limit, -1)
        if not members:
            return []

        pipe = r.pipeline()
        for member in members:
            pipe.hgetall(window_key(float(member)))
        results = cast(list[dict[str, str]], await pipe.execute())

    points = []
    for raw in results:
        if not raw:
            continue
        point = history_point(_slices(raw))
        if point is not None:
            points.append(point)
    return points


# ---------------------------------------------------------------------------
# Cold path reads
# ---------------------------------------------------------------------------

async def run_cold_query(
    name: str, build: Callable[[], analytics.Query]
) -> list[dict[str, Any]] | JSONResponse:
    """
    Build, run and time one analytics query, translating the two expected
    failures: a request that cannot be answered as asked (400), and ClickHouse
    being unreachable (503). Anything else is a bug and is allowed to be a 500.
    """
    try:
        query = build()
    except analytics.AnalyticsError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)

    try:
        with metrics.clickhouse_query_seconds.labels(query=name).time():
            return await app.state.cold.rows(query)
    except analytics.ColdPathUnavailable as exc:
        metrics.cold_path_unavailable.inc()
        log.warning("cold path unavailable: %s", exc)
        return JSONResponse(
            {"error": "cold path unavailable", "detail": str(exc)}, status_code=503
        )


async def cold_path_state(cold: analytics.ColdPath) -> dict[str, Any]:
    """
    Freshness of the newest event in ClickHouse, for /healthz.

    Bounded to two seconds and never raises: a health check that hangs or 500s
    because a secondary store is slow is worse than one that reports it.
    """
    try:
        rows = await asyncio.wait_for(
            cold.rows(analytics.freshness_query(cold.database)), timeout=2
        )
    except Exception as exc:  # noqa: BLE001 - reported, never propagated
        log.warning("cold path health check failed: %s", exc)
        return {"state": "unavailable"}

    age = rows[0]["age_seconds"] if rows else None
    if age is None:
        return {"state": "empty"}
    return {
        "state": "ok" if age <= config.STALE_AFTER_SECONDS else "stale",
        "age_seconds": round(float(age), 1),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/healthz")
async def healthz():
    """
    Ready only if Redis answers *and* a window was written recently.

    The cold path is reported but does not decide the verdict: this endpoint is
    what CI and the gateway treat as "the live pipeline works", and a ClickHouse
    outage does not stop that. scripts/check_cold_path.sh owns the cold path.

    Its field is `state`, not `status`, on purpose - callers grep for
    `"status":"ok"`, and a nested one would match even when this is degraded.

    Freshness is judged from the newest window's own timestamp rather than a
    key's TTL, because with several aggregator replicas no single one owns the
    "latest" key.
    """
    cold = await cold_path_state(app.state.cold)

    try:
        await app.state.redis.ping()
    except Exception as exc:
        return JSONResponse(
            {"status": "error", "redis": str(exc), "cold_path": cold}, status_code=503
        )

    latest = await read_latest(app.state.redis)
    if latest is None:
        return JSONResponse(
            {"status": "degraded", "reason": "no windows", "cold_path": cold},
            status_code=503,
        )

    age = time.time() - latest["window_end"]
    if age > config.STALE_AFTER_SECONDS:
        return JSONResponse(
            {
                "status": "degraded",
                "reason": "no recent window",
                "age_seconds": round(age, 1),
                "cold_path": cold,
            },
            status_code=503,
        )
    return {
        "status": "ok",
        "latest_window": latest["window_start"],
        "partitions": latest["partitions"],
        "cold_path": cold,
    }


@app.get("/metrics")
async def metrics_endpoint():
    """Scraped by Prometheus. One uvicorn worker, so no multiprocess mode needed."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/latest")
async def latest():
    data = await read_latest(app.state.redis)
    if data is None:
        return JSONResponse({"error": "no window available"}, status_code=404)
    return data


@app.get("/api/history")
async def history(limit: int = 60):
    limit = max(1, min(limit, config.HISTORY_MAX))
    return {"points": await read_history(app.state.redis, limit)}


@app.get("/api/analytics/timeline")
async def analytics_timeline(
    range_: str = Query("24h", alias="range"),
    bucket: str = "1h",
):
    """Edits over time. A missing bucket means the pipeline was down, not zero edits."""
    result = await run_cold_query(
        "timeline",
        lambda: analytics.timeline_query(range_, bucket, config.CLICKHOUSE_DB),
    )
    if isinstance(result, JSONResponse):
        return result
    return {"range": range_, "bucket": bucket, "points": result}


@app.get("/api/analytics/top-wikis")
async def analytics_top_wikis(
    range_: str = Query("24h", alias="range"),
    limit: int = 10,
):
    result = await run_cold_query(
        "top_wikis",
        lambda: analytics.top_wikis_query(range_, limit, config.CLICKHOUSE_DB),
    )
    if isinstance(result, JSONResponse):
        return result
    return {"range": range_, "wikis": result}


@app.get("/api/analytics/top-editors")
async def analytics_top_editors(
    range_: str = Query("24h", alias="range"),
    limit: int = 10,
):
    result = await run_cold_query(
        "top_editors",
        lambda: analytics.top_editors_query(range_, limit, config.CLICKHOUSE_DB),
    )
    if isinstance(result, JSONResponse):
        return result
    return {"range": range_, "editors": result}


@app.websocket("/ws")
async def ws(socket: WebSocket):
    await socket.accept()
    r = app.state.redis
    last_sent: tuple[float, int] | None = None
    metrics.ws_clients.inc()

    try:
        # Prime the client with enough history to draw a full chart.
        await socket.send_json(
            {"type": "history", "points": await read_history(r, config.HISTORY_MAX)}
        )

        while True:
            data = await read_latest(r)
            if data is not None:
                # Track the total as well as the start: with several replicas a
                # window can gain a partition's slice after we first read it,
                # and the client needs that correction.
                fingerprint = (data["window_start"], data["total_edits"])
                if fingerprint != last_sent:
                    last_sent = fingerprint
                    await socket.send_json({"type": "window", "window": data})
            await asyncio.sleep(config.WS_PUSH_INTERVAL)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.warning("websocket closed: %s", exc)
    finally:
        metrics.ws_clients.dec()


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)


if __name__ == "__main__":
    main()
