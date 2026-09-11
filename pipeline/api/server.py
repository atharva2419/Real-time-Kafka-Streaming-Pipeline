"""
FastAPI read layer over the Redis window store.

The API never touches Kafka. The aggregator owns the write path, Redis is the
hand-off point, and this process is a stateless reader, so it can be restarted
or scaled without disturbing the stream.

Windows are stored as one field per Kafka partition (see consumer/sink.py), so
every read merges those fields back together via pipeline/merge.py.
"""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

from pipeline import config
from pipeline.consumer.sink import window_key
from pipeline.merge import history_point, merge_partials

log = logging.getLogger("api")
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = aioredis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True
    )
    yield
    await app.state.redis.aclose()


app = FastAPI(title="Wiki Stream API", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def _slices(raw: dict[str, str]) -> list[dict]:
    """The per-partition slices of one window, newest write per partition."""
    return [json.loads(value) for value in raw.values()]


async def _index(r: aioredis.Redis, start: int, stop: int) -> list[str]:
    # redis-py shares one signature between its sync and async clients, so the
    # declared return type is a union the checker cannot resolve here.
    return cast(list[str], await r.zrange(config.HISTORY_KEY, start, stop))


async def read_latest(r: aioredis.Redis) -> dict | None:
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
# Routes
# ---------------------------------------------------------------------------

@app.get("/healthz")
async def healthz():
    """
    Ready only if Redis answers *and* a window was written recently.

    Freshness is judged from the newest window's own timestamp rather than a
    key's TTL, because with several aggregator replicas no single one owns the
    "latest" key.
    """
    try:
        await app.state.redis.ping()
    except Exception as exc:
        return JSONResponse({"status": "error", "redis": str(exc)}, status_code=503)

    latest = await read_latest(app.state.redis)
    if latest is None:
        return JSONResponse(
            {"status": "degraded", "reason": "no windows"}, status_code=503
        )

    age = time.time() - latest["window_end"]
    if age > config.STALE_AFTER_SECONDS:
        return JSONResponse(
            {
                "status": "degraded",
                "reason": "no recent window",
                "age_seconds": round(age, 1),
            },
            status_code=503,
        )
    return {
        "status": "ok",
        "latest_window": latest["window_start"],
        "partitions": latest["partitions"],
    }


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


@app.websocket("/ws")
async def ws(socket: WebSocket):
    await socket.accept()
    r = app.state.redis
    last_sent: tuple[float, int] | None = None

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


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)


if __name__ == "__main__":
    main()
