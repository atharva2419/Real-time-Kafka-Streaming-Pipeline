"""
FastAPI read layer over the Redis window store.

The API never touches Kafka. The aggregator owns the write path, Redis is the
hand-off point, and this process is a stateless reader, so it can be restarted
or scaled without disturbing the stream.
"""

import asyncio
import json
import logging
from collections.abc import Awaitable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

from pipeline import config

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


async def read_latest(r: aioredis.Redis) -> dict | None:
    raw = await r.get(config.LATEST_KEY)
    return json.loads(raw) if raw else None


async def read_history(r: aioredis.Redis, limit: int) -> list[dict]:
    """
    Read the newest `limit` windows, oldest first.

    The zset holds only ordering (member == window_start); the payloads live in
    a parallel hash so a rewritten window overwrites in place. See sink.py.
    """
    # redis-py shares one signature between its sync and async clients, so
    # the declared return type is a union the checker cannot resolve here.
    members = cast(list[str], await r.zrange(config.HISTORY_KEY, -limit, -1))
    if not members:
        return []
    payloads = await cast(
        "Awaitable[list[str | None]]", r.hmget(config.HISTORY_DATA_KEY, members)
    )
    return [json.loads(p) for p in payloads if p]


@app.get("/healthz")
async def healthz():
    """Ready only if Redis answers *and* a window has been written recently."""
    try:
        await app.state.redis.ping()
    except Exception as exc:
        return JSONResponse({"status": "error", "redis": str(exc)}, status_code=503)

    latest = await read_latest(app.state.redis)
    if latest is None:
        # Key has a TTL, so its absence means the aggregator has gone quiet.
        return JSONResponse(
            {"status": "degraded", "reason": "no recent window"}, status_code=503
        )
    return {"status": "ok", "latest_window": latest["window_start"]}


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
    last_sent: float | None = None

    try:
        # Prime the client with enough history to draw a full chart.
        await socket.send_json(
            {"type": "history", "points": await read_history(r, config.HISTORY_MAX)}
        )

        while True:
            data = await read_latest(r)
            if data and data["window_start"] != last_sent:
                last_sent = data["window_start"]
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
