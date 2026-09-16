"""
Shared setup for tests that run against a real ClickHouse.

Each test module gets its own throwaway database, loaded from the real init
files. 03_kafka_source.sql is never loaded: it would start a genuine consumer
on the genuine topic and pull live events into the test database.
"""

import pathlib
import re

import pytest
import requests

from pipeline import config

INIT = pathlib.Path(__file__).resolve().parents[1] / "clickhouse" / "init"
URL = f"http://{config.CLICKHOUSE_HOST}:{config.CLICKHOUSE_PORT}/"

# Everything except the ingest source, in init order.
SCHEMA_FILES = ("01_raw.sql", "02_rollups.sql")


def q(sql: str) -> str:
    response = requests.post(URL, data=sql.encode(), timeout=30)
    if response.status_code != 200:
        raise AssertionError(f"ClickHouse error for:\n{sql}\n\n{response.text}")
    return response.text.strip()


def statements(filename: str, database: str) -> list[str]:
    """One init file, retargeted at `database`, split into statements."""
    text = re.sub(r"--[^\n]*", "", (INIT / filename).read_text(encoding="utf-8"))
    text = re.sub(r"DATABASE IF NOT EXISTS wiki\b", f"DATABASE IF NOT EXISTS {database}", text)
    text = re.sub(r"\bwiki\.", f"{database}.", text)
    return [s.strip() for s in text.split(";") if s.strip()]


def require_clickhouse() -> None:
    try:
        requests.get(URL + "ping", timeout=1).raise_for_status()
    except requests.RequestException as exc:
        pytest.skip(
            f"ClickHouse not reachable at {URL} ({type(exc).__name__}) - "
            "run `docker compose up -d clickhouse`",
            allow_module_level=True,
        )


def create_schema(database: str) -> None:
    q(f"DROP DATABASE IF EXISTS {database} SYNC")
    for filename in SCHEMA_FILES:
        for stmt in statements(filename, database):
            q(stmt)


def drop(database: str) -> None:
    q(f"DROP DATABASE IF EXISTS {database} SYNC")


def truncate(database: str) -> None:
    for table in ("edits", "edits_1m", "edits_1d"):
        q(f"TRUNCATE TABLE {database}.{table}")
