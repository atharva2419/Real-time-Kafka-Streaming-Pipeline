"""
Historical queries over the cold path (ClickHouse).

Two layers, kept apart so the rules can be tested without a database:

  *_query() functions  Validate a request and build SQL plus bound parameters.
                       Pure: no I/O, no client.

  ColdPath             Runs a Query against ClickHouse, connecting lazily.

Why lazy: creating a clickhouse-connect client opens a connection and raises
if ClickHouse is down. Connecting at API startup would let a cold-path outage
take the live dashboard down with it, which defeats the point of having two
independent paths. Instead the first analytics request connects, and a failure
surfaces as ColdPathUnavailable - a 503 on that route, nothing else.

Nothing a caller sends is ever formatted into SQL. Ranges, buckets and limits
are checked against fixed tables of allowed values, and everything reaches
ClickHouse as a typed server-side parameter - including the database name,
bound as an Identifier.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

log = logging.getLogger("analytics")

# How far back a request may look, in seconds.
RANGES: dict[str, int] = {
    "1h": 3_600,
    "6h": 21_600,
    "24h": 86_400,
    "7d": 604_800,
    "30d": 2_592_000,
    "90d": 7_776_000,
}

# Chart resolution, in seconds.
BUCKETS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "1h": 3_600,
    "1d": 86_400,
}

# A day at one-minute resolution. Past this a chart is unreadable and the
# response is large for no benefit, so the caller is asked for a coarser bucket.
MAX_POINTS = 1_440

# Retention, mirroring the TTLs in clickhouse/init. A request past these would
# silently return partial data, which is worse than a clear refusal.
RAW_RETENTION = RANGES["7d"]
MINUTE_RETENTION = RANGES["90d"]

MAX_LIMIT = 50


class AnalyticsError(ValueError):
    """A request that is well-formed but cannot be answered as asked."""


class ColdPathUnavailable(RuntimeError):
    """ClickHouse could not be reached or did not answer in time."""


@dataclass(frozen=True)
class Query:
    sql: str
    parameters: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def parse_range(value: str) -> int:
    if value not in RANGES:
        raise AnalyticsError(f"unknown range {value!r}; allowed: {', '.join(RANGES)}")
    return RANGES[value]


def parse_bucket(value: str) -> int:
    if value not in BUCKETS:
        raise AnalyticsError(f"unknown bucket {value!r}; allowed: {', '.join(BUCKETS)}")
    return BUCKETS[value]


def parse_limit(value: int) -> int:
    if not 1 <= value <= MAX_LIMIT:
        raise AnalyticsError(f"limit must be between 1 and {MAX_LIMIT}, got {value}")
    return value


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------

def timeline_query(range_: str, bucket: str, database: str) -> Query:
    """
    Edits over time. Daily buckets read edits_1d; everything finer reads
    edits_1m and re-buckets it.

    A missing bucket in the result means the pipeline was not running, not that
    Wikipedia had no edits - so no zero rows are invented to fill it.
    """
    seconds = parse_range(range_)
    step = parse_bucket(bucket)

    if step > seconds:
        raise AnalyticsError(f"bucket {bucket} is wider than range {range_}")

    points = seconds // step
    if points > MAX_POINTS:
        raise AnalyticsError(
            f"range {range_} at bucket {bucket} is {points} points; "
            f"the limit is {MAX_POINTS}, use a coarser bucket"
        )

    params = {"db": database, "secs": seconds}

    if step >= BUCKETS["1d"]:
        return Query(
            """
            SELECT toUnixTimestamp(toDateTime(day)) AS t,
                   sum(edits)          AS edits,
                   sum(bot_edits)      AS bot_edits,
                   sum(new_pages)      AS new_pages,
                   uniqMerge(editors)  AS editors
            FROM {db:Identifier}.edits_1d
            WHERE day >= toDate(now() - toIntervalSecond({secs:UInt32}))
            GROUP BY t
            ORDER BY t
            """,
            params,
        )

    if seconds > MINUTE_RETENTION:
        raise AnalyticsError(f"minute-level data is kept for 90 days; range {range_} is longer")

    return Query(
        """
        SELECT toUnixTimestamp(toStartOfInterval(minute, toIntervalSecond({step:UInt32}))) AS t,
               sum(edits)          AS edits,
               sum(bot_edits)      AS bot_edits,
               sum(new_pages)      AS new_pages,
               uniqMerge(editors)  AS editors
        FROM {db:Identifier}.edits_1m
        WHERE minute >= now() - toIntervalSecond({secs:UInt32})
        GROUP BY t
        ORDER BY t
        """,
        {**params, "step": step},
    )


def top_wikis_query(range_: str, limit: int, database: str) -> Query:
    """The busiest wikis over a rolling window, from the minute rollup."""
    seconds = parse_range(range_)
    if seconds > MINUTE_RETENTION:
        raise AnalyticsError(f"minute-level data is kept for 90 days; range {range_} is longer")

    return Query(
        """
        SELECT wiki,
               sum(edits)          AS edits,
               sum(bot_edits)      AS bot_edits,
               uniqMerge(editors)  AS editors
        FROM {db:Identifier}.edits_1m
        WHERE minute >= now() - toIntervalSecond({secs:UInt32})
        GROUP BY wiki
        ORDER BY edits DESC, wiki
        LIMIT {limit:UInt16}
        """,
        {"db": database, "secs": seconds, "limit": parse_limit(limit)},
    )


def top_editors_query(range_: str, limit: int, database: str) -> Query:
    """
    The most active editors, from raw events - the rollups keep only a sketch
    of distinct editors, not per-user counts.

    Counts distinct (partition, offset) pairs rather than rows. The Kafka engine
    is at-least-once and the raw table only collapses redeliveries at merge
    time, so count() could double-count a message that has not merged yet;
    a Kafka coordinate identifies a message exactly once however many times it
    was inserted.
    """
    seconds = parse_range(range_)
    if seconds > RAW_RETENTION:
        raise AnalyticsError(f"top editors reads raw events, which are kept for 7 days; "
                             f"range {range_} is longer")

    return Query(
        """
        SELECT `user`,
               uniqExact(kafka_partition, kafka_offset)                   AS edits,
               uniqExactIf((kafka_partition, kafka_offset), bot = 1)      AS bot_edits,
               uniqExact(wiki)                                            AS wikis
        FROM {db:Identifier}.edits
        WHERE ingested_at >= now64(3) - toIntervalSecond({secs:UInt32})
          AND `user` IS NOT NULL
        GROUP BY `user`
        ORDER BY edits DESC, `user`
        LIMIT {limit:UInt16}
        """,
        {"db": database, "secs": seconds, "limit": parse_limit(limit)},
    )


def freshness_query(database: str) -> Query:
    """
    Seconds since the newest raw event, or NULL if there are none. Restricted to
    the last two daily partitions so it never scans the whole week.
    """
    return Query(
        """
        SELECT if(count() = 0, NULL,
                  dateDiff('millisecond', max(ingested_at), now64(3)) / 1000) AS age_seconds
        FROM {db:Identifier}.edits
        WHERE toDate(ingested_at) >= yesterday()
        """,
        {"db": database},
    )


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

class ColdPath:
    """Lazily connected, self-healing access to ClickHouse."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        connect_timeout: int = 2,
        query_timeout: int = 10,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.connect_timeout = connect_timeout
        self.query_timeout = query_timeout
        self._client: Any = None

    async def _connect(self) -> Any:
        if self._client is None:
            try:
                self._client = await clickhouse_connect.get_async_client(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    connect_timeout=self.connect_timeout,
                    send_receive_timeout=self.query_timeout,
                )
            except Exception as exc:
                raise ColdPathUnavailable(
                    f"cannot reach ClickHouse at {self.host}:{self.port}: {exc}"
                ) from exc
        return self._client

    async def rows(self, query: Query) -> list[dict[str, Any]]:
        client = await self._connect()
        try:
            result = await client.query(query.sql, parameters=query.parameters)
        except OperationalError as exc:
            # Checked first on purpose: clickhouse-connect follows DB-API, where
            # OperationalError *subclasses* DatabaseError, so the order of these
            # clauses decides whether an outage reads as a 503 or a 500.
            await self._reset()
            raise ColdPathUnavailable(f"ClickHouse unreachable: {exc}") from exc
        except DatabaseError:
            # The server answered and rejected the query: a bug, not an outage.
            raise
        except Exception as exc:
            # A dropped connection or timeout below the driver. Forget the client
            # so the next request reconnects instead of reusing a dead one.
            await self._reset()
            raise ColdPathUnavailable(f"ClickHouse query failed: {exc}") from exc
        return [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]

    async def _reset(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.close()
            except Exception:  # noqa: BLE001 - already failing; closing is best effort
                log.debug("error closing a failed ClickHouse client", exc_info=True)

    async def close(self) -> None:
        await self._reset()
