"""
Redis sink for closed windows.

Idempotency is the property that matters here: after a crash, the tail of the
Kafka log replays and windows are recomputed, so a window may be written more
than once - and a window that was written *partially* before the crash may be
rewritten with a different (correct) total.

Both keys are therefore addressed by `window_start` alone, never by payload
content, so a rewrite overwrites in place:

  wiki:history       ZSET  member = window_start, score = window_start
  wiki:history_data  HASH  field  = window_start -> compact JSON point
  wiki:latest_window STRING with a TTL, so it disappears if we stop

An earlier version made the payload the ZSET member, which deduplicated only
when the replay produced byte-identical output. A partial window followed by
its complete recomputation is *not* byte-identical, so it left two points for
the same instant.

The write and its trim run as one Lua script: atomic, and one round trip
regardless of batch size. Trimming the ZSET and the hash separately would
otherwise risk leaving orphaned hash fields behind.
"""

import json

from pipeline import config

# KEYS[1] = history zset, KEYS[2] = history hash, KEYS[3] = latest key
# ARGV[1] = max history length
# ARGV[2] = latest payload, ARGV[3] = latest TTL seconds
# ARGV[4..] = repeating (window_start, point json)
_WRITE_WINDOWS_LUA = """
local zkey, hkey, latest = KEYS[1], KEYS[2], KEYS[3]
local maxlen = tonumber(ARGV[1])

for i = 4, #ARGV, 2 do
  local member = ARGV[i]
  redis.call('ZADD', zkey, tonumber(member), member)
  redis.call('HSET', hkey, member, ARGV[i + 1])
end

local excess = redis.call('ZCARD', zkey) - maxlen
if excess > 0 then
  local stale = redis.call('ZRANGE', zkey, 0, excess - 1)
  redis.call('ZREMRANGEBYRANK', zkey, 0, excess - 1)
  if #stale > 0 then
    redis.call('HDEL', hkey, unpack(stale))
  end
end

redis.call('SET', latest, ARGV[2], 'EX', tonumber(ARGV[3]))
return redis.call('ZCARD', zkey)
"""


def history_point(summary: dict) -> str:
    """The compact per-window record the dashboard charts."""
    return json.dumps(
        {
            "t": summary["window_start"],
            "total": summary["total_edits"],
            "rate": summary["edits_per_second"],
            "bot": summary["bot_vs_human"]["bot"],
        },
        sort_keys=True,
        separators=(",", ":"),
    )


class RedisSink:
    def __init__(self, client) -> None:
        self.redis = client
        self._script = client.register_script(_WRITE_WINDOWS_LUA)

    def write(self, summaries: list[dict]) -> int | None:
        """Write closed windows. Returns the resulting history length."""
        if not summaries:
            return None

        args = [
            config.HISTORY_MAX,
            json.dumps(summaries[-1]),
            config.LATEST_TTL_SECONDS,
        ]
        for summary in summaries:
            args.append(str(int(summary["window_start"])))
            args.append(history_point(summary))

        return self._script(
            keys=[config.HISTORY_KEY, config.HISTORY_DATA_KEY, config.LATEST_KEY],
            args=args,
        )
