"""
Redis sink for closed windows.

Two properties have to hold at once, and they pull against each other:

  Idempotency   A crash replays the uncommitted tail, so the same window gets
                written again - sometimes with a *different*, more complete
                value than the partial one written just before the crash. The
                rewrite has to overwrite in place.

  Parallelism   Each aggregator instance owns only a subset of the topic's
                partitions, so what it computes for a window is a *slice*, not
                the whole thing.

Keying everything on `window_start` alone satisfies the first and breaks the
second: two replicas both write the same key and the last one wins, so the
window is silently undercounted (measured: 104 stored where the true total was
117). Keying on (window_start, partition) satisfies both. A replay overwrites
exactly the field it wrote before, a replica can only ever touch fields for
partitions it owns, and a rebalance simply moves which instance writes a field.

Layout:

  wiki:history          ZSET  member = score = window_start (the index)
  wiki:win:<start>      HASH  field = partition -> that partition's slice JSON

Readers sum the fields back together (see pipeline/merge.py). The write and its
trim run as one Lua script: atomic, one round trip, and the per-window hashes
are deleted in the same step that drops them from the index, so no orphans.
"""

import json

from pipeline import config

# KEYS[1] = index zset
# ARGV[1] = max history length
# ARGV[2] = per-window hash key prefix
# ARGV[3] = per-window hash TTL seconds
# ARGV[4..] = repeating (window_start, partition, slice json)
_WRITE_WINDOWS_LUA = """
local zkey = KEYS[1]
local maxlen = tonumber(ARGV[1])
local prefix = ARGV[2]
local ttl = tonumber(ARGV[3])

for i = 4, #ARGV, 3 do
  local start = ARGV[i]
  local hkey = prefix .. start
  redis.call('ZADD', zkey, tonumber(start), start)
  redis.call('HSET', hkey, ARGV[i + 1], ARGV[i + 2])
  redis.call('EXPIRE', hkey, ttl)
end

local excess = redis.call('ZCARD', zkey) - maxlen
if excess > 0 then
  local stale = redis.call('ZRANGE', zkey, 0, excess - 1)
  redis.call('ZREMRANGEBYRANK', zkey, 0, excess - 1)
  for _, start in ipairs(stale) do
    redis.call('DEL', prefix .. start)
  end
end

return redis.call('ZCARD', zkey)
"""


def window_key(window_start: float) -> str:
    return f"{config.WINDOW_KEY_PREFIX}{int(window_start)}"


class RedisSink:
    def __init__(self, client) -> None:
        self.redis = client
        self._script = client.register_script(_WRITE_WINDOWS_LUA)

    def write(self, entries: list[tuple[int, dict]]) -> int | None:
        """
        Write `(partition, window summary)` pairs. Returns the history length.

        The per-window hash keys are built inside the script from the window
        start, which means they are not declared in KEYS. That is fine on a
        single Redis and would need hash tags on Redis Cluster.
        """
        if not entries:
            return None

        args: list[object] = [
            config.HISTORY_MAX,
            config.WINDOW_KEY_PREFIX,
            config.WINDOW_TTL_SECONDS,
        ]
        for partition, summary in entries:
            args.append(str(int(summary["window_start"])))
            args.append(str(partition))
            args.append(json.dumps(summary, separators=(",", ":")))

        return self._script(keys=[config.HISTORY_KEY], args=args)
