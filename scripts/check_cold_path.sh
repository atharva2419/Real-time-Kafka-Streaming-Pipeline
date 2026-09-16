#!/usr/bin/env bash
#
# Check the cold path end to end: events are landing in ClickHouse, they keep
# landing, they are recent, and nothing has been dead-lettered.
#
#   bash scripts/check_cold_path.sh
#   CLICKHOUSE_URL=http://host:8123 bash scripts/check_cold_path.sh
#
# Every check is its own command. Under `set -e` a failure inside
# `check && echo ok` does not stop the script, so chaining them would let a
# failed check report success.

set -euo pipefail

CH="${CLICKHOUSE_URL:-http://localhost:8123}"
ATTEMPTS="${ATTEMPTS:-24}"
SLEEP_SECONDS="${SLEEP_SECONDS:-5}"
# The Kafka engine flushes roughly every 7.5s by default, so a minute is a
# generous bound on how stale the newest row can be.
MAX_AGE_SECONDS="${MAX_AGE_SECONDS:-60}"

query() {
  curl -sf --data-binary "$1" "$CH/"
}

# 1. Rows are arriving at all.
rows=0
for attempt in $(seq 1 "$ATTEMPTS"); do
  rows=$(query "SELECT count() FROM wiki.edits" || echo 0)
  if [ "$rows" -gt 0 ]; then
    break
  fi
  sleep "$SLEEP_SECONDS"
done
if [ "$rows" -le 0 ]; then
  echo "FAIL  no rows in wiki.edits after $((ATTEMPTS * SLEEP_SECONDS))s"
  exit 1
fi
echo "ok    $rows rows in wiki.edits"

# 2. They keep arriving: one flush interval later, there are more.
sleep 15
later=$(query "SELECT count() FROM wiki.edits")
if [ "$later" -le "$rows" ]; then
  echo "FAIL  row count did not grow in 15s ($rows -> $later)"
  exit 1
fi
echo "ok    still ingesting ($rows -> $later)"

# 3. The newest row is recent.
age=$(query "SELECT dateDiff('second', max(ingested_at), now64(3)) FROM wiki.edits")
if [ "$age" -gt "$MAX_AGE_SECONDS" ]; then
  echo "FAIL  newest row is ${age}s old (limit ${MAX_AGE_SECONDS}s)"
  exit 1
fi
echo "ok    newest row is ${age}s old"

# 4. Nothing failed to parse. The producer validates, so this should be zero.
dead=$(query "SELECT count() FROM wiki.edits_errors")
if [ "$dead" -ne 0 ]; then
  echo "FAIL  $dead messages dead-lettered; first error:"
  query "SELECT error FROM wiki.edits_errors ORDER BY seen_at LIMIT 1"
  exit 1
fi
echo "ok    dead-letter table empty"

echo "cold path healthy"
