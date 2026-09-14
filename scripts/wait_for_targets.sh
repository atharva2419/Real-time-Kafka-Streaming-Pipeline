#!/usr/bin/env bash
#
# Wait until Prometheus reports every scrape target healthy.
#
# Used by CI, and runnable by hand against a local stack:
#
#   bash scripts/wait_for_targets.sh            # expect 6 targets, 30 attempts
#   bash scripts/wait_for_targets.sh 8 60       # expect 8, wait longer
#   PROMETHEUS_URL=http://host:8000/prometheus bash scripts/wait_for_targets.sh
#
# On counting: the targets API returns all of its JSON on a single line, so
# `grep -c` reports 1 no matter how many targets there are - it counts matching
# *lines*. Run `grep -o` first and count the lines it emits. The first version
# of this check got that wrong and could never pass.

set -uo pipefail

URL="${PROMETHEUS_URL:-http://localhost:8000/prometheus}/api/v1/targets?state=active"
EXPECTED="${1:-6}"
ATTEMPTS="${2:-30}"
SLEEP_SECONDS="${SLEEP_SECONDS:-5}"

for attempt in $(seq 1 "$ATTEMPTS"); do
  health=$(curl -sf "$URL" | grep -o '"health":"[a-z]*"' || true)
  total=$(printf '%s\n' "$health" | grep -c '"health"' || true)
  up=$(printf '%s\n' "$health" | grep -c '"health":"up"' || true)

  if [ "$total" -ge "$EXPECTED" ] && [ "$up" -eq "$total" ]; then
    echo "all $up/$total scrape targets up (attempt $attempt)"
    exit 0
  fi

  echo "attempt $attempt/$ATTEMPTS: $up/$total targets up, want >= $EXPECTED"
  sleep "$SLEEP_SECONDS"
done

echo "scrape targets did not all come up"
curl -s "$URL"
exit 1
