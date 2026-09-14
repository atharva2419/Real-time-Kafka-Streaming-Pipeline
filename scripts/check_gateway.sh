#!/usr/bin/env bash
#
# Check that the gateway reaches every UI behind it.
#
#   bash scripts/check_gateway.sh                     # default http://localhost:8000
#   GATEWAY_URL=http://host:8000 bash scripts/check_gateway.sh
#
# Each check is its own command on purpose. Under `set -e`, a failure inside
# `check && echo ok` does not stop the script - only the last command of an &&
# list counts - so a chained version of this can report success after a failed
# check.

set -euo pipefail

BASE="${GATEWAY_URL:-http://localhost:8000}"
ATTEMPTS="${ATTEMPTS:-12}"
SLEEP_SECONDS="${SLEEP_SECONDS:-5}"

# Grafana takes a few seconds longer than everything else to come up.
wait_for() {
  local name="$1" url="$2"
  for attempt in $(seq 1 "$ATTEMPTS"); do
    if curl -sf -o /dev/null "$url"; then
      echo "ok    $name  ($url)"
      return 0
    fi
    sleep "$SLEEP_SECONDS"
  done
  echo "FAIL  $name  ($url)"
  return 1
}

wait_for "app"        "$BASE/"
wait_for "api"        "$BASE/healthz"
wait_for "grafana"    "$BASE/grafana/api/health"
wait_for "dashboard"  "$BASE/grafana/d/wiki-stream/wiki-stream-pipeline"
wait_for "prometheus" "$BASE/prometheus/-/healthy"
wait_for "alerts"     "$BASE/prometheus/api/v1/rules"

# The API's own /metrics must not be reachable from outside.
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/metrics")
if [ "$code" != "404" ]; then
  echo "FAIL  /metrics is exposed on the gateway (HTTP $code)"
  exit 1
fi
echo "ok    /metrics hidden from the gateway"

echo "gateway routes every UI"
