#!/usr/bin/env bash
#
# Run test modules that need a live service and fail if any of them skipped.
#
#   bash scripts/assert_no_skips.sh tests/test_sink.py tests/test_rollups.py
#   PYTHON=.venv/Scripts/python.exe bash scripts/assert_no_skips.sh tests/test_sink.py
#
# Those modules skip rather than fail when Redis or ClickHouse is unreachable,
# which is right on a laptop and wrong in CI, where a skip means the tests that
# guard idempotency and rollup correctness quietly did not run.
#
# addopts is overridden on purpose. pyproject.toml already passes -q, and a
# second -q suppresses pytest's summary line - so the previous version of this
# check, which grepped that summary for "skipped", could never fire.

set -uo pipefail

PYTHON="${PYTHON:-python}"

out=$("$PYTHON" -m pytest "$@" -o addopts="" -q -rs 2>&1)
status=$?
echo "$out" | tail -6

if [ "$status" -ne 0 ]; then
  exit "$status"
fi

if echo "$out" | grep -qi "skipped"; then
  echo "FAIL  service-backed tests were skipped: a required service was not reachable"
  exit 1
fi

echo "ok    no service-backed tests skipped"
