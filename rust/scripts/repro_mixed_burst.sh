#!/usr/bin/env bash
set -euo pipefail

# Mixed traffic burst for Horsies showcase.
# Sends workflow starts plus task add/divide waits and optional fetch tasks.
#
# Usage (example):
#   WF_COUNT=200 WF_PARALLEL=50 \
#   ADD_WAIT_COUNT=200 ADD_WAIT_PARALLEL=50 \
#   DIV_WAIT_COUNT=200 DIV_WAIT_PARALLEL=50 \
#   FETCH_COUNT=50 FETCH_PARALLEL=25 \
#   BASE_URL=http://localhost:8000 \
#   ./scripts/repro_mixed_burst.sh

BASE_URL="${BASE_URL:-http://localhost:8000}"
OUT_DIR="${OUT_DIR:-/tmp/horse_mixed_$(date +%Y%m%d_%H%M%S)}"

WF_COUNT="${WF_COUNT:-100}"
WF_PARALLEL="${WF_PARALLEL:-25}"
URLS_JSON="${URLS_JSON:-[\"https://httpbin.org/html\",\"https://httpbin.org/robots.txt\",\"https://httpbin.org/json\"]}"

ADD_WAIT_COUNT="${ADD_WAIT_COUNT:-50}"
ADD_WAIT_PARALLEL="${ADD_WAIT_PARALLEL:-20}"

DIV_WAIT_COUNT="${DIV_WAIT_COUNT:-50}"
DIV_WAIT_PARALLEL="${DIV_WAIT_PARALLEL:-20}"

FETCH_COUNT="${FETCH_COUNT:-25}"
FETCH_PARALLEL="${FETCH_PARALLEL:-10}"
FETCH_URL="${FETCH_URL:-https://httpbin.org/html}"

mkdir -p "${OUT_DIR}"

WF_BODY=$(printf '{"urls":%s}' "${URLS_JSON}")
export BASE_URL OUT_DIR WF_BODY FETCH_URL

echo "Starting mixed burst..."
echo "OUT_DIR=${OUT_DIR}"

(
  seq 1 "${WF_COUNT}" | xargs -P "${WF_PARALLEL}" -I{} sh -c \
    'curl -s -X POST "$BASE_URL/workflows/scrape-docs" \
       -H "Content-Type: application/json" \
       -d "$WF_BODY" > "$OUT_DIR/wf_{}.json"'
) &

(
  seq 1 "${ADD_WAIT_COUNT}" | xargs -P "${ADD_WAIT_PARALLEL}" -I{} sh -c \
    'curl -s -X POST "$BASE_URL/tasks/add/wait?a=1&b=2" > "$OUT_DIR/add_wait_{}.json"'
) &

(
  seq 1 "${DIV_WAIT_COUNT}" | xargs -P "${DIV_WAIT_PARALLEL}" -I{} sh -c \
    'curl -s -X POST "$BASE_URL/tasks/divide/wait?a=10&b=0" > "$OUT_DIR/div_wait_{}.json"'
) &

(
  seq 1 "${FETCH_COUNT}" | xargs -P "${FETCH_PARALLEL}" -I{} sh -c \
    'curl -s -X POST "$BASE_URL/tasks/fetch-page?url=$FETCH_URL" > "$OUT_DIR/fetch_{}.json"'
) &

wait

echo "Mixed burst complete."
