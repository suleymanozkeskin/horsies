#!/usr/bin/env bash
set -euo pipefail

# Burst-submit doc_scraper workflows to reproduce contention issues.
#
# Usage:
#   COUNT=50 PARALLEL=20 BASE_URL=http://localhost:8000 ./scripts/repro_burst.sh
#
# Outputs JSON responses to a timestamped directory (default under /tmp).

BASE_URL="${BASE_URL:-http://localhost:8000}"
COUNT="${COUNT:-50}"
PARALLEL="${PARALLEL:-20}"
OUT_DIR="${OUT_DIR:-/tmp/horse_burst_$(date +%Y%m%d_%H%M%S)}"
URLS_JSON="${URLS_JSON:-[\"https://httpbin.org/html\",\"https://httpbin.org/robots.txt\",\"https://httpbin.org/json\"]}"

mkdir -p "${OUT_DIR}"

BODY=$(printf '{"urls":%s}' "${URLS_JSON}")
export BASE_URL BODY OUT_DIR

seq 1 "${COUNT}" | xargs -P "${PARALLEL}" -I{} sh -c \
  'curl -s -X POST "$BASE_URL/workflows/scrape-docs" \
     -H "Content-Type: application/json" \
     -d "$BODY" > "$OUT_DIR/wf_{}.json"'

echo "Submitted ${COUNT} workflows."
echo "Responses saved to ${OUT_DIR}"
