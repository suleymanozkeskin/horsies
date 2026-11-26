#!/usr/bin/env bash
# The showcase tests on one runtime, each test file under the watchdog (2 GiB, TEST_FILE_WALL_SECONDS). The command
# of the `ts-showcase` lane.
#
#   HORSIES_NATIVE_ADDON_PATH=... [ACME_DATABASE_URL=postgresql://...] scripts/test.sh <node|bun>
#
# ACME_DATABASE_URL turns the database tests on; without it they pass with no check. In this workspace each process
# runs with the dev start module (examples/dev/source-package.ts). Node runs one test file at a time: a worker test
# starts a worker of six isolated children and one shared child.
#
# Exit: 0 when every file passed; 1 when one failed or the watchdog stopped it.
set -euo pipefail

showcase="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
start_module="$(cd "$showcase/../dev" && pwd)/source-package.ts"
# Every entry process that a test or the run starts runs this start module too (scripts/entry-process.ts).
export SHOWCASE_START_MODULE="$start_module"
memory_bound_mb=2048
test_file_wall_seconds=900

case "${1:-}" in
  node) runner=(node --import "$start_module" --test --test-concurrency=1) ;;
  bun) runner=(bun test --preload "$start_module") ;;
  *)
    echo "usage: scripts/test.sh <node|bun>" >&2
    exit 2
    ;;
esac

cd "$showcase"
status=0

for file in tests/*.test.ts; do
  printf '\n==> %s (%s)\n' "$file" "$1"
  python3 scripts/watchdog.py --max-mb "$memory_bound_mb" --max-seconds "$test_file_wall_seconds" -- \
    "${runner[@]}" "./$file" || status=1
done

exit "$status"
