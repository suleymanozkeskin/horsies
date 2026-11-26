#!/usr/bin/env bash
# The bounded run of the showcase on one runtime (scripts/demo.ts), under the watchdog: the whole process tree is
# killed at 2 GiB of memory or at RUN_WALL_SECONDS.
#
#   ACME_DATABASE_URL=postgresql://... HORSIES_NATIVE_ADDON_PATH=... scripts/run.sh <node|bun>
#
# In this workspace the package is not built: each process runs with the dev start module
# (examples/dev/source-package.ts), which loads the dev addon that HORSIES_NATIVE_ADDON_PATH names.
#
# Exit: the exit code of the run; 137 when the watchdog killed it at a bound; 3 when it left processes.
set -euo pipefail

showcase="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
start_module="$(cd "$showcase/../dev" && pwd)/source-package.ts"
# Every entry process that a test or the run starts runs this start module too (scripts/entry-process.ts).
export SHOWCASE_START_MODULE="$start_module"
memory_bound_mb=2048
run_wall_seconds=900

case "${1:-}" in
  node) runtime=(node --import "$start_module") ;;
  bun) runtime=(bun --preload "$start_module") ;;
  *)
    echo "usage: scripts/run.sh <node|bun>" >&2
    exit 2
    ;;
esac

python3 "$showcase/scripts/watchdog.py" --max-mb "$memory_bound_mb" --max-seconds "$run_wall_seconds" -- \
  "${runtime[@]}" "$showcase/scripts/demo.ts"
