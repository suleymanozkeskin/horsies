#!/usr/bin/env bash
# Runs the demo of each example on one runtime against a new database per example: a worker of the example's profile
# runs in the background, `demo` sends and reads, then the worker stops. The command of the `typescript-examples` lane.
#
#   HORSIES_NATIVE_ADDON_PATH=... EXAMPLES_DATABASE_SERVER=postgresql://user:password@host:port ci/examples.sh <node|bun>
#
# `EXAMPLES_DATABASE_SERVER` names a throwaway PostgreSQL server with no database in the URL; the script creates one
# database per example (`horsies_example_<example>_<runtime>`), drops it at the end of the example, and needs `psql`.
# Each process runs with the dev start module (examples/dev/source-package.ts), which loads the dev addon. A local run
# goes under the watchdog (examples/showcase/scripts/watchdog.py): it bounds memory and wall time of the whole tree.
#
# Exit: 0 when every demo passed; 1 when one failed; 2 for a wrong argument or a missing variable.
set -euo pipefail

workspace="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
start_module="$workspace/examples/dev/source-package.ts"
demo_wall_seconds=180
worker_stop_seconds=30
schema_wait_seconds=60

# The examples that have a demo, and the worker profile of each. `worker-profiles` names its profile `mixed`.
examples=(quick-start basic-tasks custom-queues dynamic-start retries runtime-state schedules web-mount workflows worker-profiles zod-models)

profile_of() {
  case "$1" in
    worker-profiles) echo mixed ;;
    *) echo default ;;
  esac
}

case "${1:-}" in
  node) runtime=(node --import "$start_module") ;;
  bun) runtime=(bun --preload "$start_module") ;;
  *)
    echo "usage: ci/examples.sh <node|bun>" >&2
    exit 2
    ;;
esac

if [ -z "${EXAMPLES_DATABASE_SERVER:-}" ] || [ -z "${HORSIES_NATIVE_ADDON_PATH:-}" ]; then
  echo "set EXAMPLES_DATABASE_SERVER and HORSIES_NATIVE_ADDON_PATH" >&2
  exit 2
fi

server="${EXAMPLES_DATABASE_SERVER%/}"
status=0

run_example() {
  local example="$1" profile database worker_pid
  profile="$(profile_of "$example")"
  database="horsies_example_${example//-/_}_${runtime[0]}"
  psql "$server/postgres" -v ON_ERROR_STOP=1 -q -c "CREATE DATABASE $database"
  export DATABASE_URL="$server/$database"

  printf '\n==> %s on %s (profile %s)\n' "$example" "${runtime[0]}" "$profile"
  "${runtime[@]}" "$workspace/examples/$example.ts" worker --profile "$profile" &
  worker_pid=$!

  # The worker installs the schema at its start; a demo that only reads (the schedules demo) needs it installed.
  local waited=0
  until [ "$(psql "$server/$database" -tAc "SELECT to_regproc('horsies_monitoring_schedules') IS NOT NULL")" = t ]; do
    waited=$((waited + 1))
    if [ "$waited" -ge "$schema_wait_seconds" ]; then
      echo "$example: the worker installed no schema within $schema_wait_seconds seconds" >&2
      break
    fi
    sleep 1
  done

  local demo_status=0
  python3 "$workspace/examples/showcase/scripts/watchdog.py" --max-mb 2048 --max-seconds "$demo_wall_seconds" -- \
    "${runtime[@]}" "$workspace/examples/$example.ts" demo || demo_status=$?

  kill -TERM "$worker_pid" 2>/dev/null || true
  for _ in $(seq 1 "$worker_stop_seconds"); do
    kill -0 "$worker_pid" 2>/dev/null || break
    sleep 1
  done
  kill -KILL "$worker_pid" 2>/dev/null || true
  wait "$worker_pid" 2>/dev/null || true
  psql "$server/postgres" -q -c "DROP DATABASE IF EXISTS $database WITH (FORCE)" || true
  printf '%s: demo exit %s\n' "$example" "$demo_status"

  return "$demo_status"
}

for example in "${examples[@]}"; do
  run_example "$example" || status=1
done

exit "$status"
