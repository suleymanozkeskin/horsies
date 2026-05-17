#!/usr/bin/env bash
set -euo pipefail

env_file="${1:-}"
if [[ -z "${env_file}" ]]; then
  if [[ -f ".env.planetscale" ]]; then
    env_file=".env.planetscale"
  elif [[ -f "../task-lib/ignored-content/.env.planetscale" ]]; then
    env_file="../task-lib/ignored-content/.env.planetscale"
  fi
fi

if [[ -n "${env_file}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${env_file}"
  set +a
fi

HORSIES_PLANETSCALE_TEST=1 \
cargo test -p horsies-test-worker --test pgbouncer_planetscale_manual -- --ignored --nocapture --test-threads=1
