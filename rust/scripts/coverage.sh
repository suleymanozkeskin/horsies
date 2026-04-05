#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-summary}"
shift || true

REPORT_EXCLUDES=(
  --exclude-from-report horsies-examples
  --exclude-from-report horsies-test-support
  --exclude-from-report horsies-test-worker
  --exclude-from-report horsies-queue-mismatch-tests
)

CORE_TEST_ARGS=(
  --workspace
  --exclude horsies-examples
  --exclude horsies-test-worker
  "${REPORT_EXCLUDES[@]}"
)

FULL_TEST_ARGS=(
  --workspace
  "${REPORT_EXCLUDES[@]}"
)

case "$MODE" in
  summary)
    cargo llvm-cov "${CORE_TEST_ARGS[@]}" "$@"
    ;;
  full)
    cargo llvm-cov "${FULL_TEST_ARGS[@]}" "$@"
    ;;
  html)
    cargo llvm-cov "${CORE_TEST_ARGS[@]}" --html --output-dir target/llvm-cov/html "$@"
    ;;
  full-html)
    cargo llvm-cov "${FULL_TEST_ARGS[@]}" --html --output-dir target/llvm-cov/html "$@"
    ;;
  lcov)
    mkdir -p target/llvm-cov
    cargo llvm-cov "${CORE_TEST_ARGS[@]}" --lcov --output-path target/llvm-cov/lcov.info "$@"
    ;;
  full-lcov)
    mkdir -p target/llvm-cov
    cargo llvm-cov "${FULL_TEST_ARGS[@]}" --lcov --output-path target/llvm-cov/lcov.info "$@"
    ;;
  json)
    mkdir -p target/llvm-cov
    cargo llvm-cov "${CORE_TEST_ARGS[@]}" --json --summary-only --output-path target/llvm-cov/summary.json "$@"
    ;;
  full-json)
    mkdir -p target/llvm-cov
    cargo llvm-cov "${FULL_TEST_ARGS[@]}" --json --summary-only --output-path target/llvm-cov/summary.json "$@"
    ;;
  *)
    cat <<'EOF'
Usage: ./scripts/coverage.sh [summary|full|html|full-html|lcov|full-lcov|json|full-json] [extra cargo-llvm-cov args...]

Modes:
  summary    Run the core coverage suite and print the terminal summary
  full       Run the full workspace coverage suite (includes worker e2e tests)
  html       Generate an HTML report for the core suite in target/llvm-cov/html
  full-html  Generate an HTML report for the full workspace suite
  lcov       Generate target/llvm-cov/lcov.info for the core suite
  full-lcov  Generate target/llvm-cov/lcov.info for the full workspace suite
  json       Generate target/llvm-cov/summary.json (summary-only) for the core suite
  full-json  Generate target/llvm-cov/summary.json (summary-only) for the full workspace suite

The report always excludes non-library workspace crates from reported coverage:
  - horsies-examples
  - horsies-test-support
  - horsies-test-worker
  - horsies-queue-mismatch-tests

The default "core" suite also skips `horsies-test-worker` test execution so you can
get a stable local baseline without a fully provisioned e2e worker environment.
EOF
    exit 1
    ;;
esac
