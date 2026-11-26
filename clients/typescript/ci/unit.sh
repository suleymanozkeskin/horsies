#!/usr/bin/env bash
# The checks of the TypeScript client. The `typescript` job of tests.yml and
# the `ts-unit` job of tests-full.yml run this script, so both jobs run the
# same steps. A package that adds a check adds its command here.
#
# Needs on PATH: `bun` (1.4) and `node` (26). The workflows install the exact
# versions that their `env` block names. `bun test` needs `node`: tests/lint/
# runs each vendored rule test in a Node process.
#
# The first step that fails stops the script, with the exit status of that step.
set -euo pipefail

workspace_folder="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
floor_compiler_folder="$workspace_folder/ci/typescript-floor"
floor_compiler="$floor_compiler_folder/node_modules/.bin/tsc"

print_step_title() {
  printf '\n==> %s\n' "$1"
}

cd "$workspace_folder"

print_step_title "Runtime versions"
printf 'bun %s\nnode %s\n' "$(bun --revision)" "$(node --version)"

print_step_title "Install the workspace from bun.lock"
bun install --frozen-lockfile

print_step_title "Type check with bun check"
bun check

print_step_title "Lint with Oxlint"
bun run lint

print_step_title "Run the tests with bun test"
bun test

# TypeScript 6.0 is the oldest compiler that the published types support.
# This step type checks the workspace projects with it. Build mode reads the
# project list of tsconfig.json, as `bun check` does; `--clean` then removes
# the build info files that build mode writes.
print_step_title "Type check with tsc of TypeScript 6.0"
(cd "$floor_compiler_folder" && bun install --frozen-lockfile)
"$floor_compiler" --version
"$floor_compiler" --build
"$floor_compiler" --build --clean
