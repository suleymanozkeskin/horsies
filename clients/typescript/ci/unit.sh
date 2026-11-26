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

# The codec runs in a task child and in a seat isolate on Node as well as on
# Bun, and the zod adapter runs where a contract is checked, so the codec, wire
# type and zod adapter tests and the stored-form tests (the vectors and the
# corpus of Python's texts) run on Node too (`node:test`). The task child code
# (the body adapter, the runtime, the run store) runs on Node too.
print_step_title "Run the codec, wire type, zod adapter, child and stored-form tests on Node"
node --test 'tests/unit/codec/**/*.test.ts' 'tests/unit/wire/**/*.test.ts' 'tests/unit/zod/**/*.test.ts' 'tests/unit/child/**/*.test.ts' 'tests/stored-forms/**/*.test.ts'

# The lint rule package (eslint-plugin/) runs in ESLint, on Node: its
# RuleTester cases, and ESLint and tsc of TypeScript 7 on its fixture project.
print_step_title "Run the lint rule package tests on Node"
bun run --cwd eslint-plugin test

# TypeScript 6.0 is the oldest compiler that the published types support.
# This step type checks the workspace projects with it. Build mode reads the
# project list of tsconfig.json, as `bun check` does; `--clean` then removes
# the build info files that build mode writes.
print_step_title "Type check with tsc of TypeScript 6.0"
(cd "$floor_compiler_folder" && bun install --frozen-lockfile)
"$floor_compiler" --version
"$floor_compiler" --build
"$floor_compiler" --build --clean

# The public API gate of the published entry points (`horsies`,
# `horsies/zod`): the API report of each one against
# tests/public-api/report/, each type that a public declaration names is
# exported by its entry point, and the no-`any` check with the `unknown`
# allow list (tests/public-api/unknown-allowed.json). A difference prints the
# command that writes the reports; a change of the public API needs the
# owner's word.
print_step_title "Public API report and the no-any check"
bun tests/public-api/run.ts check

# The type tests (tests/types/pass/, tests/types/fail/) and the consumer
# project (tests/types/consumer/), with tsc of TypeScript 7, bun check, and
# tsc of TypeScript 6.0 (the floor compiler installed above).
print_step_title "Type tests and the consumer project in both compiler lanes"
bun tests/types/run.ts
