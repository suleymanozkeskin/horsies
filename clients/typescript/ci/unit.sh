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
floor_typescript_folder="$floor_compiler_folder/node_modules/typescript"

# The V8 heap bound, in MiB, of each tsc process of the step "Type check with
# tsc of TypeScript 6.0" (`node --max-old-space-size`). The value is the
# largest peak memory footprint of one process of the step over 5 runs, plus
# the spread of that peak over the 5 runs, rounded up to the next 256 MiB.
readonly floor_compiler_heap_bound_megabytes=1280

print_step_title() {
  printf '\n==> %s\n' "$1"
}

# Prints the folder of each project that tsconfig.json references, one per
# line, as the floor compiler resolves them. Fails when tsconfig.json has an
# error or references no project.
print_workspace_projects() {
  node -e '
    const ts = require(process.argv[1]);
    const configPath = process.argv[2];
    const read = ts.readConfigFile(configPath, ts.sys.readFile);
    if (read.error !== undefined) {
      console.error(ts.formatDiagnostics([read.error], formatHost()));
      process.exit(1);
    }
    const directory = require("node:path").dirname(configPath);
    const parsed = ts.parseJsonConfigFileContent(read.config, ts.sys, directory, undefined, configPath);
    if (parsed.errors.length > 0) {
      console.error(ts.formatDiagnostics(parsed.errors, formatHost()));
      process.exit(1);
    }
    const references = parsed.projectReferences ?? [];
    if (references.length === 0) {
      console.error(`error: ${configPath} references no project`);
      process.exit(1);
    }
    for (const reference of references) console.log(reference.path);
    function formatHost() {
      return { getCanonicalFileName: (name) => name, getCurrentDirectory: ts.sys.getCurrentDirectory, getNewLine: () => "\n" };
    }
  ' "$floor_typescript_folder/lib/typescript.js" "$workspace_folder/tsconfig.json"
}

# Runs tsc of TypeScript 6.0 with the heap bound; the arguments go to tsc.
# When the process reaches the bound, prints the bound and how to raise it.
# Returns the exit status of tsc.
run_floor_compiler() {
  local error_output_file
  error_output_file="$(mktemp)"
  local status=0
  node --max-old-space-size="$floor_compiler_heap_bound_megabytes" "$floor_typescript_folder/lib/tsc.js" "$@" \
    </dev/null 2>"$error_output_file" || status=$?
  cat "$error_output_file" >&2
  if grep -q 'JavaScript heap out of memory' "$error_output_file"; then
    printf '\nerror: floor compiler heap bound reached: `tsc %s` needs more than %s MiB of V8 heap.\n' \
      "$*" "$floor_compiler_heap_bound_megabytes" >&2
    printf 'To raise the bound, set floor_compiler_heap_bound_megabytes in ci/unit.sh to the largest peak memory\n' >&2
    printf 'footprint of one process of this step over 5 runs, plus its spread, rounded up to the next 256 MiB.\n' >&2
  fi
  rm -f "$error_output_file"
  return "$status"
}

# Type checks each project that tsconfig.json references with tsc of
# TypeScript 6.0, one process per project, then removes the build info files
# of that project. Checks every project, also after a project fails, so the
# step reports the errors of all projects. Returns the exit status of the
# first project that fails, or 0.
type_check_workspace_projects() {
  local workspace_projects project_folder project_name
  local failed_projects="" first_failure_status=0 status
  workspace_projects="$(print_workspace_projects)"
  while IFS= read -r project_folder; do
    project_name="${project_folder#"$workspace_folder/"}"
    printf -- '-- %s\n' "$project_name"
    status=0
    run_floor_compiler --build "$project_name" || status=$?
    run_floor_compiler --build --clean "$project_name"
    if ((status != 0)); then
      failed_projects="$failed_projects $project_name"
      if ((first_failure_status == 0)); then first_failure_status=$status; fi
    fi
  done <<<"$workspace_projects"
  if ((first_failure_status != 0)); then
    printf '\nerror: the TypeScript 6.0 type check failed in:%s\n' "$failed_projects" >&2
  fi
  return "$first_failure_status"
}

cd "$workspace_folder"

print_step_title "Runtime versions"
printf 'bun %s\nnode %s\n' "$(bun --revision)" "$(node --version)"

# Every pin that a later step uses is installed here, before the first step
# that runs a tool: the workspace (bun.lock; it holds the tests package and
# its TypeScript 7 compiler) and the TypeScript 6.0 compiler of the floor lane
# (ci/typescript-floor/bun.lock). `bun test` runs both compilers: the docs
# check test compiles its fixture page with each of them.
print_step_title "Install the workspace and the floor compiler from their lockfiles"
bun install --frozen-lockfile
(cd "$floor_compiler_folder" && bun install --frozen-lockfile)

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
# (the body adapter, the runtime, the run store) runs on Node too, and so do
# the workflow builder, its checks and the child spec answer, which a child
# build runs in a task child, and the telemetry (the spans and the context
# across `await` with the OpenTelemetry SDK). The schedule checks read time
# zones through the engine's Temporal (V8 on Node, JavaScriptCore on Bun), so
# the schedule tests run on Node too. The seat isolates of SHARED_THREAD
# children run on Node, so their tests run there too.
print_step_title "Run the codec, wire type, zod adapter, child, seat, workflow, telemetry, schedule and stored-form tests on Node"
node --test 'tests/unit/codec/**/*.test.ts' 'tests/unit/wire/**/*.test.ts' 'tests/unit/zod/**/*.test.ts' 'tests/unit/child/**/*.test.ts' 'tests/unit/seats/**/*.test.ts' 'tests/unit/workflow/**/*.test.ts' 'tests/unit/telemetry/**/*.test.ts' 'tests/unit/schedules/**/*.test.ts' 'tests/stored-forms/**/*.test.ts'

# The lint rule package (eslint-plugin/) runs in ESLint, on Node: its
# RuleTester cases, and ESLint and tsc of TypeScript 7 on its fixture project.
print_step_title "Run the lint rule package tests on Node"
bun run --cwd eslint-plugin test

# The ESLint config of the lint rule package (horsies/no-dropped-result and
# @typescript-eslint/switch-exhaustiveness-check) on the client's own code:
# the files of horsies/tsconfig.json, tests/tsconfig.json, examples/tsconfig.json
# and examples/showcase/tsconfig.json. A dropped Result
# or a switch that misses a member of a union fails the step.
print_step_title "Lint the client's own code with ESLint: dropped results and switch cases"
node eslint-plugin/lint-client.ts

# TypeScript 6.0 is the oldest compiler that the published types support.
# This step type checks each project that tsconfig.json references, as
# `bun check` does, with the options of that project's tsconfig.json. Each
# project runs in a tsc process of its own (`tsc --build <project>`), so no
# process holds the programs of all projects. `--clean` then removes the build
# info files that build mode writes.
print_step_title "Type check with tsc of TypeScript 6.0"
run_floor_compiler --version
type_check_workspace_projects

# The public API gate of the published entry points (`horsies`,
# `horsies/zod`, `horsies/rerun/pg`, `horsies/rerun/postgres`): the API
# report of each one against
# tests/public-api/report/, each type that a public declaration names is
# exported by its entry point, and the no-`any` check with the `unknown`
# allow list (tests/public-api/unknown-allowed.json). A difference prints the
# command that writes the reports; a change of the public API needs the
# owner's word.
print_step_title "Public API report and the no-any check"
bun tests/public-api/run.ts check

# The type tests (tests/types/pass/, tests/types/fail/) and the consumer
# project (tests/types/consumer/), with tsc of TypeScript 7, bun check, and
# tsc of TypeScript 6.0 (the floor compiler of the install step).
print_step_title "Type tests and the consumer project in both compiler lanes"
bun tests/types/run.ts

# The docs check (tests/docs-check/): each TypeScript code block of the docs
# pages (website/src/content/docs/typescript/) and of the skill files
# (horsies/.agents/skills/horsies/) compiles against the built package in the
# consumer project, with the same three compilers. A failing block is printed
# with its page and line. The docs pages are held on the docs branch until
# the release; on other branches the step reads the skill files only. A
# missing skill folder fails the step.
print_step_title "Docs check of the TypeScript code blocks in both compiler lanes"
bun tests/docs-check/run.ts
