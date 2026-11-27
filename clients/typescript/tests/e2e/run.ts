// The runner of the end-to-end suite:
//
//   cd clients/typescript && bun tests/e2e/run.ts <node|bun> [<layer file name> ...]
//
// - Needs `HORSIES_TEST_DATABASE_URL` (a throwaway PostgreSQL server; there is no default) and
//   `python3` for the watchdog, and the runtime of the lane on `PATH` (`node` 26 or `bun` 1.4). The PgBouncer lane also
//   sets `HORSIES_TEST_DATABASE_URL_TRANSACTION`.
// - The entry of the run is `HORSIES_E2E_ENTRY`: `source` (the default; needs `HORSIES_NATIVE_ADDON_PATH`, the absolute
//   path of a dev build of the addon) or `package` (the built `horsies` package and its platform package, installed by
//   `install-built-package.ts`; the files import `#horsies`, which `tests/package.json` maps to `entry/package.ts`
//   under the condition `horsies-package`). The runner sets that condition for each process of the run.
// - Runs each layer file (`layers.ts`) in its own process, one file at a time: `node --test <file>` or
//   `bun test ./<file>`, under the watchdog (`watchdog.py`: 2 GiB of the process tree and the file's wall bound). The
//   watchdog kills each process of the tree that is left after the file, also after a failure.
// - After each file, drops the databases of the run that a failed test left.
// - Prints the pending scenarios, then one line per file. Exit: 0 when every file passed, 1 when one failed, 2 for
//   arguments or an environment that the suite cannot run with.

import { spawnSync } from "node:child_process";
import { isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";

import { LAYER_FILES, type LayerFile, PENDING_SCENARIOS } from "./layers.ts";
import { dropDatabase, randomHex, RUN_ID_VARIABLE, runDatabases, type ServerSettings, serverSettings, settingsRefusedText } from "./support/database.ts";
import { postgresErrorText } from "./support/postgres/client.ts";

/** The runtimes of the lanes. */
export type Runtime = "node" | "bun";

/** The memory bound of the process tree of one file, in MiB. */
const MEMORY_BOUND_MB = 2_048;

/** The variable of the dev addon path. */
const ADDON_PATH_VARIABLE = "HORSIES_NATIVE_ADDON_PATH";

/** The variable that selects the entry of the run. */
const ENTRY_VARIABLE = "HORSIES_E2E_ENTRY";

/** The entries of a run: the source of the package with the dev addon, or the built package. */
export type EntryKind = "source" | "package";

/** The condition of `imports` in `tests/package.json` that selects the built package. */
const PACKAGE_CONDITION = "horsies-package";

/** The workspace folder: every command runs there. */
const WORKSPACE: string = fileURLToPath(new URL("../..", import.meta.url));

const WATCHDOG: string = fileURLToPath(new URL("./watchdog.py", import.meta.url));

const EXIT_PASSED = 0;
const EXIT_FAILED = 1;
const EXIT_USAGE = 2;

/** What the runner runs: the runtime and the files. */
type RunPlan = { readonly runtime: Runtime; readonly files: readonly LayerFile[] };

/** The end of one file. */
type FileEnd = { readonly file: LayerFile; readonly exit: number; readonly seconds: number; readonly leftDatabases: readonly string[] };

/** The plan of `args`, or the usage error. Pure. */
export function runPlan(args: readonly string[]): { readonly ok: true; readonly plan: RunPlan } | { readonly ok: false; readonly error: string } {
  const [runtime, ...names] = args;

  if (runtime !== "node" && runtime !== "bun") {
    return { ok: false, error: "the first argument is the runtime: node or bun" };
  }

  const unknown = names.filter((name) => !LAYER_FILES.some((file) => file.name === name));

  if (unknown.length > 0) {
    return { ok: false, error: `unknown layer files: ${unknown.join(", ")}; the files: ${LAYER_FILES.map((file) => file.name).join(", ")}` };
  }

  const files = names.length === 0 ? LAYER_FILES : LAYER_FILES.filter((file) => names.includes(file.name));

  return { ok: true, plan: { runtime, files } };
}

/** A command: the program and its arguments. */
export type Command = { readonly program: string; readonly args: readonly string[] };

/** The command of one file on `runtime`, under the watchdog. Pure. */
export function fileCommand(runtime: Runtime, file: LayerFile): Command {
  const path = `tests/e2e/layers/${file.name}.ts`;
  const test = runtime === "node" ? ["node", "--test", "--test-reporter=spec", path] : ["bun", "test", `./${path}`];

  return { program: "python3", args: [WATCHDOG, "--max-mb", String(MEMORY_BOUND_MB), "--max-seconds", String(file.boundSeconds), "--", ...test] };
}

/** The entry that `value` (the variable) names, or the text of the refusal. Pure. */
export function entryOf(value: string | undefined): { readonly ok: true; readonly entry: EntryKind } | { readonly ok: false; readonly error: string } {
  switch (value) {
    case undefined:
    case "source":
      return { ok: true, entry: "source" };
    case "package":
      return { ok: true, entry: "package" };
    default:
      return { ok: false, error: `${ENTRY_VARIABLE} is ${JSON.stringify(value)}; it is source or package` };
  }
}

/** The environment that selects `entry` in every process of the run: the condition for the runtime's own option variable. Pure. */
export function entryEnvironment(runtime: Runtime, entry: EntryKind): Readonly<Record<string, string>> {
  switch (entry) {
    case "source":
      return {};
    case "package":
      return runtime === "node" ? { NODE_OPTIONS: `--conditions=${PACKAGE_CONDITION}` } : { BUN_OPTIONS: `--conditions=${PACKAGE_CONDITION}` };
  }
}

/** The settings of the run, or why the suite cannot run. Effects: reads the environment. */
function runSettings(runId: string): { readonly ok: true; readonly settings: ServerSettings; readonly entry: EntryKind } | { readonly ok: false; readonly error: string } {
  const settings = serverSettings((name) => (name === RUN_ID_VARIABLE ? runId : process.env[name]));

  if (!settings.ok) {
    return { ok: false, error: settingsRefusedText(settings.error) };
  }

  const entry = entryOf(process.env[ENTRY_VARIABLE]);

  if (!entry.ok) {
    return { ok: false, error: entry.error };
  }

  const addon = process.env[ADDON_PATH_VARIABLE] ?? "";

  if (entry.entry === "source" && !isAbsolute(addon)) {
    return { ok: false, error: `${ADDON_PATH_VARIABLE}: set it to the absolute path of a dev build of the addon (bun run --cwd bridge build:dev)` };
  }

  return { ok: true, settings: settings.value, entry: entry.entry };
}

/** Run one file and drop the databases that it left. Effects: a process, and statements on the test server. */
async function runFile(runtime: Runtime, file: LayerFile, settings: ServerSettings, entry: EntryKind): Promise<FileEnd> {
  const command = fileCommand(runtime, file);
  const startedAt = performance.now();

  process.stdout.write(`\n==> ${file.name} on ${runtime}\n`);

  const ran = spawnSync(command.program, command.args, { cwd: WORKSPACE, stdio: "inherit", env: { ...process.env, ...entryEnvironment(runtime, entry), [RUN_ID_VARIABLE]: settings.runId } });
  const exit = ran.status ?? EXIT_FAILED;
  const seconds = Math.round((performance.now() - startedAt) / 1_000);

  if (ran.error !== undefined) {
    process.stderr.write(`${file.name}: ${command.program} did not start: ${ran.error.message}\n`);
  }

  return { file, exit, seconds, leftDatabases: await dropLeftDatabases(settings) };
}

/** Drop every database of the run that still exists; their names. */
async function dropLeftDatabases(settings: ServerSettings): Promise<readonly string[]> {
  const left = await runDatabases(settings);

  if (!left.ok) {
    process.stderr.write(`list the databases of the run: ${postgresErrorText(left.error)}\n`);

    return [];
  }

  for (const name of left.value) {
    const dropped = await dropDatabase(settings, name);

    if (!dropped.ok) {
      process.stderr.write(`drop ${name}: ${postgresErrorText(dropped.error)}\n`);
    }
  }

  return left.value;
}

function printPending(): void {
  process.stdout.write("\nPending scenarios (each waits for the package of its issue):\n");

  for (const pending of PENDING_SCENARIOS) {
    process.stdout.write(`  #${String(pending.issue)} ${pending.needs}:\n`);

    for (const mirror of pending.mirrors) {
      process.stdout.write(`    - ${mirror}\n`);
    }
  }
}

function printSummary(runtime: Runtime, ends: readonly FileEnd[]): void {
  process.stdout.write(`\nEnd-to-end files on ${runtime}:\n`);

  for (const end of ends) {
    const left = end.leftDatabases.length === 0 ? "" : `; left ${String(end.leftDatabases.length)} databases (dropped)`;

    process.stdout.write(`  ${end.exit === EXIT_PASSED ? "pass" : "FAIL"} ${end.file.name} (exit ${String(end.exit)}, ${String(end.seconds)} s${left})\n`);
  }
}

async function main(args: readonly string[]): Promise<number> {
  const plan = runPlan(args);

  if (!plan.ok) {
    process.stderr.write(`usage: bun tests/e2e/run.ts <node|bun> [<layer file name> ...]\nrun.ts: ${plan.error}\n`);

    return EXIT_USAGE;
  }

  const run = runSettings(randomHex(8));

  if (!run.ok) {
    process.stderr.write(`run.ts: ${run.error}\n`);

    return EXIT_USAGE;
  }

  const ends: FileEnd[] = [];

  for (const file of plan.plan.files) {
    ends.push(await runFile(plan.plan.runtime, file, run.settings, run.entry));
  }

  printPending();
  printSummary(plan.plan.runtime, ends);

  return ends.every((end) => end.exit === EXIT_PASSED && end.leftDatabases.length === 0) ? EXIT_PASSED : EXIT_FAILED;
}

if (import.meta.main) {
  process.exitCode = await main(process.argv.slice(2));
}
