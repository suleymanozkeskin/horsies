// The showcase entry as a process of a test or of the run script: a worker that runs while they send, and a command
// that runs to its end. A worker runs every task body in a child that is the entry again, so a test cannot run a worker
// in its own process: it starts the entry with the runtime of its own process and the start module that
// `START_MODULE_VARIABLE` names.

import { type ChildProcess, spawn } from "node:child_process";
import { closeSync, mkdtempSync, openSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

/** The entry of the showcase. */
export const SHOWCASE_ENTRY: string = fileURLToPath(new URL("../src/main.ts", import.meta.url));

/** The longest wait for a stopping worker before it is killed. */
export const STOP_WAIT_MS = 30_000;

/** The longest run of a command that runs to its end. */
export const COMMAND_WAIT_MS = 60_000;

/** The pause between two looks at a stopping process. */
const STOP_POLL_MS = 100;

/**
 * The variable that names the start module of every entry process (an absolute path): `scripts/test.sh` and
 * `scripts/run.sh` set it to the dev start module of this workspace. Not set: the entry runs with no start module, as
 * an app that installs `horsies` from npm does.
 */
export const START_MODULE_VARIABLE = "SHOWCASE_START_MODULE";

/** The runtime of this process: Bun when `process.versions.bun` is set, else Node. */
export type Runtime = "node" | "bun";

export function runtimeOfProcess(): Runtime {
  return process.versions["bun"] === undefined ? "node" : "bun";
}

/** The runtime flags that run `startModule` before the entry: `--import` on Node, `--preload` on Bun. Pure. */
export function startModuleFlags(startModule: string | undefined, runtime: Runtime): readonly string[] {
  if (startModule === undefined || startModule === "") {
    return [];
  }

  switch (runtime) {
    case "node":
      return ["--import", startModule];
    case "bun":
      return ["--preload", startModule];
    default: {
      const unexpected: never = runtime;

      throw new TypeError(`startModuleFlags: a runtime outside its type: ${String(unexpected)}`);
    }
  }
}

/** A started entry process and the file of its stdout and stderr. */
export type EntryProcess = { readonly child: ChildProcess; readonly logPath: string };

/** How a stop of an entry process ended. */
export type StopEnd = "exited" | "killedAfterWait";

/** Start the entry with `args`, the environment of this process and `env`. Its output goes to a log file. */
export function startEntry(args: readonly string[], env: Readonly<Record<string, string>>): EntryProcess {
  return startEntryAt(SHOWCASE_ENTRY, args, env);
}

/** Start the entry file `entry` with `args`, as `startEntry` starts the showcase entry. */
export function startEntryAt(entry: string, args: readonly string[], env: Readonly<Record<string, string>>): EntryProcess {
  const logPath = join(mkdtempSync(join(tmpdir(), "acme-showcase-")), "entry.log");
  const log = openSync(logPath, "a");
  const flags = startModuleFlags(process.env[START_MODULE_VARIABLE], runtimeOfProcess());

  const child = spawn(process.execPath, [...flags, entry, ...args], {
    env: { ...process.env, ...env },
    stdio: ["ignore", log, log],
  });

  closeSync(log);

  return { child, logPath };
}

/** The last `lines` lines of the log of `entry`. */
export function logTail(entry: EntryProcess, lines: number): string {
  return readFileSync(entry.logPath, "utf8").split("\n").slice(-lines).join("\n");
}

function hasExited(child: ChildProcess): boolean {
  return child.exitCode !== null || child.signalCode !== null;
}

/** Stop `entry` with SIGTERM and wait for the drain; kill it after `STOP_WAIT_MS`. */
export async function stopEntry(entry: EntryProcess): Promise<StopEnd> {
  if (hasExited(entry.child)) {
    return "exited";
  }

  entry.child.kill("SIGTERM");

  for (let waited = 0; waited < STOP_WAIT_MS; waited += STOP_POLL_MS) {
    if (hasExited(entry.child)) {
      return "exited";
    }

    await new Promise((resolve) => setTimeout(resolve, STOP_POLL_MS));
  }

  entry.child.kill("SIGKILL");

  return "killedAfterWait";
}

/** The end of a command that ran to its end: its exit code (`null` after a signal) and its output. */
export type CommandRun = { readonly code: number | null; readonly output: string };

/** Run the entry with `args` to its end; kill it after `COMMAND_WAIT_MS`. */
export async function runEntry(args: readonly string[], env: Readonly<Record<string, string>>): Promise<CommandRun> {
  const entry = startEntry(args, env);

  const ended = new Promise<number | null>((resolve) => {
    entry.child.once("exit", (code) => {
      resolve(code);
    });
  });

  const timer = setTimeout(() => {
    entry.child.kill("SIGKILL");
  }, COMMAND_WAIT_MS);

  const code = await ended;

  clearTimeout(timer);

  return { code, output: readFileSync(entry.logPath, "utf8") };
}
