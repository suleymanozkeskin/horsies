// Runs one case of a child script of the native tests in a new process of this runtime, with the
// environment of this process (the addon path and the database).

import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

/** The end of one child case. */
export interface ChildEnd {
  readonly stdout: string;
  readonly stderr: string;
  readonly status: number | null;
  readonly signal: string | null;
}

/**
 * The shell command that sets the core file size limit to 0, then replaces the shell with its arguments. Where the
 * kernel gives core files to a handler program (`systemd-coredump` on Linux), the handler reads the limit; with no
 * limit it stores the core of the aborted runtime first, and the process ends only after that.
 */
const NO_CORE_FILE_COMMAND = 'ulimit -c 0 && exec "$@"';

/** The shell of `NO_CORE_FILE_COMMAND`. */
const SHELL_PATH = "/bin/sh";

/** Run the case `name` of the script `script` (a file of this folder), killed at `boundMs`. */
export function runChildCase(script: string, name: string, boundMs: number): ChildEnd {
  const path = fileURLToPath(new URL(script, import.meta.url));
  const result = spawnSync(process.execPath, [path, name], { encoding: "utf8", timeout: boundMs });

  return { stdout: result.stdout.trim(), stderr: result.stderr, status: result.status, signal: result.signal };
}

/**
 * Run the case `name` of the script `script` (a file of this folder), a case that aborts its process, killed at
 * `boundMs`. The process starts with a core file size limit of 0, so its end is the abort and not the store of a
 * core file. The shell replaces itself with the process, so the signal is the signal of the process.
 */
export function runAbortingChildCase(script: string, name: string, boundMs: number): ChildEnd {
  const path = fileURLToPath(new URL(script, import.meta.url));
  const shellArguments = ["-c", NO_CORE_FILE_COMMAND, SHELL_PATH, process.execPath, path, name];
  const result = spawnSync(SHELL_PATH, shellArguments, { encoding: "utf8", timeout: boundMs });

  return { stdout: result.stdout.trim(), stderr: result.stderr, status: result.status, signal: result.signal };
}
