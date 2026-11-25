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

/** Run the case `name` of the script `script` (a file of this folder), killed at `boundMs`. */
export function runChildCase(script: string, name: string, boundMs: number): ChildEnd {
  const path = fileURLToPath(new URL(script, import.meta.url));
  const result = spawnSync(process.execPath, [path, name], { encoding: "utf8", timeout: boundMs });

  return { stdout: result.stdout.trim(), stderr: result.stderr, status: result.status, signal: result.signal };
}
