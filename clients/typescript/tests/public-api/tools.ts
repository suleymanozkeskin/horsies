// The tools of the public API gate (the API report, the no-`any` check, the compiler lanes and the type tests): their
// paths in the workspace, and one bounded run of a tool.

import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import type { Result } from "../../horsies/src/wire/result.ts";

/** `clients/typescript/`. */
export const WORKSPACE_DIR: string = fileURLToPath(new URL("../../", import.meta.url));

/** The package folder of `horsies`. */
export const PACKAGE_DIR = `${WORKSPACE_DIR}horsies/`;

/** The tests folder. Its `package.json` pins the TypeScript 7 compiler and API Extractor of the gate. */
export const TESTS_DIR = `${WORKSPACE_DIR}tests/`;

/** The TypeScript 7 compiler: it writes the published declarations, and it is the `tsc` of the current lane. */
export const CURRENT_COMPILER = `${TESTS_DIR}node_modules/.bin/tsc`;

/** The TypeScript 6.0 compiler of the floor lane. `ci/typescript-floor/package.json` pins its version. */
export const FLOOR_COMPILER = `${WORKSPACE_DIR}ci/typescript-floor/node_modules/.bin/tsc`;

/** The bounds of one tool run: its wall time, and the output that it can write (stdout and stderr each). */
export type ToolLimits = { readonly time: Temporal.Duration; readonly outputBytes: number };

/** The bounds of a compiler run of the gate. */
export const TOOL_LIMITS: ToolLimits = { time: Temporal.Duration.from({ seconds: 300 }), outputBytes: 16_777_216 };

/** A tool that ran to its end: its exit code and its output (stdout, then stderr). */
export type ToolExit = { readonly code: number; readonly output: string };

/** A tool run that did not end with an exit code. */
export type ToolFailed =
  | { readonly kind: "NotStarted"; readonly tool: string; readonly cause: string }
  | { readonly kind: "TimedOut"; readonly tool: string; readonly bound: Temporal.Duration }
  | { readonly kind: "OutputTooLarge"; readonly tool: string; readonly limitBytes: number }
  | { readonly kind: "Signalled"; readonly tool: string; readonly signal: string };

/** The code of the system error of a spawn (`ENOENT`, `ETIMEDOUT`, ...), `none` when the spawn had no error. */
function systemErrorCode(error: Error | undefined): string {
  if (error === undefined) {
    return "none";
  }

  // SAFETY: spawnSync sets `error` to a system error of Node, which carries its `code`.
  return (error as NodeJS.ErrnoException).code ?? "no code";
}

/**
 * Runs `command args` in `cwd` and waits for its end, within `limits`; at a bound the tool is stopped. Effects: the
 * tool's own. It does not read the exit code: a caller decides what each code means.
 */
export function runTool(
  command: string,
  args: readonly string[],
  cwd: string,
  limits: ToolLimits = TOOL_LIMITS,
): Result<ToolExit, ToolFailed> {
  const tool = [command, ...args].join(" ");

  const run = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
    timeout: limits.time.total("milliseconds"),
    maxBuffer: limits.outputBytes,
  });

  switch (systemErrorCode(run.error)) {
    case "none":
      break;
    case "ETIMEDOUT":
      return { ok: false, error: { kind: "TimedOut", tool, bound: limits.time } };
    case "ENOBUFS":
      return { ok: false, error: { kind: "OutputTooLarge", tool, limitBytes: limits.outputBytes } };
    default:
      return { ok: false, error: { kind: "NotStarted", tool, cause: String(run.error) } };
  }

  if (run.status === null) {
    return { ok: false, error: { kind: "Signalled", tool, signal: String(run.signal) } };
  }

  return { ok: true, value: { code: run.status, output: `${run.stdout}${run.stderr}` } };
}

/** The text of a failed tool run, for a message. */
export function toolFailedText(failure: ToolFailed): string {
  switch (failure.kind) {
    case "NotStarted":
      return `${failure.tool}: did not start: ${failure.cause}`;
    case "TimedOut":
      return `${failure.tool}: no end within ${failure.bound.toString()}`;
    case "OutputTooLarge":
      return `${failure.tool}: wrote more than ${failure.limitBytes} bytes`;
    case "Signalled":
      return `${failure.tool}: ended by the signal ${failure.signal}`;
  }
}
