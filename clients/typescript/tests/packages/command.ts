// One bounded run of a command of the npm package checks, with its stdout and stderr apart (the JSON that `npm pack
// --json` writes is stdout only).

import { spawnSync } from "node:child_process";

import type { Result } from "../../horsies/src/wire/result.ts";
import { TOOL_LIMITS } from "../public-api/tools.ts";
import type { ToolFailed, ToolLimits } from "../public-api/tools.ts";

/** A command that ran to its end: its exit code, stdout and stderr. */
export type CommandExit = { readonly code: number; readonly stdout: string; readonly stderr: string };

/**
 * Runs `command args` in `cwd` with the environment of this process plus `environment`, and waits for its end, within
 * `limits`; at a bound the command is stopped. Effects: the command's own. It does not read the exit code: a caller
 * decides what each code means.
 */
export function runCommand(
  command: string,
  args: readonly string[],
  cwd: string,
  environment: Readonly<Record<string, string>> = {},
  limits: ToolLimits = TOOL_LIMITS,
): Result<CommandExit, ToolFailed> {
  const tool = [command, ...args].join(" ");

  const run = spawnSync(command, args, {
    cwd,
    env: { ...process.env, ...environment },
    encoding: "utf8",
    timeout: limits.time.total("milliseconds"),
    maxBuffer: limits.outputBytes,
  });

  // SAFETY: spawnSync sets `error` to a system error of Node, which carries its `code`.
  const code = run.error === undefined ? "none" : ((run.error as NodeJS.ErrnoException).code ?? "no code");

  switch (code) {
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

  return { ok: true, value: { code: run.status, stdout: run.stdout, stderr: run.stderr } };
}

/** The text of a command that did not succeed: its failure, or its exit code and output. Pure. */
export function commandFailedText(command: string, run: Result<CommandExit, ToolFailed>): string {
  if (!run.ok) {
    return `${command}: ${run.error.kind}`;
  }

  return `${command}: exit ${run.value.code}\n${run.value.stdout}${run.value.stderr}`;
}
