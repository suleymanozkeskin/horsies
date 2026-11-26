// The `check` command: the static check of the app, and with `--live` the live steps on its database.
//
//   <entry> check [--live] [--loglevel LEVEL]
//
// - `--live`: after a passing static check, the connectivity of the database and the connection fit of each worker
//   profile. It installs no schema.
// - `--loglevel`: the lowest level of the core records of this process; default `WARNING`.
//
// Exit codes: 0 for a passing check (its summary on stdout); 1 for a check report (on stderr); 2 for arguments that
// do not parse. Sends stay suppressed in this command.

import type { WorkerLogLevel } from "../_catalog/worker_manifest.ts";
import type { AppContext } from "../app/context.ts";
import { liveCheckOutcome } from "../check/live-check.ts";
import { type CheckOutcome, type CheckPassed, passedText, reportText } from "../check/report.ts";
import { staticCheck } from "../check/static-check.ts";
import { configureLogging } from "../logging.ts";
import { checkAppConfig, checkTaskOptions, parseDeclaredErrorCodes } from "../native/sends/sends.ts";
import { checkLive } from "../native/worker/worker.ts";
import { ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { appCoreHandle } from "../worker/run.ts";
import {
  type CommandExitCode,
  type CommandOutput,
  EXIT_FAILED,
  EXIT_PASSED,
  EXIT_USAGE,
  parseFlags,
  parseLogLevel,
  processOutput,
  switchFlag,
  textFlag,
  type UsageError,
  usageErrorLines,
} from "./usage.ts";

/** The usage line of `check`. */
export const CHECK_USAGE = "<entry> check [--live] [--loglevel LEVEL]";

/** The default `--loglevel` of `check`. */
export const CHECK_DEFAULT_LOG_LEVEL: WorkerLogLevel = "WARNING";

/** The flags of `check`. */
const CHECK_FLAGS = {
  live: { type: "boolean" },
  loglevel: { type: "string" },
} as const;

/** Which steps `check` runs: the static check, or the static check and then the live steps. */
export type CheckSteps = "staticOnly" | "staticAndLive";

/** The parsed flags of `check`. */
export type CheckFlags = { readonly steps: CheckSteps; readonly logLevel: WorkerLogLevel };

/** What `check` calls that has effects. */
export interface CheckCommandDeps {
  readonly output: CommandOutput;
  setLogLevel(level: WorkerLogLevel): void;
  staticCheck(): CheckOutcome;
  /** The live steps after a static check that passed with `passed`. */
  liveCheck(passed: CheckPassed): Promise<CheckOutcome>;
}

/** The dependencies of `check` for the app of `context` in this process. */
export function processCheckCommandDeps(context: AppContext): CheckCommandDeps {
  const checks = { checkAppConfig, parseDeclaredErrorCodes, checkTaskOptions };

  return {
    output: processOutput(),
    setLogLevel: (level) => configureLogging({ levels: { default: level } }),
    staticCheck: () => staticCheck(context, checks, "withSchedules", "check"),
    liveCheck: async (passed) => liveCheckOutcome(passed, await checkLive(appCoreHandle(context, "PRODUCER"), context.config.coreConfigJson)),
  };
}

/** Run `check` with the arguments after the command name. Resolves with the exit code. */
export async function runCheckCommand(args: readonly string[], deps: CheckCommandDeps): Promise<CommandExitCode> {
  const flags = parseCheckFlags(args);

  if (!flags.ok) {
    for (const line of usageErrorLines("check", CHECK_USAGE, flags.error)) {
      deps.output.writeError(line);
    }

    return EXIT_USAGE;
  }

  deps.setLogLevel(flags.value.logLevel);

  const checked = deps.staticCheck();
  const outcome = checked.kind === "passed" && flags.value.steps === "staticAndLive" ? await deps.liveCheck(checked.passed) : checked;

  switch (outcome.kind) {
    case "passed":
      deps.output.writeOut(passedText(outcome.passed));

      return EXIT_PASSED;
    case "refused":
      deps.output.writeError(reportText(outcome.report));

      return EXIT_FAILED;
    default:
      return unreachable(outcome, "runCheckCommand");
  }
}

/** The flags of `check`, or the usage error of the first flag that does not parse. Pure. */
export function parseCheckFlags(args: readonly string[]): Result<CheckFlags, UsageError> {
  const flags = parseFlags(args, CHECK_FLAGS);

  if (!flags.ok) {
    return flags;
  }

  const levelText = textFlag(flags.value, "loglevel");
  const logLevel = levelText === undefined ? ok(CHECK_DEFAULT_LOG_LEVEL) : parseLogLevel(levelText);

  if (!logLevel.ok) {
    return logLevel;
  }

  const steps: CheckSteps = switchFlag(flags.value, "live") ? "staticAndLive" : "staticOnly";

  return ok({ steps, logLevel: logLevel.value });
}
