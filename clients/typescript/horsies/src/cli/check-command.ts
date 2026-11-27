// The `check` command: the static check of the app, and with `--live` the live steps on its database.
//
//   <entry> check [--live] [--log-level LEVEL]
//
// - `--live`: after a passing static check, the connectivity of the database and the connection fit of each worker
//   profile. It installs no schema.
// - `--log-level`: the lowest level of the core records of this process; default `WARNING`.
//
// Exit codes: 0 for a passing check (its summary on stdout); 1 for a check report, or a core that refused the live
// steps (on stderr); 2 for arguments that do not parse. Sends stay suppressed in this command.

import type { WorkerLogLevel } from "../_catalog/worker_manifest.ts";
import type { AppContext } from "../app/context.ts";
import { liveCheckOutcome } from "../check/live-check.ts";
import { type CheckOutcome, type CheckPassed, passedText, reportText } from "../check/report.ts";
import { staticCheck } from "../check/static-check.ts";
import { configureLogging } from "../logging.ts";
import { checkAppConfig, checkTaskOptions, parseDeclaredErrorCodes } from "../native/sends/sends.ts";
import type { LiveCheckOutcome } from "../native/worker/addon.ts";
import { checkLive } from "../native/worker/worker.ts";
import { ok, type Result } from "../wire/result.ts";
import { unreachable } from "../native/call/never.ts";
import { CoreError } from "../thrown-errors.ts";
import { appCoreHandle } from "../app/core-access.ts";
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
export const CHECK_USAGE = "<entry> check [--live] [--log-level LEVEL]";

/** The default `--log-level` of `check`. */
export const CHECK_DEFAULT_LOG_LEVEL: WorkerLogLevel = "WARNING";

/** The flags of `check`. */
const CHECK_FLAGS = {
  live: { type: "boolean" },
  "log-level": { type: "string" },
} as const;

/** Which steps `check` runs: the static check, or the static check and then the live steps. */
export type CheckSteps = "staticOnly" | "staticAndLive";

/** The parsed flags of `check`. */
export type CheckFlags = { readonly steps: CheckSteps; readonly logLevel: WorkerLogLevel };

/**
 * How the live steps ended: the outcome of the check, or the refusal of the core of this process (it could not start,
 * it stopped, or this process is a fork).
 */
export type LiveStepsOutcome = CheckOutcome | { readonly kind: "coreRefused"; readonly error: CoreError };

/** What `check` calls that has effects. */
export interface CheckCommandDeps {
  readonly output: CommandOutput;
  setLogLevel(level: WorkerLogLevel): void;
  staticCheck(): CheckOutcome;
  /** The live steps after a static check that passed with `passed`. */
  liveCheck(passed: CheckPassed): Promise<LiveStepsOutcome>;
}

/** The dependencies of `check` for the app of `context` in this process. */
export function processCheckCommandDeps(context: AppContext): CheckCommandDeps {
  const checks = { checkAppConfig, parseDeclaredErrorCodes, checkTaskOptions };

  return {
    output: processOutput(),
    setLogLevel: (level) => configureLogging({ levels: { default: level } }),
    staticCheck: () => staticCheck(context, checks, "withSchedules", "check"),
    liveCheck: async (passed) => liveSteps(passed, async () => checkLive(appCoreHandle(context.core), context.config.coreConfigJson)),
  };
}

/** The outcome of the live steps that `run` gives, or the core refusal that it rejects with. A defect passes on. */
export async function liveSteps(
  passed: CheckPassed,
  run: () => Promise<LiveCheckOutcome>,
): Promise<LiveStepsOutcome> {
  try {
    return liveCheckOutcome(passed, await run());
  } catch (cause) {
    if (cause instanceof CoreError) {
      return { kind: "coreRefused", error: cause };
    }

    throw cause;
  }
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

  const outcome: LiveStepsOutcome =
    checked.kind === "passed" && flags.value.steps === "staticAndLive" ? await deps.liveCheck(checked.passed) : checked;

  switch (outcome.kind) {
    case "passed":
      deps.output.writeOut(passedText(outcome.passed));

      return EXIT_PASSED;
    case "refused":
      deps.output.writeError(reportText(outcome.report));

      return EXIT_FAILED;
    case "coreRefused":
      deps.output.writeError(`horsies check: the core refused the live check: ${outcome.error.name}: ${outcome.error.message}`);

      return EXIT_FAILED;
    default:
      return unreachable(outcome);
  }
}

/** The flags of `check`, or the usage error of the first flag that does not parse. Pure. */
export function parseCheckFlags(args: readonly string[]): Result<CheckFlags, UsageError> {
  const flags = parseFlags(args, CHECK_FLAGS);

  if (!flags.ok) {
    return flags;
  }

  const levelText = textFlag(flags.value, "log-level");
  const logLevel = levelText === undefined ? ok(CHECK_DEFAULT_LOG_LEVEL) : parseLogLevel(levelText);

  if (!logLevel.ok) {
    return logLevel;
  }

  const steps: CheckSteps = switchFlag(flags.value, "live") ? "staticAndLive" : "staticOnly";

  return ok({ steps, logLevel: logLevel.value });
}
