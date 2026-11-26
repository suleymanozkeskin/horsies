// The `scheduler` command: run the scheduler of the app's schedules until `SIGTERM` or `SIGINT`.
//
//   <entry> scheduler [--loglevel LEVEL]
//
// - `--loglevel`: the lowest level of the core records of this process; default `INFO`.
//
// Exit codes: 0 after an operator stop; 1 for each scheduler run error; 2 for arguments that do not parse. Sends at
// module level stay suppressed in this process until the scheduler starts to serve.

import type { WorkerLogLevel } from "../_catalog/worker_manifest.ts";
import type { AppContext } from "../app/context.ts";
import {
  processSchedulerRunDeps,
  runSchedulerOf,
  type SchedulerRunError,
  schedulerRunErrorText,
  type SchedulerStart,
  type SchedulerStopped,
} from "../scheduler/run.ts";
import { ok, type Result } from "../wire/result.ts";
import { processSignals, stopOnSignals } from "./signals.ts";
import {
  type CommandExitCode,
  type CommandOutput,
  EXIT_FAILED,
  EXIT_PASSED,
  EXIT_USAGE,
  parseFlags,
  parseLogLevel,
  processOutput,
  textFlag,
  type UsageError,
  usageErrorLines,
} from "./usage.ts";

/** The usage line of `scheduler`. */
export const SCHEDULER_USAGE = "<entry> scheduler [--loglevel LEVEL]";

/** The default `--loglevel` of `scheduler`. */
export const SCHEDULER_DEFAULT_LOG_LEVEL: WorkerLogLevel = "INFO";

/** The flags of `scheduler`. */
const SCHEDULER_FLAGS = {
  loglevel: { type: "string" },
} as const;

/** What `scheduler` calls that has effects. */
export interface SchedulerCommandDeps {
  readonly output: CommandOutput;
  runScheduler(start: SchedulerStart): Promise<Result<SchedulerStopped, SchedulerRunError>>;
}

/** The dependencies of `scheduler` for the app of `context` in this process. */
export function processSchedulerCommandDeps(context: AppContext): SchedulerCommandDeps {
  const output = processOutput();

  return {
    output,
    runScheduler: async (start) =>
      runSchedulerOf(
        context,
        start,
        processSchedulerRunDeps(context, (requestStop) =>
          stopOnSignals(processSignals(), "scheduler", requestStop, (line) => output.writeError(line)),
        ),
      ),
  };
}

/** Run `scheduler` with the arguments after the command name. Resolves with the exit code. */
export async function runSchedulerCommand(args: readonly string[], deps: SchedulerCommandDeps): Promise<CommandExitCode> {
  const start = parseSchedulerStart(args);

  if (!start.ok) {
    for (const line of usageErrorLines("scheduler", SCHEDULER_USAGE, start.error)) {
      deps.output.writeError(line);
    }

    return EXIT_USAGE;
  }

  const ended = await deps.runScheduler(start.value);

  if (ended.ok) {
    return EXIT_PASSED;
  }

  deps.output.writeError(schedulerRunErrorText(ended.error));

  return EXIT_FAILED;
}

/** The start of `scheduler` from its arguments, or the usage error of the first flag that does not parse. Pure. */
export function parseSchedulerStart(args: readonly string[]): Result<SchedulerStart, UsageError> {
  const flags = parseFlags(args, SCHEDULER_FLAGS);

  if (!flags.ok) {
    return flags;
  }

  const levelText = textFlag(flags.value, "loglevel");
  const logLevel = levelText === undefined ? ok(SCHEDULER_DEFAULT_LOG_LEVEL) : parseLogLevel(levelText);

  return logLevel.ok ? ok({ logLevel: logLevel.value }) : logLevel;
}
