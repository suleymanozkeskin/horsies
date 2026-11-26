// The `worker` command: run a worker of a profile of the app until `SIGTERM` or `SIGINT`.
//
//   <entry> worker --profile NAME [--loglevel LEVEL] [--metrics-listen HOST:PORT]
//
// - `--profile`: no default; the core refuses a start with no profile and names the profiles of the app.
// - `--loglevel`: the lowest level of the core records of this process and of its children; default `INFO`.
// - `--metrics-listen`: serve `/metrics`, `/livez` and `/readyz` on `HOST:PORT`; the core's rule checks the value.
//
// Exit codes: 0 after an operator stop and the drain; 1 for each worker run error; 2 for arguments that do not parse.
// The core ends the process itself with 77 or 79 when the worker fails closed.

import type { MetricsEndpoint, ProfileRequest, WorkerLogLevel } from "../_catalog/worker_manifest.ts";
import type { AppContext } from "../app/context.ts";
import type { ListenAddressCheck } from "../native/worker/addon.ts";
import { parseListenAddress } from "../native/worker/worker.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../native/call/never.ts";
import {
  processWorkerRunDeps,
  runWorkerOf,
  type WorkerRunError,
  workerRunErrorText,
  type WorkerStart,
  type WorkerStopped,
} from "../worker/run.ts";
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
  usageError,
  type UsageError,
  usageErrorLines,
} from "./usage.ts";

/** The usage line of `worker`. */
export const WORKER_USAGE = "<entry> worker --profile NAME [--loglevel LEVEL] [--metrics-listen HOST:PORT]";

/** The default `--loglevel` of `worker`. */
export const WORKER_DEFAULT_LOG_LEVEL: WorkerLogLevel = "INFO";

/** The flags of `worker`. */
const WORKER_FLAGS = {
  profile: { type: "string" },
  loglevel: { type: "string" },
  "metrics-listen": { type: "string" },
} as const;

/** The endpoint of a start with no `--metrics-listen`. */
const NO_METRICS_ENDPOINT: MetricsEndpoint = { kind: "NO_METRICS_ENDPOINT" };

/** What `worker` calls that has effects. */
export interface WorkerCommandDeps {
  readonly output: CommandOutput;
  parseListenAddress(text: string): ListenAddressCheck;
  runWorker(start: WorkerStart): Promise<Result<WorkerStopped, WorkerRunError>>;
}

/** The dependencies of `worker` for the app of `context` in this process. */
export function processWorkerCommandDeps(context: AppContext): WorkerCommandDeps {
  const output = processOutput();

  return {
    output,
    parseListenAddress,
    runWorker: async (start) =>
      runWorkerOf(
        context,
        start,
        processWorkerRunDeps(context, (requestStop) =>
          stopOnSignals(processSignals(), "worker", requestStop, (line) => output.writeError(line)),
        ),
      ),
  };
}

/** Run `worker` with the arguments after the command name. Resolves with the exit code. */
export async function runWorkerCommand(args: readonly string[], deps: WorkerCommandDeps): Promise<CommandExitCode> {
  const start = parseWorkerStart(args, deps.parseListenAddress);

  if (!start.ok) {
    for (const line of usageErrorLines("worker", WORKER_USAGE, start.error)) {
      deps.output.writeError(line);
    }

    return EXIT_USAGE;
  }

  const ended = await deps.runWorker(start.value);

  if (ended.ok) {
    return EXIT_PASSED;
  }

  deps.output.writeError(workerRunErrorText(ended.error));

  return EXIT_FAILED;
}

/** The start of `worker` from its arguments, or the usage error of the first flag that does not parse. Pure. */
export function parseWorkerStart(
  args: readonly string[],
  checkAddress: (text: string) => ListenAddressCheck,
): Result<WorkerStart, UsageError> {
  const flags = parseFlags(args, WORKER_FLAGS);

  if (!flags.ok) {
    return flags;
  }

  const profileName = textFlag(flags.value, "profile");
  const levelText = textFlag(flags.value, "loglevel");
  const logLevel = levelText === undefined ? ok(WORKER_DEFAULT_LOG_LEVEL) : parseLogLevel(levelText);

  if (!logLevel.ok) {
    return logLevel;
  }

  const listenText = textFlag(flags.value, "metrics-listen");
  const metricsEndpoint = listenText === undefined ? ok(NO_METRICS_ENDPOINT) : metricsEndpointOf(checkAddress(listenText));

  if (!metricsEndpoint.ok) {
    return metricsEndpoint;
  }

  const profile: ProfileRequest = profileName === undefined ? { kind: "NOT_GIVEN" } : { kind: "NAMED", name: profileName };

  return ok({ profile, logLevel: logLevel.value, metricsEndpoint: metricsEndpoint.value });
}

/** The endpoint of a checked `--metrics-listen` value, or its usage error with the core's text. Pure. */
function metricsEndpointOf(checked: ListenAddressCheck): Result<MetricsEndpoint, UsageError> {
  switch (checked.kind) {
    case "address":
      return ok({ kind: "LISTEN_ADDRESS", host: checked.host, port: checked.port });
    case "refused":
      return err(usageError(`--metrics-listen: ${checked.cause}`));
    default:
      return unreachable(checked);
  }
}
