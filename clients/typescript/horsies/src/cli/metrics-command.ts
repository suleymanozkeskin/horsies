// The `metrics` command: serve the fleet metrics of the app's database at `GET /metrics` on
// `--listen` until `SIGINT` or `SIGTERM`.
//
// `<entry> metrics --listen HOST:PORT [--loglevel INFO]`
//
// - The core reads the fleet state at once and then at most once per collect interval; a scrape
//   renders the last read, so scrapes add no load on the database.
// - The `queue` and `schedule` label values are the names of the app config; other names are
//   `(unknown)`.
// - The server reads through its own connection, with no schema step and no listener.
// - A failed connect or a failed statement is a failure gauge in the text, never an end of the
//   command.
//
// Exit codes: 0 after an operator stop; 1 when the address does not bind; 2 for arguments that do
// not parse, `--listen` included.

import type { AppContext } from "../app/context.ts";
import type { WorkerLogLevel } from "../_catalog/worker_manifest.ts";
import { configureLogging } from "../logging.ts";
import { nativeValue, nativeValueAsync } from "../native/call/envelope.ts";
import { unreachable } from "../native/call/never.ts";
import { fleetMetricsAddon } from "../native/facades/fleet-metrics.ts";
import type { FleetMetricsRunEnd, MetricsListenAddressCheck } from "../native/facades/fleet-metrics.ts";
import type { MonitoringHandlerApp } from "../web/handler.ts";
import { err, ok } from "../wire/result.ts";
import type { Result } from "../wire/result.ts";
import { processSignals, type SignalSource, stopOnSignals } from "./signals.ts";
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
import { monitoringAppOf } from "./web-command.ts";

/** The usage line of `metrics`. */
export const METRICS_USAGE = "<entry> metrics --listen HOST:PORT [--loglevel LEVEL]";

/** The default `--loglevel` of `metrics`, as in the Python and Rust clients. */
export const METRICS_DEFAULT_LOG_LEVEL: WorkerLogLevel = "INFO";

/** The flags of `metrics`, as `node:util` `parseArgs` reads them. */
const METRICS_FLAG_OPTIONS = {
  listen: { type: "string" },
  loglevel: { type: "string" },
} as const;

/** The parsed flags of `metrics`. */
export interface MetricsFlags {
  /** The `HOST:PORT` text, which the core checks. */
  readonly listen: string;
  readonly logLevel: WorkerLogLevel;
}

/** The facts of an app that the `metrics` command reads. */
export type FleetMetricsApp = Pick<MonitoringHandlerApp, "coreHandle" | "queueNames" | "scheduleNames">;

/** One run of the fleet metrics server. */
export interface FleetMetricsRun {
  /** How the run ended. */
  readonly ended: Promise<FleetMetricsRunEnd>;
  /** Ask the run to stop. Idempotent; returns at once. */
  requestStop(): undefined;
}

/** What `metrics` calls that does I/O or calls the addon. */
export interface MetricsCommandDeps {
  readonly output: CommandOutput;
  readonly setLogLevel: (level: WorkerLogLevel) => void;
  readonly checkListenAddress: (text: string) => MetricsListenAddressCheck;
  readonly startFleetMetrics: (listen: string) => FleetMetricsRun;
  /** The source of the stop signals: the process, or a fake in a test. */
  readonly signals: SignalSource;
}

/** The dependencies of `metrics` for the app of `context` in this process. */
export function processMetricsCommandDeps(context: AppContext): MetricsCommandDeps {
  return metricsCommandDeps(monitoringAppOf(context));
}

/** The production dependencies of `metrics` for `app`. */
export function metricsCommandDeps(app: FleetMetricsApp): MetricsCommandDeps {
  return {
    output: processOutput(),
    setLogLevel: (level) => {
      configureLogging({ levels: { default: level } });
    },
    checkListenAddress: (text) =>
      nativeValue("parseMetricsListenAddress", () => fleetMetricsAddon().parseMetricsListenAddress(text)),
    startFleetMetrics: (listen) => startFleetMetrics(app, listen),
    signals: processSignals(),
  };
}

/** Prepare the server of `app` on `listen` and start its run. */
function startFleetMetrics(app: FleetMetricsApp, listen: string): FleetMetricsRun {
  const server = nativeValue("prepareFleetMetrics", () =>
    fleetMetricsAddon().prepareFleetMetrics(app.coreHandle.native, {
      listen,
      queueNames: [...app.queueNames],
      scheduleNames: [...app.scheduleNames],
    }),
  );

  return {
    ended: nativeValueAsync("FleetMetricsServer.run", () => server.run()),
    requestStop: () => nativeValue("FleetMetricsServer.requestStop", () => server.requestStop()),
  };
}

/** Run `metrics` with the arguments after the command name. Resolves with the exit code. */
export async function runMetricsCommand(args: readonly string[], deps: MetricsCommandDeps): Promise<CommandExitCode> {
  const flags = preparedMetrics(args, deps.checkListenAddress);

  if (!flags.ok) {
    for (const line of usageErrorLines("metrics", METRICS_USAGE, flags.error)) {
      deps.output.writeError(line);
    }

    return EXIT_USAGE;
  }

  deps.setLogLevel(flags.value.logLevel);

  const end = await runUntilStop(flags.value.listen, deps);

  switch (end.kind) {
    case "stopped":
      return EXIT_PASSED;
    case "failed":
      deps.output.writeError(end.message);

      return EXIT_FAILED;
    default:
      return unreachable(end);
  }
}

/** Every argument check of `metrics`, in order: the flags, then the core's check of `--listen`. */
function preparedMetrics(
  args: readonly string[],
  check: (text: string) => MetricsListenAddressCheck,
): Result<MetricsFlags, UsageError> {
  const flags = parseMetricsFlags(args);

  if (!flags.ok) {
    return flags;
  }

  const listen = checkedListen(flags.value.listen, check);

  return listen.ok ? flags : listen;
}

/**
 * Run the server on `listen` until it ends; the first stop signal asks it to stop. The handlers stay until the process
 * ends (`cli.run` ends it), and each signal writes its line.
 */
async function runUntilStop(listen: string, deps: MetricsCommandDeps): Promise<FleetMetricsRunEnd> {
  const run = deps.startFleetMetrics(listen);

  stopOnSignals(deps.signals, "metrics", () => run.requestStop(), (line) => deps.output.writeError(line));

  return run.ended;
}

/** The flags of `metrics`: `--listen` is required. Pure. */
export function parseMetricsFlags(args: readonly string[]): Result<MetricsFlags, UsageError> {
  const parsed = parseFlags(args, METRICS_FLAG_OPTIONS);

  if (!parsed.ok) {
    return parsed;
  }

  const listen = textFlag(parsed.value, "listen");

  if (listen === undefined) {
    return err(usageError("--listen HOST:PORT is required; there is no default"));
  }

  const logLevel = parseLogLevel(textFlag(parsed.value, "loglevel") ?? METRICS_DEFAULT_LOG_LEVEL);

  return logLevel.ok ? ok({ listen, logLevel: logLevel.value }) : logLevel;
}

/** The core's check of `--listen`; a refusal is a usage error with the core's rule. */
function checkedListen(text: string, check: (text: string) => MetricsListenAddressCheck): Result<undefined, UsageError> {
  const checked = check(text);

  switch (checked.kind) {
    case "address":
      return ok(undefined);
    case "refused":
      return err(usageError(`invalid --listen: ${checked.message}`));
    default:
      return unreachable(checked);
  }
}
