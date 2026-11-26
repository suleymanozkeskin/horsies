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

import { parseArgs } from "node:util";

import { configureLogging } from "../logging.ts";
import { nativeValue, nativeValueAsync } from "../native/call/envelope.ts";
import { unreachable } from "../native/call/never.ts";
import { fleetMetricsAddon } from "../native/facades/fleet-metrics.ts";
import type { FleetMetricsRunEnd, MetricsListenAddressCheck } from "../native/facades/fleet-metrics.ts";
import type { MonitoringHandlerApp } from "../web/handler.ts";
import { err, ok } from "../wire/result.ts";
import type { Result } from "../wire/result.ts";
import {
  DEFAULT_LOG_LEVEL,
  EXIT_FAILED,
  EXIT_STOPPED,
  EXIT_USAGE,
  listenForStopSignals,
  parseArgsUsageError,
  parseLogLevel,
  processOutput,
  usageError,
  usageErrorLine,
} from "./command-parts.ts";
import type { CliLogLevel, CommandExitCode, CommandOutput, StopSignals, UsageError } from "./command-parts.ts";

/** The flags of `metrics`, as `node:util` `parseArgs` reads them. */
const METRICS_FLAG_OPTIONS = {
  listen: { type: "string" },
  loglevel: { type: "string" },
} as const;

/** The parsed flags of `metrics`. */
export interface MetricsFlags {
  /** The `HOST:PORT` text, which the core checks. */
  readonly listen: string;
  readonly logLevel: CliLogLevel;
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
  readonly setLogLevel: (level: CliLogLevel) => undefined;
  readonly checkListenAddress: (text: string) => MetricsListenAddressCheck;
  readonly startFleetMetrics: (listen: string) => FleetMetricsRun;
  readonly stopSignals: () => StopSignals;
}

/** The production dependencies of `metrics` for `app`. */
export function metricsCommandDeps(app: FleetMetricsApp): MetricsCommandDeps {
  return {
    output: processOutput(),
    setLogLevel: (level) => {
      configureLogging({ levels: { default: level } });

      return undefined;
    },
    checkListenAddress: (text) =>
      nativeValue("parseMetricsListenAddress", () => fleetMetricsAddon().parseMetricsListenAddress(text)),
    startFleetMetrics: (listen) => startFleetMetrics(app, listen),
    stopSignals: listenForStopSignals,
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
    deps.output.writeError(usageErrorLine("metrics", flags.error));

    return EXIT_USAGE;
  }

  deps.setLogLevel(flags.value.logLevel);

  const end = await runUntilStop(flags.value.listen, deps);

  switch (end.kind) {
    case "stopped":
      return EXIT_STOPPED;
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

/** Run the server on `listen` until it ends or a stop signal asks it to stop; then its end. */
async function runUntilStop(listen: string, deps: MetricsCommandDeps): Promise<FleetMetricsRunEnd> {
  const signals = deps.stopSignals();

  try {
    const run = deps.startFleetMetrics(listen);
    const first = await Promise.race([run.ended.then(endedFirst), signals.received.then(signalledFirst)]);

    switch (first.kind) {
      case "signalled":
        run.requestStop();
        break;
      case "ended":
        break;
      default:
        return unreachable(first);
    }

    return await run.ended;
  } finally {
    signals.remove();
  }
}

/** Which came first: the end of the run or a stop signal. */
type FirstEvent = { readonly kind: "ended" } | { readonly kind: "signalled" };

function endedFirst(): FirstEvent {
  return { kind: "ended" };
}

function signalledFirst(): FirstEvent {
  return { kind: "signalled" };
}

/** The flags of `metrics`: `--listen` is required. Pure. */
export function parseMetricsFlags(args: readonly string[]): Result<MetricsFlags, UsageError> {
  let values: ReturnType<typeof parseMetricsArgs>["values"];

  try {
    values = parseMetricsArgs(args).values;
  } catch (cause) {
    return err(parseArgsUsageError(cause));
  }

  if (values.listen === undefined) {
    return err(usageError("--listen HOST:PORT is required; there is no default"));
  }

  const logLevel = parseLogLevel(values.loglevel ?? DEFAULT_LOG_LEVEL);

  return logLevel.ok ? ok({ listen: values.listen, logLevel: logLevel.value }) : logLevel;
}

/** `parseArgs` over the flags of `metrics`: strict, no positional argument. Throws its usage error. */
function parseMetricsArgs(args: readonly string[]) {
  return parseArgs({ args: [...args], options: METRICS_FLAG_OPTIONS, strict: true, allowPositionals: false });
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
