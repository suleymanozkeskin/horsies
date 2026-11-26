// The parts that the `web` and `metrics` commands share: exit codes, the command output, the
// `--loglevel` values, usage errors, and the stop signals.

import { thrownFacts } from "../native/call/thrown-facts.ts";
import { err, ok } from "../wire/result.ts";
import type { Result } from "../wire/result.ts";

/** The exit code of a command: 0 after an operator stop, 1 for a failure, 2 for a usage error. */
export type CommandExitCode = 0 | 1 | 2;

/** The exit code after an operator stop (`SIGINT`, `SIGTERM`). */
export const EXIT_STOPPED = 0;

/** The exit code of a failure: a check refusal, an address that does not bind. */
export const EXIT_FAILED = 1;

/** The exit code of arguments that do not parse. */
export const EXIT_USAGE = 2;

/** Where a command writes its lines. */
export interface CommandOutput {
  /** Write one line to stdout. */
  writeOut(line: string): undefined;
  /** Write one line to stderr. */
  writeError(line: string): undefined;
}

/** The output of this process: stdout and stderr. */
export function processOutput(): CommandOutput {
  return {
    writeOut: (line) => {
      process.stdout.write(`${line}\n`);

      return undefined;
    },
    writeError: (line) => {
      process.stderr.write(`${line}\n`);

      return undefined;
    },
  };
}

/** Arguments that do not parse. `message` names the flag and the rule. */
export interface UsageError {
  readonly kind: "usageError";
  readonly message: string;
}

/** A usage error with `message`. Pure. */
export function usageError(message: string): UsageError {
  return { kind: "usageError", message };
}

/** The usage error of a refusal of `node:util` `parseArgs` (an unknown flag, a missing value). */
export function parseArgsUsageError(cause: unknown): UsageError {
  return usageError(thrownFacts(cause).message);
}

/** The line of a usage error of `command`, as Python's argparse and the Rust client write it. Pure. */
export function usageErrorLine(command: string, error: UsageError): string {
  return `horsies ${command}: error: ${error.message}`;
}

/** The `--loglevel` values. */
export const CLI_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] as const;

/** A `--loglevel` value: the lowest level of the core records that the process writes. */
export type CliLogLevel = (typeof CLI_LOG_LEVELS)[number];

/** The default `--loglevel` of the `web` and `metrics` commands. */
export const DEFAULT_LOG_LEVEL: CliLogLevel = "INFO";

/** The level of a `--loglevel` text, in any case. Pure. */
export function parseLogLevel(text: string): Result<CliLogLevel, UsageError> {
  const upper = text.toUpperCase();
  const level = CLI_LOG_LEVELS.find((known) => known === upper);

  return level === undefined
    ? err(usageError(`--loglevel must be one of ${CLI_LOG_LEVELS.join(", ")}, not ${JSON.stringify(text)}`))
    : ok(level);
}

/** A signal that stops a serving command. */
export type StopSignalName = "SIGINT" | "SIGTERM";

/** The stop signals of one command run. */
export interface StopSignals {
  /** The first stop signal. A later signal does nothing more. */
  readonly received: Promise<StopSignalName>;
  /** Remove the handlers. Idempotent. */
  remove(): undefined;
}

/**
 * Install handlers for `SIGINT` and `SIGTERM` until `remove()`. While they are installed, the
 * default action of the signals does not run: the command stops through its own close.
 */
export function listenForStopSignals(): StopSignals {
  const { promise, resolve } = Promise.withResolvers<StopSignalName>();

  const onInterrupt = (): void => {
    resolve("SIGINT");
  };

  const onTerminate = (): void => {
    resolve("SIGTERM");
  };

  process.on("SIGINT", onInterrupt);
  process.on("SIGTERM", onTerminate);

  return {
    received: promise,
    remove: () => {
      process.off("SIGINT", onInterrupt);
      process.off("SIGTERM", onTerminate);

      return undefined;
    },
  };
}
