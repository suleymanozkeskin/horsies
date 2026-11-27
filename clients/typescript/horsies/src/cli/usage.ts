// The parts that every command of `cli.run` shares: the exit codes, the output lines, the `--log-level` values, the
// flag parse with `node:util` `parseArgs`, and the usage error.

import { parseArgs, type ParseArgsOptionsConfig } from "node:util";

import type { WorkerLogLevel } from "../_catalog/worker_manifest.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { err, ok, type Result } from "../wire/result.ts";

/** The exit code of a command: 0 after an operator stop or a passing check, 1 for a failure, 2 for a usage error. */
export type CommandExitCode = 0 | 1 | 2;

/** An operator stop after the drain, or a passing check. */
export const EXIT_PASSED: CommandExitCode = 0;

/** A check report, a refused start, a failed run. */
export const EXIT_FAILED: CommandExitCode = 1;

/** Arguments that do not parse. */
export const EXIT_USAGE: CommandExitCode = 2;

/** Where a command writes its lines. */
export interface CommandOutput {
  /** Write one line to stdout. */
  writeOut(line: string): void;
  /** Write one line to stderr. */
  writeError(line: string): void;
}

/** The output of this process: stdout and stderr. */
export function processOutput(): CommandOutput {
  return {
    writeOut: (line) => {
      process.stdout.write(`${line}\n`);
    },
    writeError: (line) => {
      process.stderr.write(`${line}\n`);
    },
  };
}

/** Arguments that do not parse: `message` names the flag or the command and the rule. */
export type UsageError = { readonly kind: "usageError"; readonly message: string };

/** A usage error with `message`. Pure. */
export function usageError(message: string): UsageError {
  return { kind: "usageError", message };
}

/** The lines of a usage error of `command`: the usage line, then the error, as Python's `argparse` writes them. Pure. */
export function usageErrorLines(command: string, usage: string, error: UsageError): readonly string[] {
  return [`usage: ${usage}`, `horsies ${command}: error: ${error.message}`];
}

/** Each `--log-level` value, by itself: the record names each level of the core's `WorkerLogLevel` once. */
const LOG_LEVEL_NAMES: { readonly [Level in WorkerLogLevel]: Level } = {
  DEBUG: "DEBUG",
  INFO: "INFO",
  WARNING: "WARNING",
  ERROR: "ERROR",
  CRITICAL: "CRITICAL",
};

/** The `--log-level` values, lowest first. */
export const LOG_LEVELS: readonly WorkerLogLevel[] = Object.values(LOG_LEVEL_NAMES);

/** The level of a `--log-level` text, in any case. Pure. */
export function parseLogLevel(text: string): Result<WorkerLogLevel, UsageError> {
  const upper = text.toUpperCase();
  const level = LOG_LEVELS.find((known) => known === upper);

  return level === undefined
    ? err(usageError(`--log-level must be one of ${LOG_LEVELS.join(", ")}, not ${JSON.stringify(text)}`))
    : ok(level);
}

/** The values of the flags of one command, by flag name, as `parseArgs` gives them. */
export type FlagValues = { readonly [name: string]: string | boolean | (string | boolean)[] | undefined };

/**
 * The flags of `args` by the flag table `options`: `--flag value` and `--flag=value` for a text flag, `--flag` for a
 * switch. An unknown flag, a text flag with no value, a value given to a switch, and a positional argument are usage
 * errors. Pure.
 */
export function parseFlags(args: readonly string[], options: ParseArgsOptionsConfig): Result<FlagValues, UsageError> {
  try {
    return ok(parseArgs({ args: [...args], options, strict: true, allowPositionals: false }).values);
  } catch (cause) {
    // A refusal of `parseArgs` is a `TypeError` with one of its error codes; any other throw passes on.
    if (cause instanceof TypeError && "code" in cause && PARSE_ARGS_ERROR_CODES.some((code) => code === cause.code)) {
      return err(usageError(cause.message));
    }

    throw cause;
  }
}

/** The error codes of `parseArgs` for arguments that do not parse. */
const PARSE_ARGS_ERROR_CODES: readonly string[] = [
  "ERR_PARSE_ARGS_UNKNOWN_OPTION",
  "ERR_PARSE_ARGS_INVALID_OPTION_VALUE",
  "ERR_PARSE_ARGS_UNEXPECTED_POSITIONAL",
];

/** The text value of `name`, or none. A non-text value is a defect of the flag table: `ClientDefect`. Pure. */
export function textFlag(values: FlagValues, name: string): string | undefined {
  const value = values[name];

  if (value === true || value === false || Array.isArray(value)) {
    return raiseDefect(new ClientDefect({ subject: "textFlag", rule: `--${name} is a single text flag in the flag table` }));
  }

  return value;
}

/** Whether the switch `name` was given. A text value is a defect of the flag table: `ClientDefect`. Pure. */
export function switchFlag(values: FlagValues, name: string): boolean {
  const value = values[name];

  if (value === undefined) {
    return false;
  }

  if (value === true || value === false) {
    return value;
  }

  return raiseDefect(new ClientDefect({ subject: "switchFlag", rule: `--${name} is a single switch in the flag table` }));
}
