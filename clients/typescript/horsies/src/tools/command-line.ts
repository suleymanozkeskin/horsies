// The parts that the commands of the `horsies` tool share: the exit codes, the output lines and the flag parse. The
// exit codes and the usage lines are those of the commands of `cli.run` (`src/cli/usage.ts`), as Python's `argparse`
// gives them. This module imports no module that loads the native addon.

import { parseArgs, type ParseArgsOptionsConfig } from "node:util";

import type { CommandExitCode } from "../cli/usage.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { err, ok, type Result } from "../wire/result.ts";

/** A command that succeeded. */
export const TOOL_PASSED: CommandExitCode = 0;

/** A command that failed. */
export const TOOL_FAILED: CommandExitCode = 1;

/** Arguments that do not parse, and a command of the app's own entry. */
export const TOOL_USAGE: CommandExitCode = 2;

/** Where a command writes its lines. */
export interface ToolOutput {
  /** Write one line to stdout. */
  writeOut(line: string): void;
  /** Write one line to stderr. */
  writeError(line: string): void;
}

/** The output of this process: stdout and stderr. */
export function processToolOutput(): ToolOutput {
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
export type ToolUsageError = { readonly kind: "usageError"; readonly message: string };

/** Write the usage error of `command` (for example `get-docs`, or none for the tool) and give the usage exit code. */
export function reportUsageError(output: ToolOutput, command: string | undefined, usage: string, error: ToolUsageError): CommandExitCode {
  const prefix = command === undefined ? "horsies" : `horsies ${command}`;

  output.writeError(`usage: ${usage}`);
  output.writeError(`${prefix}: error: ${error.message}`);

  return TOOL_USAGE;
}

/** The values of the flags of one command, by flag name, as `parseArgs` gives them. */
export type ToolFlagValues = { readonly [name: string]: string | boolean | (string | boolean)[] | undefined };

/** The error codes of `parseArgs` for arguments that do not parse. */
const PARSE_ARGS_ERROR_CODES: readonly string[] = [
  "ERR_PARSE_ARGS_UNKNOWN_OPTION",
  "ERR_PARSE_ARGS_INVALID_OPTION_VALUE",
  "ERR_PARSE_ARGS_UNEXPECTED_POSITIONAL",
];

/**
 * The flags of `args` by the flag table `options`: `--flag value` and `--flag=value` for a text flag, `--flag` for a
 * switch. An unknown flag, a text flag with no value, a value given to a switch and a positional argument are usage
 * errors. Pure.
 */
export function parseToolFlags(args: readonly string[], options: ParseArgsOptionsConfig): Result<ToolFlagValues, ToolUsageError> {
  try {
    return ok(parseArgs({ args: [...args], options, strict: true, allowPositionals: false }).values);
  } catch (cause) {
    // A refusal of `parseArgs` is a `TypeError` with one of its error codes; any other throw passes on.
    if (cause instanceof TypeError && "code" in cause && PARSE_ARGS_ERROR_CODES.some((code) => code === cause.code)) {
      return err({ kind: "usageError", message: cause.message });
    }

    throw cause;
  }
}

/** The value of the text flag `name`: a text, or `notGiven`. Another value is a defect of the flag table: `ClientDefect`. */
export function textFlagOf(values: ToolFlagValues, name: string): { readonly kind: "given"; readonly text: string } | { readonly kind: "notGiven" } {
  const value = values[name];

  if (value === undefined) {
    return { kind: "notGiven" };
  }

  if (value === true || value === false || Array.isArray(value)) {
    throw new ClientDefect({ subject: "textFlagOf", rule: `--${name} is a single text flag in the flag table` });
  }

  return { kind: "given", text: value };
}

/** Whether the switch `name` was given. A text value is a defect of the flag table: `ClientDefect`. */
export function switchFlagOf(values: ToolFlagValues, name: string): boolean {
  const value = values[name];

  if (value === undefined || value === false) {
    return false;
  }

  if (value === true) {
    return true;
  }

  throw new ClientDefect({ subject: "switchFlagOf", rule: `--${name} is a switch in the flag table` });
}
