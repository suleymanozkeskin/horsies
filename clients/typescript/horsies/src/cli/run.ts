// `cli.run(app)`: the one entry of every process of an app. It checks the mode of this process first, then runs the
// command of its arguments, then ends the process with the command's exit code.
//
// Modes, in order:
// 1. A worker thread: a defect. `cli.run` runs on the main thread of a process.
// 2. Else: the command named by the argument after the entry script, from the command table.
//
// The command table is a closed record: one runner per command name. A command module parses its own flags.
//
// The exit rule: `cli.run` ends the process itself in every mode, so an open handle of user code (a pool, a timer)
// does not keep a stopped service alive. Before the end it waits, at most `EXIT_LOG_WAIT_MS`, for the core log records
// that still wait in the channel to reach the log sink.

import { isMainThread } from "node:worker_threads";

import type { Horsies } from "../app.ts";
import { appInternals } from "../app/internals.ts";
import { workerLogBacklog } from "../native/worker/worker.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { processCheckCommandDeps, runCheckCommand } from "./check-command.ts";
import type { LogBacklog } from "../native/worker/addon.ts";
import { type CommandExitCode, type CommandOutput, EXIT_USAGE, processOutput, usageError, type UsageError } from "./usage.ts";
import { processWorkerCommandDeps, runWorkerCommand } from "./worker-command.ts";

/** The commands of `cli.run`. */
export type CommandName = "worker" | "check";

/** Each command name, by itself: the record names each member of `CommandName` once. */
const COMMAND_NAME_SET: { readonly [Name in CommandName]: Name } = { worker: "worker", check: "check" };

/** The command names. */
export const COMMAND_NAMES: readonly CommandName[] = Object.values(COMMAND_NAME_SET);

/** The runner of one command: the app and the arguments after the command name; resolves with the exit code. */
export type CommandRunner = (app: Horsies, args: readonly string[]) => Promise<CommandExitCode>;

/** The command table: one runner per command name. */
export type CommandTable = { readonly [Name in CommandName]: CommandRunner };

/** The commands of this package. */
export const COMMANDS: CommandTable = {
  worker: async (app, args) => runWorkerCommand(args, processWorkerCommandDeps(appInternals(app))),
  check: async (app, args) => runCheckCommand(args, processCheckCommandDeps(appInternals(app))),
};

/** The usage line of `cli.run` with no command or an unknown one. */
export const CLI_USAGE: string = `<entry> {${COMMAND_NAMES.join(",")}} ...`;

/** The longest wait for the core log records before the process ends, in milliseconds. */
export const EXIT_LOG_WAIT_MS = 2000;

/** The pause between two reads of the log backlog, in milliseconds. */
export const EXIT_LOG_POLL_MS = 10;

/** The mode of this process. */
export type ProcessMode =
  | { readonly kind: "workerThread" }
  | { readonly kind: "command"; readonly command: CommandName; readonly args: readonly string[] }
  | { readonly kind: "usageError"; readonly error: UsageError };

/** The facts of this process that decide its mode. */
export type ProcessFacts = {
  /** The thread: the main thread of the process, or a `worker_threads` isolate. */
  readonly thread: "main" | "worker";
  /** The arguments of the process (`process.argv`): the runtime, the entry script, the command and its flags. */
  readonly argv: readonly string[];
};

/** The facts of this process. Effects: reads the thread and `process.argv`. */
export function processFacts(): ProcessFacts {
  return { thread: isMainThread ? "main" : "worker", argv: [...process.argv] };
}

/** The mode of a process with `facts`. Pure. */
export function processMode(facts: ProcessFacts): ProcessMode {
  if (facts.thread === "worker") {
    return { kind: "workerThread" };
  }

  const [command, ...args] = facts.argv.slice(2);

  if (command === undefined) {
    return { kind: "usageError", error: usageError("a command is required") };
  }

  const name = COMMAND_NAMES.find((known) => known === command);

  return name === undefined
    ? { kind: "usageError", error: usageError(`unknown command ${JSON.stringify(command)}; the commands: ${COMMAND_NAMES.join(", ")}`) }
    : { kind: "command", command: name, args };
}

/** What `cli.run` does that has effects. */
export interface CliDeps {
  readonly facts: ProcessFacts;
  readonly table: CommandTable;
  readonly output: CommandOutput;
  /** Whether core log records wait in the channel. */
  logBacklog(): LogBacklog;
  /** Wait `milliseconds`, letting the event loop run. */
  pause(milliseconds: number): Promise<void>;
  /** End the process with `code`. */
  exit(code: number): never;
}

/** The exit code of the command of this process. A worker thread is a defect: `ClientDefect`. */
export async function commandExitCode(app: Horsies, deps: CliDeps): Promise<CommandExitCode> {
  const mode = processMode(deps.facts);

  switch (mode.kind) {
    case "workerThread":
      throw new ClientDefect({ subject: "cli.run", rule: "cli.run runs on the main thread of a process" });
    case "usageError":
      deps.output.writeError(`usage: ${CLI_USAGE}`);
      deps.output.writeError(`horsies: error: ${mode.error.message}`);

      return EXIT_USAGE;
    case "command":
      return deps.table[mode.command](app, mode.args);
    default:
      return unreachable(mode, "commandExitCode");
  }
}

/** Run the command of this process, wait for the core log records, and end the process with the exit code. */
export async function runCli(app: Horsies, deps: CliDeps): Promise<number> {
  const code = await commandExitCode(app, deps);

  await waitForLogRecords(deps);

  return deps.exit(code);
}

/** Wait while core log records wait in the channel and the forwarder hands them on, at most `EXIT_LOG_WAIT_MS`. */
export async function waitForLogRecords(deps: Pick<CliDeps, "logBacklog" | "pause">): Promise<void> {
  const polls = EXIT_LOG_WAIT_MS / EXIT_LOG_POLL_MS;

  for (let poll = 0; poll < polls && deps.logBacklog() === "waiting"; poll += 1) {
    await deps.pause(EXIT_LOG_POLL_MS);
  }
}

/** The dependencies of `cli.run` in this process. */
export function processCliDeps(): CliDeps {
  return {
    facts: processFacts(),
    table: COMMANDS,
    output: processOutput(),
    logBacklog: workerLogBacklog,
    pause: async (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)),
    exit: (code) => process.exit(code),
  };
}
