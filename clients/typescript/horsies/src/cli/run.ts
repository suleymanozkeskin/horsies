// `cli.run(app)`: the one entry of every process of an app. It checks the mode of this process first, then runs the
// command of its arguments, then ends the process with the command's exit code.
//
// Modes, in order:
// 1. A worker thread whose `workerData` holds the seat marker: a seat isolate of a `SHARED_THREAD` child. The child
//    started this entry again in the thread; the isolate serves runs until its process ends (`src/seats/isolate.ts`).
// 2. Another worker thread: a defect. `cli.run` runs on the main thread of a process, or in a seat isolate.
// 3. The argument after the entry script is the child flag (`--horsies-child`): child mode. The worker started this
//    entry again as a task child or as its handler child; the child ends its process itself (`src/child/host.ts`).
// 4. Else: the command named by the argument after the entry script, from the command table.
//
// The command table is a closed record: one runner per command name. A command module parses its own flags.
//
// The exit rule: `cli.run` ends the process itself in every mode but the seat isolate, so an open handle of user code (a pool, a timer)
// does not keep a stopped service alive. Before the end it waits, at most `EXIT_LOG_WAIT_MS` by the clock, for the core
// log records that still wait in the channel to reach the log sink. A command that throws ends the process with 1
// through the same rule: a defect of the package first follows the defect rule (in a worker process it aborts), and
// the throw is written to stderr.

import { isMainThread } from "node:worker_threads";

import type { Horsies } from "../app.ts";
import { appInternals } from "../app/internals.ts";
import { CHILD_MODE_FLAG, processStart } from "../app/suppression.ts";
import { processChildModeDeps, runChildMode } from "../child/host.ts";
import { processSeatIsolateDeps, runSeatIsolate } from "../seats/isolate.ts";
import { workerLogBacklog } from "../native/worker/worker.ts";
import { raiseDefect, reportDefect } from "../native/call/defect.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import { ClientDefect, CoreDefect } from "../thrown-errors.ts";
import { unreachable } from "../native/call/never.ts";
import { processCheckCommandDeps, runCheckCommand } from "./check-command.ts";
import type { LogBacklog } from "../native/worker/addon.ts";
import {
  type CommandExitCode,
  type CommandOutput,
  EXIT_FAILED,
  EXIT_USAGE,
  processOutput,
  usageError,
  type UsageError,
} from "./usage.ts";
import { processMetricsCommandDeps, runMetricsCommand } from "./metrics-command.ts";
import { processSchedulerCommandDeps, runSchedulerCommand } from "./scheduler-command.ts";
import { processWebCommandDeps, runWebCommand } from "./web-command.ts";
import { processWorkerCommandDeps, runWorkerCommand } from "./worker-command.ts";

/** The commands of `cli.run`. */
export type CommandName = "worker" | "check" | "scheduler" | "web" | "metrics";

/** Each command name, by itself: the record names each member of `CommandName` once. */
const COMMAND_NAME_SET: { readonly [Name in CommandName]: Name } = {
  worker: "worker",
  check: "check",
  scheduler: "scheduler",
  web: "web",
  metrics: "metrics",
};

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
  scheduler: async (app, args) => runSchedulerCommand(args, processSchedulerCommandDeps(appInternals(app))),
  web: async (app, args) => runWebCommand(args, processWebCommandDeps(appInternals(app))),
  metrics: async (app, args) => runMetricsCommand(args, processMetricsCommandDeps(appInternals(app))),
};

/** The usage line of `cli.run` with no command or an unknown one. */
export const CLI_USAGE: string = `<entry> {${COMMAND_NAMES.join(",")}} ...`;

/** The longest wait for the core log records before the process ends, in milliseconds. */
export const EXIT_LOG_WAIT_MS = 2000;

/** The pause between two reads of the log backlog, in milliseconds. */
export const EXIT_LOG_POLL_MS = 10;

/** The most reads of the log backlog in one wait: the wait ends at this count or at `EXIT_LOG_WAIT_MS`, the first. */
export const MAX_EXIT_LOG_POLLS: number = EXIT_LOG_WAIT_MS / EXIT_LOG_POLL_MS;

/** The mode of this process. `child`: the arguments after the entry script, the child flag first. */
export type ProcessMode =
  | { readonly kind: "seatIsolate" }
  | { readonly kind: "workerThread" }
  | { readonly kind: "child"; readonly args: readonly string[] }
  | { readonly kind: "command"; readonly command: CommandName; readonly args: readonly string[] }
  | { readonly kind: "usageError"; readonly error: UsageError };

/** The facts of this process that decide its mode. */
export type ProcessFacts = {
  /**
   * The thread: the main thread of the process, a seat isolate (a `worker_threads` isolate with the seat marker), or
   * another `worker_threads` isolate.
   */
  readonly thread: "main" | "seatIsolate" | "worker";
  /** The arguments of the process (`process.argv`): the runtime, the entry script, the command and its flags. */
  readonly argv: readonly string[];
};

/** The facts of this process. Effects: reads the thread, its `workerData` and `process.argv`. */
export function processFacts(): ProcessFacts {
  return { thread: threadOf(isMainThread, processStart().seatIsolate), argv: [...process.argv] };
}

function threadOf(mainThread: boolean, seatIsolate: boolean): ProcessFacts["thread"] {
  if (mainThread) {
    return "main";
  }

  return seatIsolate ? "seatIsolate" : "worker";
}

/** The mode of a process with `facts`. Pure. */
export function processMode(facts: ProcessFacts): ProcessMode {
  switch (facts.thread) {
    case "seatIsolate":
      return { kind: "seatIsolate" };
    case "worker":
      return { kind: "workerThread" };
    case "main":
      return mainThreadMode(facts.argv);
    default:
      return unreachable(facts.thread);
  }
}

/** The mode of the main thread with the process arguments `argv`. Pure. */
function mainThreadMode(argv: readonly string[]): ProcessMode {
  const [command, ...args] = argv.slice(2);

  if (command === CHILD_MODE_FLAG) {
    return { kind: "child", args: argv.slice(2) };
  }

  if (command === undefined) {
    return { kind: "usageError", error: usageError("a command is required") };
  }

  const name = COMMAND_NAMES.find((known) => known === command);

  return name === undefined
    ? { kind: "usageError", error: usageError(`unknown command ${JSON.stringify(command)}; the commands: ${COMMAND_NAMES.join(", ")}`) }
    : { kind: "command", command: name, args };
}

/** Child mode: serve the app as a child with the arguments after the entry script; the process ends at its end. */
export type ChildRunner = (app: Horsies, args: readonly string[]) => Promise<never>;

/** Seat isolate mode: serve the app as a seat isolate until the process ends. */
export type SeatRunner = (app: Horsies) => Promise<never>;

/** What `cli.run` does that has effects. */
export interface CliDeps {
  readonly facts: ProcessFacts;
  readonly table: CommandTable;
  readonly child: ChildRunner;
  readonly seat: SeatRunner;
  readonly output: CommandOutput;
  /** Whether core log records wait in the channel. Does not wait. */
  logBacklog(): LogBacklog;
  /** Wait `milliseconds`, letting the event loop run. */
  pause(milliseconds: number): Promise<void>;
  /** A monotonic clock, in milliseconds. */
  now(): number;
  /** The defect rule of the process (`reportDefect`): in a worker process it aborts. */
  reportDefect(defect: CoreDefect): void;
  /** End the process with `code`. */
  exit(code: number): never;
}

/** Run the command of this process, wait for the core log records, and end the process with the exit code. */
export async function runCli(app: Horsies, deps: CliDeps): Promise<number> {
  const mode = processMode(deps.facts);

  switch (mode.kind) {
    case "seatIsolate":
      return deps.seat(app);
    case "workerThread":
      // A worker thread has no process to end: the defect is thrown.
      return raiseDefect(
        new ClientDefect({ subject: "cli.run", rule: "cli.run runs on the main thread of a process, or in a seat isolate" }),
      );
    case "child":
      return deps.child(app, mode.args);
    case "usageError":
    case "command":
      return endProcess(await commandExitCode(app, mode, deps), deps);
    default:
      return unreachable(mode);
  }
}

/** The exit code of a usage error or a command. A command that throws gives 1 (`failedCommand`). */
export async function commandExitCode(
  app: Horsies,
  mode: Extract<ProcessMode, { readonly kind: "usageError" | "command" }>,
  deps: CliDeps,
): Promise<CommandExitCode> {
  switch (mode.kind) {
    case "usageError":
      deps.output.writeError(`usage: ${CLI_USAGE}`);
      deps.output.writeError(`horsies: error: ${mode.error.message}`);

      return EXIT_USAGE;
    case "command":
      try {
        return await deps.table[mode.command](app, mode.args);
      } catch (cause) {
        return failedCommand(mode.command, cause, deps);
      }

    default:
      return unreachable(mode);
  }
}

/**
 * The exit code of a command that threw `cause`: a defect of the package follows the defect rule first (in a worker
 * process the process aborts there); then the throw is written to stderr with its name and message. Never throws.
 */
function failedCommand(command: CommandName, cause: unknown, deps: Pick<CliDeps, "output" | "reportDefect">): CommandExitCode {
  if (cause instanceof CoreDefect) {
    deps.reportDefect(cause);
  }

  const facts = thrownFacts(cause);

  deps.output.writeError(`horsies ${command}: the command failed: ${facts.name}: ${facts.message}`);

  return EXIT_FAILED;
}

/** Wait for the core log records, then end the process with `code`. */
async function endProcess(code: CommandExitCode, deps: CliDeps): Promise<never> {
  await waitForLogRecords(deps);

  return deps.exit(code);
}

/**
 * Wait while core log records wait in the channel and the forwarder hands them on: until `EXIT_LOG_WAIT_MS` passed by
 * the clock, or `MAX_EXIT_LOG_POLLS` reads, the first.
 */
export async function waitForLogRecords(deps: Pick<CliDeps, "logBacklog" | "pause" | "now">): Promise<void> {
  const deadline = deps.now() + EXIT_LOG_WAIT_MS;

  for (let poll = 0; poll < MAX_EXIT_LOG_POLLS && deps.now() < deadline && deps.logBacklog() === "waiting"; poll += 1) {
    await deps.pause(EXIT_LOG_POLL_MS);
  }
}

/** The dependencies of `cli.run` in this process. */
export function processCliDeps(): CliDeps {
  return {
    facts: processFacts(),
    table: COMMANDS,
    child: async (app, args) => runChildMode(app, args, processChildModeDeps()),
    seat: async (app) => runSeatIsolate(app, processSeatIsolateDeps()),
    output: processOutput(),
    logBacklog: workerLogBacklog,
    pause: async (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)),
    now: () => performance.now(),
    reportDefect,
    exit: (code) => process.exit(code),
  };
}
