// The process events of a task child.
//
// - `SIGINT`, `SIGTERM`: no-op handlers, so a signal sent to every process drains through the parent. Node and Bun have
//   no call that sets a signal to ignored; a handler resets at `exec`, so a subprocess of task code keeps the default.
// - `uncaughtException` with a run in the run store: the throw of a callback that a body scheduled. It is logged with
//   the task id, the task name and the attempt; the child lives and the run's outcome does not change.
// - `uncaughtException` with no run: code outside every run failed and the process state is not known. It is logged,
//   and the child ends with 1 through the exit rule; the parent replaces it.
// - `unhandledRejection`: a Promise that no body awaits is not tracked. It is logged (with the run of the store on
//   Node; Bun gives no store there); the child lives.

import { type CurrentRun, currentRun } from "./run-store.ts";
import { thrownRecord } from "./thrown-error.ts";

/** The exit code of a child whose code outside every run threw (Node's code of an uncaught exception). */
export const UNCAUGHT_EXIT_CODE = 1;

/** The process events that a child handles. */
export interface ProcessEventHooks {
  onSignal(signal: "SIGINT" | "SIGTERM", listener: () => void): void;
  onUncaught(listener: (cause: unknown) => void): void;
  onRejection(listener: (cause: unknown) => void): void;
}

/** What the handlers do that has effects. */
export interface ProcessEventDeps {
  readonly hooks: ProcessEventHooks;
  readonly writeLine: (line: string) => void;
  /** End the child with `code` through the exit rule. */
  readonly end: (code: number) => void;
  /** The run of the code that runs now. */
  readonly currentRun: () => CurrentRun;
}

/** The process events of this process. */
export function processEventHooks(): ProcessEventHooks {
  return {
    onSignal: (signal, listener) => {
      process.on(signal, listener);
    },
    onUncaught: (listener) => {
      process.on("uncaughtException", listener);
    },
    onRejection: (listener) => {
      process.on("unhandledRejection", listener);
    },
  };
}

/** Install the handlers of the process events of a child. */
export function installProcessEvents(deps: ProcessEventDeps): void {
  const ignore = (): void => undefined;

  deps.hooks.onSignal("SIGINT", ignore);
  deps.hooks.onSignal("SIGTERM", ignore);
  deps.hooks.onUncaught((cause) => {
    const run = deps.currentRun();

    switch (run.kind) {
      case "run":
        deps.writeLine(`${eventLine("a callback threw", run, cause)}; the child lives and the run's outcome does not change`);

        return;
      case "noRun":
        deps.writeLine(`${eventLine("code outside every run threw", run, cause)}; the child ends with ${String(UNCAUGHT_EXIT_CODE)}`);
        deps.end(UNCAUGHT_EXIT_CODE);

        return;
      default: {
        const unexpected: never = run;

        deps.writeLine(`horsies child: a run read outside its type: ${String(unexpected)}`);
      }
    }
  });
  deps.hooks.onRejection((cause) => {
    deps.writeLine(`${eventLine("a Promise that no body awaits rejected", deps.currentRun(), cause)}; the child lives`);
  });
}

/** One log line of an event: what happened, the run when the store names one, and the record of the value. Pure. */
function eventLine(what: string, run: CurrentRun, cause: unknown): string {
  const record = thrownRecord(cause);
  const where = runText(run);
  const traceback = record.traceback === "" ? "" : `\n${record.traceback}`;

  return `horsies child: ${what}${where}: ${record.type}: ${record.message}${traceback}`;
}

function runText(run: CurrentRun): string {
  switch (run.kind) {
    case "run":
      return ` in the run of task ${run.mark.taskName} (task ${run.mark.taskId}, attempt ${String(run.mark.attempt)})`;
    case "noRun":
      return "";
    default: {
      const unexpected: never = run;

      return ` (a run outside its type: ${String(unexpected)})`;
    }
  }
}

/** The run of the code that runs now, from the run store. */
export const storeRun: () => CurrentRun = currentRun;
