// The event handler and the handler child start hook of the `handlers` instance (Rust
// `clients/rust/tests/worker/src/handlers.rs`, Python `clients/python/tests/e2e/tasks/instance_event_handlers.py`).
//
// Both append one line to the marker `handler-lines`, which a test reads: the hook `started <pid>` once per handler
// child, the handler `<task id> <attempt> <end kind>` for each attempt that ended.

import type { Horsies } from "#horsies";
import { holdThread } from "./tasks/waits.ts";
import { appendLine, markerErrorText } from "./markers.ts";

/** The marker of the handler lines. */
export const HANDLER_LINES_MARKER = "handler-lines";

/** The name of the handler of the `AttemptEnded` events. */
export const ATTEMPT_ENDED_HANDLER = "e2e.attempt_ended";

/** The prefix of the line of the start hook. */
export const STARTED_PREFIX = "started ";

/** The line of the `AttemptEnded` event of `attempt` of task `taskId`, which ended as `endKind`. Pure. */
export function attemptEndedLine(taskId: string, attempt: number, endKind: string): string {
  return `${taskId} ${String(attempt)} ${endKind}`;
}

/** Append `line` to the marker. A failed append throws: the handler child logs and counts it, and the test sees no line. */
function writeLine(line: string): void {
  const appended = appendLine(HANDLER_LINES_MARKER, line);

  if (!appended.ok) {
    throw new Error(`the handler line ${JSON.stringify(line)}: ${markerErrorText(appended.error)}`);
  }
}

/** The start hook of the handler child and the handler of the `AttemptEnded` events, on `app`. */
export function registerHandlers(app: Horsies): void {
  app.onHandlerChildStart(() => {
    writeLine(`${STARTED_PREFIX}${String(process.pid)}`);
  });
  app.onEvent("AttemptEnded", { name: ATTEMPT_ENDED_HANDLER }, (event) => {
    writeLine(attemptEndedLine(event.taskId, event.attempt, event.end.kind));
  });
}

/** How long the hook of `handler_hook_past_bound` holds the handler child: past the 10 s bound of a start hook. */
export const HANDLER_HOOK_HOLD_MS = 15_000;

/** The handler child start hook that fails: it throws, or it holds the child past the bound. Registered before the recording hook. */
export function registerFailingHandlerChildStart(app: Horsies, instance: "handler_hook_throws" | "handler_hook_past_bound"): void {
  switch (instance) {
    case "handler_hook_throws":
      app.onHandlerChildStart(() => {
        throw new Error("e2e handler child start hook failure");
      });

      return;
    case "handler_hook_past_bound":
      app.onHandlerChildStart(() => {
        holdThread(HANDLER_HOOK_HOLD_MS);
      });

      return;
  }
}
