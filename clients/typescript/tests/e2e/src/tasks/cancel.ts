// Tasks that an operator cancels while they run (Rust `clients/rust/tests/worker/src/tasks/cancel.rs`, Python
// `clients/python/tests/e2e/tasks/cancel_tasks.py`): one per execution class and body kind that waits for the cancel,
// one that ignores it and is killed after its grace, and one that ignores it with no grace and runs to its end.
//
// Markers, each created once:
// - `<token>-started`: the process id of the child, when the body starts.
// - `<token>-saw`: `<cause>|<ms>`, the cause that a body that waits read and the wall clock in milliseconds then. An
//   async body reads the reason of `runtime.signal`; a sync body reads `runtime.cancelState()` between holds of its
//   thread. A sync body that reads no cancel within its wait writes `notRequested`.
// - `<token>-ended`: the process id of the child, when a body that ignores the cancel ends.

import { type CancelState, defineTask, h, type Horsies } from "../../../../horsies/src/index.ts";
import { err, ok, type Result } from "../../../../horsies/src/wire/result.ts";
import { createOnce, markerErrorText, unixMillis } from "../markers.ts";
import { holdThread, sleepFor } from "./waits.ts";

/** How long a body waits for its cancel, or runs when it ignores it and is killed: far past every wait of the tests. */
export const CANCEL_WAIT_MS = 120_000;

/** The hold of a sync body between two reads of its cancel state. */
const STATE_POLL_MS = 20;

/** The most reads of the cancel state of a sync body: `CANCEL_WAIT_MS` over `STATE_POLL_MS`. */
const STATE_POLLS = CANCEL_WAIT_MS / STATE_POLL_MS;

/** How long `e2e_cancel_ignored_no_grace` runs after its start, whatever the cancel. */
export const IGNORED_RUN_MS = 3_000;

/** The grace of `e2e_cancel_ignored_with_grace`: the worker kills its child this long after the cancel reached it. */
export const KILL_GRACE_MS = 500;

/** The deadline of the shared bodies: far past every wait of the tests. */
const SHARED_TIMEOUT_MS = 300_000;

/** The separator of the cause and the time in the marker `<token>-saw`. */
export const SAW_SEPARATOR = "|";

/** The text of the `<token>-saw` marker of a sync body that read no cancel. */
export const NOT_REQUESTED = "notRequested";

const CANCEL_CODES = ["MARKER_FAILED"] as const;

/** An isolated async body that waits for `runtime.signal`. */
export const cancelIsolatedAsync = defineTask({ name: "e2e_cancel_isolated_async", input: { token: h.string() }, output: h.string(), errors: CANCEL_CODES });

/** An isolated sync body that reads its cancel state between holds of its thread. */
export const cancelIsolatedSync = defineTask({ name: "e2e_cancel_isolated_sync", input: { token: h.string() }, output: h.string(), errors: CANCEL_CODES });

/** A shared async body that waits for `runtime.signal`. */
export const cancelSharedAsync = defineTask({
  name: "e2e_cancel_shared_async",
  input: { token: h.string() },
  output: h.string(),
  errors: CANCEL_CODES,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TIMEOUT_MS,
  cancelGraceMs: 2_000,
});

/** A shared thread sync body that reads its cancel state between holds of its seat thread. */
export const cancelSharedThread = defineTask({
  name: "e2e_cancel_shared_thread",
  input: { token: h.string() },
  output: h.string(),
  errors: CANCEL_CODES,
  execution: "SHARED_THREAD",
  timeoutMs: SHARED_TIMEOUT_MS,
});

/** An isolated sync body that ignores the cancel: the worker kills its child `KILL_GRACE_MS` after the cancel. */
export const cancelIgnoredWithGrace = defineTask({
  name: "e2e_cancel_ignored_with_grace",
  input: { token: h.string() },
  output: h.string(),
  errors: CANCEL_CODES,
  cancelGraceMs: KILL_GRACE_MS,
});

/** An isolated sync body with no grace that ignores the cancel: the worker kills nothing, so it runs to its end. */
export const cancelIgnoredNoGrace = defineTask({ name: "e2e_cancel_ignored_no_grace", input: { token: h.string() }, output: h.string(), errors: CANCEL_CODES });

/** Every contract of this module. */
export const CANCEL_CONTRACTS = [
  cancelIsolatedAsync,
  cancelIsolatedSync,
  cancelSharedAsync,
  cancelSharedThread,
  cancelIgnoredWithGrace,
  cancelIgnoredNoGrace,
] as const;

/** The text that a body returns once its cancel wait ended. */
const RETURNED = "returned after the cancel";

/** The text that a body that ignores the cancel returns. */
const RAN_TO_ITS_END = "ran to its end";

/** The implementations of this module on `app`. */
export function implementCancel(app: Horsies): void {
  app.implement(cancelIsolatedAsync, async ({ token }, { ok, err, signal }) => {
    const waited = await awaitCancel(token, signal);

    return waited.ok ? ok(RETURNED) : err({ code: "MARKER_FAILED", message: waited.error });
  });
  app.implement(cancelIsolatedSync, ({ token }, { ok, err, cancelState }) => {
    const waited = holdUntilCancel(token, () => cancelState());

    return waited.ok ? ok(RETURNED) : err({ code: "MARKER_FAILED", message: waited.error });
  });
  app.implement(cancelSharedAsync, async ({ token }, { ok, err, signal }) => {
    const waited = await awaitCancel(token, signal);

    return waited.ok ? ok(RETURNED) : err({ code: "MARKER_FAILED", message: waited.error });
  });
  app.implement(cancelSharedThread, ({ token }, { ok, err, cancelState }) => {
    const waited = holdUntilCancel(token, () => cancelState());

    return waited.ok ? ok(RETURNED) : err({ code: "MARKER_FAILED", message: waited.error });
  });
  app.implement(cancelIgnoredWithGrace, ({ token }, { ok, err }) => {
    const ran = holdIgnoringCancel(token, CANCEL_WAIT_MS);

    return ran.ok ? ok(RAN_TO_ITS_END) : err({ code: "MARKER_FAILED", message: ran.error });
  });
  app.implement(cancelIgnoredNoGrace, ({ token }, { ok, err }) => {
    const ran = holdIgnoringCancel(token, IGNORED_RUN_MS);

    return ran.ok ? ok(RAN_TO_ITS_END) : err({ code: "MARKER_FAILED", message: ran.error });
  });
}

/** Create the marker `<token>-<suffix>` with `content`; the marker failure text on failure. */
function mark(token: string, suffix: string, content: string): Result<undefined, string> {
  const created = createOnce(`${token}-${suffix}`, content);

  return created.ok ? ok(undefined) : err(markerErrorText(created.error));
}

/** Mark the start, then wait on the event loop for `signal`, at most `CANCEL_WAIT_MS`, and mark the cause it fired with. */
async function awaitCancel(token: string, signal: AbortSignal): Promise<Result<undefined, string>> {
  const started = mark(token, "started", String(process.pid));

  if (!started.ok) {
    return started;
  }

  const end = await sleepFor(CANCEL_WAIT_MS, signal);

  switch (end.kind) {
    case "slept":
      return ok(undefined);
    case "aborted":
      return mark(token, "saw", `${end.reason}${SAW_SEPARATOR}${String(unixMillis())}`);
  }
}

/** Mark the start, then read the cancel state between holds of the thread, and mark what the last read gave. */
function holdUntilCancel(token: string, readState: () => CancelState): Result<undefined, string> {
  const started = mark(token, "started", String(process.pid));

  if (!started.ok) {
    return started;
  }

  return mark(token, "saw", `${stateText(stateAfterHolds(readState))}${SAW_SEPARATOR}${String(unixMillis())}`);
}

/** Read the cancel state until a cancel is requested, at most `STATE_POLLS` times; the last state read. */
function stateAfterHolds(readState: () => CancelState): CancelState {
  for (let read = 0; read < STATE_POLLS; read += 1) {
    const state = readState();

    switch (state.kind) {
      case "requested":
        return state;
      case "notRequested":
        holdThread(STATE_POLL_MS);
    }
  }

  return readState();
}

/** The marker text of a cancel state: its cause, or `NOT_REQUESTED`. Pure. */
function stateText(state: CancelState): string {
  switch (state.kind) {
    case "requested":
      return state.cause;
    case "notRequested":
      return NOT_REQUESTED;
  }
}

/** Mark the start, hold the thread for `durationMs` whatever the cancel, then mark the end. */
function holdIgnoringCancel(token: string, durationMs: number): Result<undefined, string> {
  const started = mark(token, "started", String(process.pid));

  if (!started.ok) {
    return started;
  }

  holdThread(durationMs);

  return mark(token, "ended", String(process.pid));
}
