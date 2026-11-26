// Event handlers on a live worker (Rust `clients/rust/tests/worker/tests/event_handlers.rs`, Python
// `clients/python/tests/e2e/test_event_handlers.py`): an app with a handler gets a handler child (the worker entry in
// child mode), which runs the `onHandlerChildStart` hook once, then gets the `AttemptEnded` event of each attempt that
// the worker ran and calls the handler. Run it with `run.ts` (see that file).

import assert from "node:assert/strict";
import { describe } from "node:test";

import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { attemptEndedLine, HANDLER_LINES_MARKER, STARTED_PREFIX } from "../src/handlers.ts";
import * as basic from "../src/tasks/basic.ts";
import { sentHandle, succeeded } from "../support/outcome.ts";
import { found, NOT_YET, pollFor } from "../support/poll.ts";
import { e2eTest } from "../support/suite.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 15_000;

/**
 * The longest wait for the handler to record an event after the task ended: the worker writes an events frame within
 * its flush interval, and the handler child takes it at once.
 */
const HANDLER_WAIT_MS = 20_000;

describe("event handlers", () => {
  e2eTest("the handler child runs its start hook once, then its AttemptEnded handler gets the end of a task's attempt", "handlers", async (suite) => {
    const worker = await suite.worker("isolated-1");
    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 21 }));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), 42);

    const line = attemptEndedLine(handle.taskId, 1, "reportedByChild");

    const lines = await pollFor(HANDLER_WAIT_MS, "the handler records the end of the attempt", async () => {
      const read = suite.markers.read(HANDLER_LINES_MARKER);
      const recorded = read.kind === "found" ? read.text.split("\n") : [];

      return recorded.includes(line) ? found(recorded) : NOT_YET;
    });

    const started = lines.filter((recorded) => recorded.startsWith(STARTED_PREFIX));

    assert.equal(started.length, 1, lines.join("\n"));
    assert.equal(lines[0], started[0], "the start hook ran before the first event");

    const hookPid = Number(started[0]?.slice(STARTED_PREFIX.length));

    assert.ok(hookPid !== process.pid && hookPid !== worker.pid, `the hook ran in the handler child, not in ${String(hookPid)}`);
  });
});
