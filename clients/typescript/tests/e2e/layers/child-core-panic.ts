// A task child whose core background task panicked ends itself; it never runs on (Python
// `clients/python/tests/e2e/test_child_core_stopped.py`). Each test makes the log forwarder of an isolated child panic
// through the test export of the dev addon (`src/forwarder-panic.ts`), at the moments that the Python file names.
//
// A task child runs under the panic rule `ABORT_PROCESS`: at the panic it writes the abort line to stderr (the
// worker's log) and aborts, so no code of the child runs after the panic. Where Python's child goes on to its next
// core call with a stopped core, the TypeScript child is already gone:
// - The panic while the child holds no task: Python releases the next start with no attempt charged; here no start
//   reaches the ended child. Either way the next task runs once, in a new child.
// - The panic while a body runs that then returns: Python keeps the result of the body. Here the body never returns:
//   the attempt takes the crashed-child path (`WORKER_CRASHED`), and the retry policy runs the task again.
// - The panic while a body runs that then sends: the crashed-child path and the retry, as in Python.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe } from "node:test";

import type { TaskAttempt, TaskHandle } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { abortLine } from "../src/forwarder-panic.ts";
import * as basic from "../src/tasks/basic.ts";
import * as panic from "../src/tasks/core-panic.ts";
import { infoOf, sentHandle, succeeded } from "../support/outcome.ts";
import { processAlive, type TestProcess } from "../support/process.ts";
import { pollUntil } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 30_000;

/** The longest wait for the end of a task whose first run ends with its child. */
const CRASH_RETRY_WAIT_MS = 60_000;

/** The longest wait for the abort line of a child and for its process to end. */
const ABORT_WAIT_MS = 30_000;

/** An attempt as the tests compare it: its outcome, its code, whether it retries, and the process that ran it. */
type AttemptFacts = readonly [TaskAttempt["outcome"], TaskAttempt["errorCode"], boolean, TaskAttempt["workerPid"]];

/** The attempts of `handle`, oldest first, as `AttemptFacts`. */
async function attemptFacts<O, E extends string>(handle: TaskHandle<O, E>): Promise<readonly AttemptFacts[]> {
  const info = await infoOf(handle);

  return info.attempts.toReversed().map((attempt): AttemptFacts => [attempt.outcome, attempt.errorCode, attempt.willRetry, attempt.workerPid]);
}

/** The facts of an attempt that completed in child `pid`. Pure. */
function completedIn(pid: number): AttemptFacts {
  return ["COMPLETED", { kind: "notRecorded" }, false, { kind: "recorded", value: pid }];
}

/** The facts of an attempt whose child ended during the run, retried, in child `pid`. Pure. */
function crashedIn(pid: number): AttemptFacts {
  return ["FAILED", { kind: "recorded", value: "WORKER_CRASHED" }, true, { kind: "recorded", value: pid }];
}

/** Wait until `worker` logged the abort line of child `pid` and the child process ended. */
async function childAborted(suite: Suite, worker: TestProcess, pid: number): Promise<void> {
  await worker.waitForLog(abortLine(pid, panic.PANIC_TEXT), ABORT_WAIT_MS);
  await pollUntil(ABORT_WAIT_MS, `child ${String(pid)} ends after its abort line`, async () => !processAlive(pid));
  assert.equal(suite.markers.read(`entry-child-${String(pid)}`).kind, "found", `process ${String(pid)} was a child of the worker`);
}

/** The process ids of the marker `name`: one line each. Fails the test when it is absent. */
function pidLines(suite: Suite, name: string): readonly number[] {
  const read = suite.markers.read(name);

  assert.equal(read.kind, "found", `the marker ${name} exists`);

  return read.kind === "found" ? read.text.split("\n").filter((line) => line !== "").map(Number) : [];
}

describe("a task child whose core background task panicked", () => {
  // Python `test_child_core_stopped.py::test_a_start_refused_by_a_stopped_core_is_released_and_a_new_child_runs_it`.
  e2eTest("a panic while the child holds no task ends the child; the next task runs once in a new child", "default", async (suite) => {
    const worker = await suite.worker("isolated-1");
    const first = await sentHandle("e2e_core_panic_after_return", suite.app.send(panic.panicAfterReturn, {}));
    const stoppedChild = await succeeded(first, RESULT_WAIT_MS);

    await childAborted(suite, worker, stoppedChild);

    const second = await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 0 }));
    const newChild = await succeeded(second, RESULT_WAIT_MS);

    assert.notEqual(newChild, stoppedChild);
    assert.deepEqual(await attemptFacts(first), [completedIn(stoppedChild)], "the run before the panic keeps its result");
    assert.deepEqual(await attemptFacts(second), [completedIn(newChild)], "the next task was charged one attempt, in the new child");
  });

  // Python `test_child_core_stopped.py::test_a_body_that_finished_keeps_its_result_and_a_new_child_serves_next`.
  e2eTest("a panic while a body runs ends the child before the body returns: the crash path and the retry in a new child", "default", async (suite) => {
    const worker = await suite.worker("isolated-1");
    const token = randomUUID();
    const handle = await sentHandle("e2e_core_panic_in_body", suite.app.send(panic.panicInBody, { token }));
    const retriedIn = await succeeded(handle, CRASH_RETRY_WAIT_MS);
    const [stoppedChild, ...laterRuns] = pidLines(suite, `${token}-runs`);

    assert.ok(stoppedChild !== undefined, "the first run wrote its process id");
    await childAborted(suite, worker, stoppedChild);
    assert.deepEqual(laterRuns, [retriedIn], "one retry ran, in the new child");
    assert.deepEqual(pidLines(suite, `${token}-returned`), [retriedIn], "the first run did not reach its return");
    assert.deepEqual(await attemptFacts(handle), [crashedIn(stoppedChild), completedIn(retriedIn)]);
  });

  // Python `test_child_core_stopped.py::test_a_body_that_the_stopped_core_interrupted_takes_the_crash_path_and_the_retry`.
  e2eTest("a body that the panic interrupts before its send takes the crash path and the retry", "default", async (suite) => {
    const worker = await suite.worker("isolated-1");
    const token = randomUUID();
    const handle = await sentHandle("e2e_core_panic_then_send", suite.app.send(panic.panicThenSend, { token }));
    const retriedIn = await succeeded(handle, CRASH_RETRY_WAIT_MS);
    const [stoppedChild, ...rest] = pidLines(suite, `${token}-first-run`);

    assert.ok(stoppedChild !== undefined && rest.length === 0, "the first run wrote its process id");
    await childAborted(suite, worker, stoppedChild);
    assert.notEqual(retriedIn, stoppedChild);
    assert.deepEqual(await attemptFacts(handle), [crashedIn(stoppedChild), completedIn(retriedIn)]);
    assert.deepEqual(await suite.rows.tasksNamed(basic.pid.name), [], "the send after the panic did not run");
  });
});
