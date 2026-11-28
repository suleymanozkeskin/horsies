// A task child whose core background task panicked ends itself with exit code 80, as a Python child does
// (`clients/python/tests/e2e/test_child_core_stopped.py`). Each test makes the log forwarder of an isolated child panic
// through the test export of the dev addon (`src/forwarder-panic.ts`), at the moments that the Python file names.
//
// A task child runs under the task child rule once its child host read its core: at the panic its core stops (the
// stop line on stderr, the worker's log), and the child ends itself with exit code 80:
// - The panic while the child holds no task: the next start is refused with no attempt charged, the child ends, and
//   the next task runs once, in a new child.
// - The panic while a body runs that then returns: the result of the body is kept (one attempt, `COMPLETED`), and the
//   child ends after its result frame.
// - The panic while a body runs that then sends: the send is a refused core call, the child ends during the body, and
//   the attempt takes the crashed-child path (`WORKER_CRASHED`) and the retry policy of the task.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe } from "node:test";

import type { TaskAttempt, TaskHandle } from "#horsies";
import { loadAddon } from "#horsies";
import { childLastLine, endedItselfLine, refusedCallLine, stopLine } from "../src/forwarder-panic.ts";
import * as basic from "../src/tasks/basic.ts";
import * as panic from "../src/tasks/core-panic.ts";
import { infoOf, sentHandle, succeeded } from "../support/outcome.ts";
import { processAlive, type TestProcess } from "../support/process.ts";
import { pollUntil } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 30_000;

/** The longest wait for the end of a task whose first run ends with its child. */
const CRASH_RETRY_WAIT_MS = 60_000;

/** The longest wait for the stop line of a child, for its end line in the worker's log, and for its process to end. */
const END_WAIT_MS = 30_000;

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

/** Wait until `worker` logged the stop line of the core of child `pid`. */
async function coreStopped(suite: Suite, worker: TestProcess, pid: number): Promise<void> {
  await worker.waitForLog(stopLine(pid, panic.PANIC_TEXT), END_WAIT_MS);
  assert.equal(suite.markers.read(`entry-child-${String(pid)}`).kind, "found", `process ${String(pid)} was a child of the worker`);
}

/** Wait until child `pid` ended itself with exit code 80 while it held no task, and its process ended. */
async function childEndedItself(worker: TestProcess, pid: number): Promise<void> {
  await worker.waitForLog(endedItselfLine(pid), END_WAIT_MS);
  await pollUntil(END_WAIT_MS, `child ${String(pid)} ends after its core stopped`, async () => !processAlive(pid));
}

/** The process ids of the marker `name`: one line each. Fails the test when it is absent. */
function pidLines(suite: Suite, name: string): readonly number[] {
  const read = suite.markers.read(name);

  if (read.kind === "absent") {
    assert.fail(`the marker ${name} exists`);
  }

  return read.text.split("\n").filter((line) => line !== "").map(Number);
}

describe("a task child whose core background task panicked", () => {
  // Python `test_child_core_stopped.py::test_a_start_refused_by_a_stopped_core_is_released_and_a_new_child_runs_it`.
  e2eTest("a panic while the child holds no task ends the child; the next task runs once in a new child", "default", async (suite) => {
    const worker = await suite.worker("isolated-1");
    const first = await sentHandle("e2e_core_panic_after_return", suite.app.send(panic.panicAfterReturn, {}));
    const stoppedChild = await succeeded(first, RESULT_WAIT_MS);

    await coreStopped(suite, worker, stoppedChild);
    assert.ok(processAlive(stoppedChild), "a child that holds no task ends at its next start");

    const second = await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 0 }));
    const newChild = await succeeded(second, RESULT_WAIT_MS);

    await childEndedItself(worker, stoppedChild);
    await worker.waitForLog(childLastLine("At the start of task", second.taskId, panic.PANIC_TEXT), END_WAIT_MS);
    assert.notEqual(newChild, stoppedChild);
    assert.deepEqual(await attemptFacts(first), [completedIn(stoppedChild)], "the run before the panic keeps its result");
    assert.deepEqual(await attemptFacts(second), [completedIn(newChild)], "the next task was charged one attempt, in the new child");
  });

  // Python `test_child_core_stopped.py::test_a_body_that_finished_keeps_its_result_and_a_new_child_serves_next`.
  e2eTest("a body that returns after the panic keeps its result, and the child ends after its result", "default", async (suite) => {
    const worker = await suite.worker("isolated-1");
    const token = randomUUID();
    const handle = await sentHandle("e2e_core_panic_in_body", suite.app.send(panic.panicInBody, { token }));
    const ranIn = await succeeded(handle, RESULT_WAIT_MS);

    await coreStopped(suite, worker, ranIn);
    await childEndedItself(worker, ranIn);
    await worker.waitForLog(childLastLine("After the body of task", handle.taskId, panic.PANIC_TEXT), END_WAIT_MS);
    assert.deepEqual(pidLines(suite, `${token}-runs`), [ranIn], "one run, with no retry");
    assert.deepEqual(pidLines(suite, `${token}-returned`), [ranIn], "the run reached its return");
    assert.deepEqual(await attemptFacts(handle), [completedIn(ranIn)]);

    const next = await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 0 }));
    const newChild = await succeeded(next, RESULT_WAIT_MS);

    assert.notEqual(newChild, ranIn, "a new child serves the next task");
  });

  // Python `test_child_core_stopped.py::test_a_body_that_the_stopped_core_interrupted_takes_the_crash_path_and_the_retry`.
  e2eTest("a body that the panic interrupts before its send takes the crash path and the retry", "default", async (suite) => {
    const worker = await suite.worker("isolated-1");
    const token = randomUUID();
    const handle = await sentHandle("e2e_core_panic_then_send", suite.app.send(panic.panicThenSend, { token }));
    const retriedIn = await succeeded(handle, CRASH_RETRY_WAIT_MS);
    const [stoppedChild, ...rest] = pidLines(suite, `${token}-first-run`);

    assert.ok(stoppedChild !== undefined && rest.length === 0, "the first run wrote its process id");
    await coreStopped(suite, worker, stoppedChild);
    await worker.waitForLog(refusedCallLine(panic.PANIC_TEXT), END_WAIT_MS);
    await pollUntil(END_WAIT_MS, `child ${String(stoppedChild)} ends at the refused call`, async () => !processAlive(stoppedChild));
    assert.notEqual(retriedIn, stoppedChild);
    assert.deepEqual(await attemptFacts(handle), [crashedIn(stoppedChild), completedIn(retriedIn)]);
    assert.deepEqual(await suite.rows.tasksNamed(basic.pid.name), [], "the send after the panic did not run");
  });
});
