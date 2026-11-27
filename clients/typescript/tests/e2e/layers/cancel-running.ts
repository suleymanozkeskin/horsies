// The operator cancel of a RUNNING task on a live worker (`app.monitoring.cancelTask` with `includeRunning`), per
// execution class and body kind (Rust `clients/rust/tests/worker/tests/cancel_running.rs`, Python
// `clients/python/tests/e2e/test_cancel_running.py`):
//
// - the task code sees the cancel within 2 s on a local database: an async body through `runtime.signal`, a sync body
//   through `runtime.cancelState()`, both with the cause `OPERATOR`;
// - the history row is `CANCELLED` with one attempt, whose outcome is `CANCELLED` with the code `TASK_CANCELLED`, which
//   the cancel statement recorded; the result reads as `TASK_CANCELLED`;
// - an `ISOLATED` task with `cancelGraceMs` that ignores the cancel is killed with its child after the grace; one with
//   no grace is not killed and runs to its end in the same child.
//
// `SHARED_THREAD` children run on Node only: on Bun the case of the thread seat is skipped (the worker refuses the
// profile, `typescript-child.ts`). Run it with `run.ts` (see that file).

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe, test } from "node:test";

import type { TaskHandle, TaskId } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { unixMillis } from "../src/markers.ts";
import * as basic from "../src/tasks/basic.ts";
import * as cancel from "../src/tasks/cancel.ts";
import { runtimeName } from "../src/tasks/waits.ts";
import { errorCode, failed, infoOf, sentHandle, succeeded } from "../support/outcome.ts";
import { found, NOT_YET, pollFor, type Probe } from "../support/poll.ts";
import { processAlive } from "../support/process.ts";
import { e2eTest, READY_LIMIT_MS, type Suite, WORKER_READY_LINE } from "../support/suite.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest wait for a task to start on a running worker. */
const START_WAIT_MS = 30_000;

/** The bound from the cancel to the moment the task code read it. */
const SEEN_BOUND_MS = 2_000;

/** The longest wait for a marker that a task writes after the cancel. */
const MARKER_WAIT_MS = 15_000;

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 30_000;

/** The longest wait for the child of the hard stop to be gone: the grace, then the kill and the wait for the exit. */
const KILL_WAIT_MS = 15_000;

/** The cause of an operator cancel, as the bodies write it. */
const OPERATOR = "OPERATOR";

/** Why the thread seat case does not run on Bun. */
const THREAD_SEATS_ON_NODE_ONLY = "SHARED_THREAD children run on Node only: the worker on Bun refuses the profile";

/** A token of one test task: the prefix of its markers. */
function newToken(): string {
  return `cancel-${randomUUID()}`;
}

/** The text of marker `name` once it exists and is written. Fails the test when `limitMs` passes first. */
async function markerText(suite: Suite, name: string, limitMs: number): Promise<string> {
  return pollFor(limitMs, `the marker ${name}`, async (): Promise<Probe<string>> => {
    const read = suite.markers.read(name);

    return read.kind === "found" && read.text !== "" ? found(read.text) : NOT_YET;
  });
}

/** The process id of the child that started the task of `token`. */
async function startedPid(suite: Suite, token: string): Promise<number> {
  return Number(await markerText(suite, `${token}-started`, START_WAIT_MS));
}

/** Cancel the running task `taskId`; the wall clock in milliseconds right before the cancel. */
async function cancelRunning(suite: Suite, taskId: TaskId): Promise<number> {
  const cancelAt = unixMillis();
  const cancelled = await suite.app.monitoring.cancelTask(taskId, { policy: "includeRunning" });

  assert.ok(cancelled.ok, `cancel the running task ${taskId}: ${cancelled.ok ? "" : cancelled.error.code}`);

  return cancelAt;
}

/** The task of `token` read the operator cancel within `SEEN_BOUND_MS` of `cancelAt`. */
async function assertSeenInTime(suite: Suite, token: string, cancelAt: number): Promise<void> {
  const saw = await markerText(suite, `${token}-saw`, MARKER_WAIT_MS);
  const [cause, at, ...rest] = saw.split(cancel.SAW_SEPARATOR);

  assert.ok(cause !== undefined && at !== undefined && rest.length === 0, `the saw marker is <cause>|<ms>, not ${saw}`);
  assert.equal(cause, OPERATOR);
  assert.ok(Number(at) - cancelAt <= SEEN_BOUND_MS, `the code saw the cancel ${String(Number(at) - cancelAt)} ms after it`);
}

/**
 * The task is `CANCELLED` in history with one attempt: `CANCELLED`, code `TASK_CANCELLED`, no retry, started, and
 * recorded by the cancel statement. Its result reads as `TASK_CANCELLED`.
 */
async function assertCancelledWithOneAttempt<O, E extends string>(suite: Suite, handle: TaskHandle<O, E>): Promise<void> {
  assert.deepEqual(errorCode(await failed(handle, RESULT_WAIT_MS)), { source: "builtIn", code: "TASK_CANCELLED" });

  const info = await infoOf(handle);

  assert.equal(info.status, "CANCELLED");
  assert.deepEqual(
    info.attempts.map((attempt) => ({ attempt: attempt.attempt, outcome: attempt.outcome, errorCode: attempt.errorCode, willRetry: attempt.willRetry })),
    [{ attempt: 1, outcome: "CANCELLED", errorCode: { kind: "recorded", value: "TASK_CANCELLED" }, willRetry: false }],
  );

  const read = await suite.app.monitoring.getTaskDetail(handle.taskId);

  assert.ok(read.ok && read.value.kind === "found", `read the task detail: ${read.ok ? read.value.kind : read.error.code}`);

  const detail = read.value.taskDetail;
  const [attempt, ...others] = detail.attempts;

  assert.equal(detail.leaf.status, "CANCELLED");
  assert.ok(attempt !== undefined && others.length === 0, `one attempt in the detail, not ${String(detail.attempts.length)}`);
  assert.notEqual(attempt.startedAt, null, "the cancelled attempt started");
  assert.equal(attempt.phases.kind === "captured" ? attempt.phases.recordedBy.kind : attempt.phases.kind, "cancel");
}

/** The process id of the isolated child that runs the next task. */
async function isolatedPid(suite: Suite): Promise<number> {
  return succeeded(await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 0 })), RESULT_WAIT_MS);
}

/** The case of the thread seat: on Node a test of `body` on a worker of `thread-only-1x4`; on Bun a skipped test. */
function threadSeatTest(name: string, body: (suite: Suite) => Promise<void>): void {
  switch (runtimeName()) {
    case "node":
      e2eTest(name, "thread_profiles", body);

      return;
    case "bun":
      test(name, { skip: THREAD_SEATS_ON_NODE_ONLY }, () => undefined);
  }
}

describe("the operator cancel of a running task", () => {
  e2eTest("an isolated async body sees the cancel through runtime.signal with the cause OPERATOR", "default", async (suite) => {
    await suite.worker("isolated-1");

    const token = newToken();
    const handle = await sentHandle("e2e_cancel_isolated_async", suite.app.send(cancel.cancelIsolatedAsync, { token }));

    await startedPid(suite, token);
    await assertSeenInTime(suite, token, await cancelRunning(suite, handle.taskId));
    await assertCancelledWithOneAttempt(suite, handle);
  });

  e2eTest("an isolated sync body sees the cancel through runtime.cancelState() with the cause OPERATOR", "default", async (suite) => {
    await suite.worker("isolated-1");

    const token = newToken();
    const handle = await sentHandle("e2e_cancel_isolated_sync", suite.app.send(cancel.cancelIsolatedSync, { token }));

    await startedPid(suite, token);
    await assertSeenInTime(suite, token, await cancelRunning(suite, handle.taskId));
    await assertCancelledWithOneAttempt(suite, handle);
  });

  e2eTest("a shared async body sees the cancel through runtime.signal with the cause OPERATOR", "default", async (suite) => {
    await suite.worker("shared-only-1x4");

    const token = newToken();
    const handle = await sentHandle("e2e_cancel_shared_async", suite.app.send(cancel.cancelSharedAsync, { token }));

    await startedPid(suite, token);
    await assertSeenInTime(suite, token, await cancelRunning(suite, handle.taskId));
    await assertCancelledWithOneAttempt(suite, handle);
  });

  threadSeatTest("a shared thread body sees the cancel through runtime.cancelState() on its seat with the cause OPERATOR", async (suite) => {
    const worker = suite.spawnWorker("thread-only-1x4");

    await worker.waitForLog(WORKER_READY_LINE, READY_LIMIT_MS);

    const token = newToken();
    const handle = await sentHandle("e2e_cancel_shared_thread", suite.app.send(cancel.cancelSharedThread, { token }));

    await startedPid(suite, token);
    await assertSeenInTime(suite, token, await cancelRunning(suite, handle.taskId));
    await assertCancelledWithOneAttempt(suite, handle);
  });

  e2eTest("an isolated task with a grace that ignores the cancel is killed with its child after the grace", "default", async (suite) => {
    await suite.worker("isolated-1");

    const token = newToken();
    const handle = await sentHandle("e2e_cancel_ignored_with_grace", suite.app.send(cancel.cancelIgnoredWithGrace, { token }));
    const cancelledPid = await startedPid(suite, token);
    const cancelAt = await cancelRunning(suite, handle.taskId);

    const goneAt = await pollFor(KILL_WAIT_MS, "the child is killed after its grace", async () =>
      processAlive(cancelledPid) ? NOT_YET : found(unixMillis()),
    );

    assert.ok(goneAt - cancelAt >= cancel.KILL_GRACE_MS, `the child was gone ${String(goneAt - cancelAt)} ms after the cancel`);
    assert.notEqual(await isolatedPid(suite), cancelledPid, "the next task runs in a new child");
    assert.deepEqual(suite.markers.read(`${token}-ended`), { kind: "absent" });
    await assertCancelledWithOneAttempt(suite, handle);
  });

  e2eTest("an isolated task with no grace that ignores the cancel is not killed: it runs to its end, and its child runs the next task", "default", async (suite) => {
    await suite.worker("isolated-1");

    const token = newToken();
    const handle = await sentHandle("e2e_cancel_ignored_no_grace", suite.app.send(cancel.cancelIgnoredNoGrace, { token }));
    const cancelledPid = await startedPid(suite, token);

    await cancelRunning(suite, handle.taskId);

    const endedPid = Number(await markerText(suite, `${token}-ended`, cancel.IGNORED_RUN_MS + MARKER_WAIT_MS));

    assert.equal(endedPid, cancelledPid);
    assert.ok(processAlive(cancelledPid), "the child lives after its body returned");
    assert.equal(await isolatedPid(suite), cancelledPid);
    await assertCancelledWithOneAttempt(suite, handle);
  });
});
