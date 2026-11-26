// The child model of a TypeScript worker: each execution class runs its bodies in children of the worker entry, and
// a timeout, a blocked event loop, a crash or a drain ends one child, never the worker (Rust
// `clients/rust/tests/worker/tests/child_model.rs`, Python `clients/python/tests/e2e/test_shared_async_tasks.py`,
// `test_task_timeout.py`, `test_child_recycling.py`).
//
// Where a JavaScript body ends otherwise than a Rust body, the test asserts the TypeScript rule:
// - A `SHARED_ASYNC` body that does not pass `runtime.signal` cannot be dropped. Its run keeps its seat, the child
//   takes no new run, and the worker kills the child once no run in it has time left: the stored `cancel_end` is
//   `KILLED_WITH_CHILD` (Rust: `DROPPED_AFTER_GRACE`), and a stuck run waits for its neighbour, as in Python.
// - A block of the event loop after an `await` names its run on Node (promise hooks). Bun has no method to name it,
//   so every run of the child takes the crash path (`WORKER_CRASHED`) and retries in its class.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe } from "node:test";

import { type TaskAttempt, type TaskHandle, TaskId } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { TEST_DRAIN_TIMEOUT_MS } from "../src/instances.ts";
import * as basic from "../src/tasks/basic.ts";
import * as fencing from "../src/tasks/fencing.ts";
import * as shared from "../src/tasks/shared.ts";
import { runtimeName } from "../src/tasks/waits.ts";
import { dataMembers, errorCode, failed, failedCode, infoOf, sentHandle, succeeded } from "../support/outcome.ts";
import { exitText } from "../support/process.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 30_000;

/** How long the neighbour of a stuck task runs. */
const NEIGHBOUR_MS = 8_000;

/** The process id of the isolated child of a split profile. */
async function isolatedPid(suite: Suite): Promise<number> {
  return succeeded(await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 0 })), RESULT_WAIT_MS);
}

/** The parts of a `<pid>:<start ms>:<end ms>` stamp. */
function stampParts(stamp: string): { readonly pid: number; readonly start: number; readonly end: number } {
  const [pid, start, end, ...rest] = stamp.split(":");

  assert.ok(pid !== undefined && start !== undefined && end !== undefined && rest.length === 0, `a stamp is pid:start:end, not ${stamp}`);

  return { pid: Number(pid), start: Number(start), end: Number(end) };
}

/** The first attempt of the info of a task. */
async function firstAttempt<O, E extends string>(handle: TaskHandle<O, E>): Promise<TaskAttempt> {
  const attempt = (await infoOf(handle)).attempts.find((read) => read.attempt === 1);

  assert.ok(attempt !== undefined, `task ${handle.taskId} has no first attempt`);

  return attempt;
}

describe("the child model", () => {
  e2eTest("shared async runs run at once; a body past its deadline that ignores the signal leaves its neighbours finished", "default", async (suite) => {
    await suite.worker("split-isolated-1-shared-1x4");

    const isolated = await isolatedPid(suite);
    const hang = await sentHandle("e2e_shared_hang", suite.app.send(shared.sharedHang, {}));
    const sleepers = [];

    for (let index = 0; index < 3; index += 1) {
      sleepers.push(await sentHandle("e2e_shared_sleep", suite.app.send(shared.sharedSleep, { milliseconds: 1_500 })));
    }

    const stamps = [];

    for (const sleeper of sleepers) {
      stamps.push(stampParts(await succeeded(sleeper, RESULT_WAIT_MS)));
    }

    const error = await failed(hang, RESULT_WAIT_MS);
    const data = dataMembers(error);

    assert.equal(errorCode(error).code, "TASK_TIMEOUT");
    assert.equal(data["cancel_end"], "KILLED_WITH_CHILD");
    assert.equal(data["timeout_ms"], shared.STOP_TIMEOUT_MS);

    const pids = new Set(stamps.map((stamp) => stamp.pid));

    assert.equal(pids.size, 1, "the sleepers share one shared child");
    assert.ok(!pids.has(isolated), "not the isolated child");
    assert.ok(Math.max(...stamps.map((stamp) => stamp.start)) < Math.min(...stamps.map((stamp) => stamp.end)), "the sleepers ran at once");
  });

  e2eTest("a shared async body that passes the signal ends at the deadline: RETURNED_AFTER_SIGNAL, cause DEADLINE", "default", async (suite) => {
    await suite.worker("shared-only-1x4");

    const token = randomUUID();
    const handle = await sentHandle("e2e_shared_cooperative", suite.app.send(shared.sharedCooperative, { token }));
    const error = await failed(handle, RESULT_WAIT_MS);

    assert.equal(errorCode(error).code, "TASK_TIMEOUT");
    assert.equal(dataMembers(error)["cancel_end"], "RETURNED_AFTER_SIGNAL");
    assert.deepEqual(suite.markers.read(`${token}-cause`), { kind: "found", text: "DEADLINE\n" });

    const info = await infoOf(handle);

    assert.deepEqual([info.status, info.retryCount], ["FAILED", 0]);
  });

  e2eTest("a stuck shared async task waits for its neighbour, then retries in the isolated child", "default", async (suite) => {
    await suite.worker("split-isolated-1-shared-1x4");

    const isolated = await isolatedPid(suite);
    const token = randomUUID();
    const neighbour = await sentHandle("the neighbour", suite.app.send(shared.sharedSleep, { milliseconds: NEIGHBOUR_MS }));
    const stuck = await sentHandle("the stuck task", suite.app.send(shared.sharedStuckOnce, { token }));
    const neighbourPid = stampParts(await succeeded(neighbour, RESULT_WAIT_MS)).pid;
    const retryPid = await succeeded(stuck, RESULT_WAIT_MS);
    const info = await infoOf(stuck);
    const first = await firstAttempt(stuck);

    assert.deepEqual(suite.markers.read(`${token}-stuck`), { kind: "found", text: String(neighbourPid) });
    assert.equal(retryPid, isolated, "the retry ran in the isolated child");
    assert.deepEqual([info.status, info.retryCount], ["COMPLETED", 1]);
    assert.deepEqual([first.errorCode, first.willRetry], [{ kind: "recorded", value: "TASK_TIMEOUT" }, true]);

    // Alone, the stuck run would be killed about 2.4 s after its start; it ended only after its neighbour ended.
    const ranMs = first.finishedAt.epochMilliseconds - first.startedAt.epochMilliseconds;

    assert.ok(ranMs >= NEIGHBOUR_MS - 1_000, `the stuck attempt ended ${String(ranMs)} ms after its start`);
  });

  e2eTest("a block of the event loop before the first await kills the child, names the run, and the worker runs on", "default", async (suite) => {
    await suite.worker("split-isolated-1-shared-1x4");

    const isolated = await isolatedPid(suite);
    const neighbour = await sentHandle("the neighbour", suite.app.send(shared.sharedSleep, { milliseconds: 20_000 }));

    await suite.rows.waitForStatus(neighbour.taskId, "RUNNING", RESULT_WAIT_MS);

    const culprit = await sentHandle("the culprit", suite.app.send(shared.sharedBlocksOnce, { token: randomUUID() }));

    await suite.rows.waitForStatus(culprit.taskId, "RUNNING", RESULT_WAIT_MS);

    const sentAt = performance.now();
    const during = await sentHandle("a task during the block", suite.app.send(basic.simple, { x: 3 }));

    assert.equal(await succeeded(during, RESULT_WAIT_MS), 6);
    assert.ok(performance.now() - sentAt < shared.BLOCK_DURATION_MS / 2, "a task sent during the block waited for it");
    assert.equal(await succeeded(culprit, 60_000), isolated, "the culprit's retry ran in the isolated child");

    const info = await infoOf(culprit);

    assert.deepEqual([info.status, info.retryCount], ["COMPLETED", 1]);
    assert.deepEqual((await firstAttempt(culprit)).errorCode, { kind: "recorded", value: "TASK_BLOCKED_LOOP" });
    assert.equal(await failedCode(neighbour, RESULT_WAIT_MS), "NEIGHBOUR_BLOCKED_LOOP");
    assert.equal((await infoOf(neighbour)).retryCount, 0);
  });

  e2eTest("a block of the event loop after an await: Node names the run; Bun names none and every run takes the crash path", "default", async (suite) => {
    await suite.worker("split-isolated-1-shared-1x4");

    const isolated = await isolatedPid(suite);
    const neighbour = await sentHandle("the neighbour", suite.app.send(shared.sharedSleepRetried, { milliseconds: 20_000 }));

    await suite.rows.waitForStatus(neighbour.taskId, "RUNNING", RESULT_WAIT_MS);

    const culprit = await sentHandle("the culprit", suite.app.send(shared.sharedBlocksAfterAwait, { token: randomUUID() }));
    const retryPid = await succeeded(culprit, 60_000);
    const culpritFirst = (await firstAttempt(culprit)).errorCode;

    switch (runtimeName()) {
      case "node":
        assert.deepEqual(culpritFirst, { kind: "recorded", value: "TASK_BLOCKED_LOOP" });
        assert.equal(retryPid, isolated, "the culprit's retry ran in the isolated child");
        assert.equal(await failedCode(neighbour, RESULT_WAIT_MS), "NEIGHBOUR_BLOCKED_LOOP");

        return;
      case "bun":
        assert.deepEqual(culpritFirst, { kind: "recorded", value: "WORKER_CRASHED" });
        assert.notEqual(retryPid, isolated, "the culprit's retry ran in its class, in a new shared child");
        await succeeded(neighbour, 60_000);
        assert.deepEqual((await firstAttempt(neighbour)).errorCode, { kind: "recorded", value: "WORKER_CRASHED" });
    }
  });

  e2eTest("a stop while a task runs past the drain timeout ends the task with WORKER_CRASHED and the worker with 0", "default", async (suite) => {
    const worker = await suite.worker("isolated-1");
    const handle = await sentHandle("e2e_outlives_drain", suite.app.send(basic.outlivesDrain, { seconds: 60 }));

    await suite.rows.waitForStatus(handle.taskId, "RUNNING", RESULT_WAIT_MS);

    const stoppedAt = performance.now();
    const end = await worker.stop();
    const tookMs = performance.now() - stoppedAt;

    assert.deepEqual(end, { kind: "exitedAfterStop", exit: { kind: "exitedWithCode", code: 0 } }, worker.report());
    assert.ok(tookMs < TEST_DRAIN_TIMEOUT_MS + 20_000, `the exit came ${String(Math.round(tookMs))} ms after the stop`);
    await suite.rows.waitForStatus(handle.taskId, "FAILED", RESULT_WAIT_MS);
    assert.equal((await suite.rows.taskRow(handle.taskId)).errorCode, "WORKER_CRASHED");
  });

  for (const ending of [
    { name: "process.abort()", contract: basic.abort },
    { name: "process.exit(3)", contract: basic.exitsChild },
  ] as const) {
    e2eTest(`a body that ends its child with ${ending.name} ends that child only; the worker runs on`, "default", async (suite) => {
      const worker = await suite.worker("isolated-2");
      const healthy = await sentHandle("the healthy task", suite.app.send(basic.slow, { durationMs: 3_000 }));
      const sentAt = performance.now();
      const crashing = await sentHandle(ending.contract.name, suite.app.send(ending.contract, {}));

      assert.equal(await failedCode(crashing, RESULT_WAIT_MS), "WORKER_CRASHED");
      assert.ok(performance.now() - sentAt < 25_000, "the crash was recorded without the stale threshold");
      assert.equal(await succeeded(healthy, RESULT_WAIT_MS), "slept_3000");
      assert.equal(worker.exitState().kind, "running", `the worker ended with the child: ${exitText(worker.exitState())}`);
      assert.equal(await succeeded(await sentHandle("after the crash", suite.app.send(basic.simple, { x: 4 })), RESULT_WAIT_MS), 8);
    });
  }

  e2eTest("an isolated child is replaced after its task limit: six tasks run in at least three children", "default", async (suite) => {
    await suite.worker("recycle-after-2-tasks");

    const pids = new Set<number>();

    for (let index = 0; index < 6; index += 1) {
      pids.add(await succeeded(await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 0 })), RESULT_WAIT_MS));
    }

    assert.ok(pids.size >= 3, `six tasks ran in ${String(pids.size)} children`);
  });

  e2eTest("two runs on one shared async child each read the token of their own task", "default", async (suite) => {
    await suite.worker("shared-only-1x4");

    const first = await sentHandle("e2e_fence_shared_async", suite.app.send(fencing.fenceSharedAsync, {}));
    const second = await sentHandle("e2e_fence_shared_async", suite.app.send(fencing.fenceSharedAsync, {}));

    for (const handle of [first, second]) {
      assert.deepEqual(await succeeded(handle, RESULT_WAIT_MS), { taskId: handle.taskId, value: 1 });
    }
  });

  e2eTest("a shared async body sends a task that a worker runs", "default", async (suite) => {
    await suite.worker("split-isolated-1-shared-1x4");

    for (const x of [5, 6]) {
      const sentId = await succeeded(await sentHandle("e2e_shared_sends", suite.app.send(shared.sharedSends, { x })), RESULT_WAIT_MS);

      assert.equal(await succeeded(suite.app.taskHandle(basic.simple, sentIdOf(sentId)), RESULT_WAIT_MS), x * 2);
    }
  });
});

/** The task id that a body gave. Fails the test for a text that is not an id. */
function sentIdOf(text: string): TaskId {
  const parsed = TaskId.parse(text);

  assert.ok(parsed.ok, `the body gave the id ${text}`);

  return parsed.value;
}
