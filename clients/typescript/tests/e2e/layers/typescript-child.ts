// The cases that only the TypeScript client has: a child runs on the runtime of its parent and runs every line of the
// entry before `cli.run`; the providers and the start hooks of each child; `runtime.spawn` work and its signal; a
// throw in a callback that a body scheduled and a Promise that no body awaits; the deadline of an isolated run; and the
// stop of a worker by signal. Where a case mirrors a Python or Rust test, its comment names it. The cases of
// `SHARED_THREAD` children are in `shared-thread.ts`.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe } from "node:test";

import { loadAddon } from "#horsies";
import { providedState } from "../src/app.ts";
import { type Instance, TEST_DRAIN_TIMEOUT_MS } from "../src/instances.ts";
import * as basic from "../src/tasks/basic.ts";
import * as shared from "../src/tasks/shared.ts";
import * as spawned from "../src/tasks/spawned.ts";
import { runtimeName } from "../src/tasks/waits.ts";
import { dataMembers, errorCode, failed, infoOf, sentHandle, succeeded } from "../support/outcome.ts";
import { processAlive } from "../support/process.ts";
import { pollUntil } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 30_000;

/** The longest wait for a worker whose child start fails to end: a boot, the 10 s hook bound and the stop. */
const FAILED_START_WAIT_MS = 60_000;

/** The longest wait for a child to end after its parent was killed. */
const ORPHAN_END_WAIT_MS = 10_000;

/** The worker exit code of every run error (a refused start among them). */
const EXIT_FAILED = 1;

/** The process id of a child of the worker. */
async function childPid(suite: Suite): Promise<number> {
  return succeeded(await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 0 })), RESULT_WAIT_MS);
}

/** Start a worker of `instance` whose child start fails, and check that it stops with `CHILD_HOOK_FAILED`. */
async function startStopsWithHookFailure(suite: Suite, instance: Instance): Promise<void> {
  const worker = suite.spawnWorker("isolated-1", instance);
  const exit = await worker.waitExit(FAILED_START_WAIT_MS);

  assert.deepEqual(exit, { kind: "exitedWithCode", code: EXIT_FAILED }, worker.report());
  assert.ok(worker.logCount("CHILD_HOOK_FAILED") > 0, worker.report());
}

describe("the TypeScript child", () => {
  e2eTest("a child runs on the runtime of its parent", "default", async (suite) => {
    await suite.worker("isolated-1");

    const child = await succeeded(await sentHandle("e2e_runtime_of_child", suite.app.send(basic.runtimeOfChild, {})), RESULT_WAIT_MS);

    assert.equal(child.runtime, runtimeName());
  });

  e2eTest("every line of the entry before cli.run runs in the worker and in each child", "default", async (suite) => {
    const worker = await suite.worker("isolated-2");
    const pids = new Set<number>();

    for (let index = 0; index < 4; index += 1) {
      pids.add(await succeeded(await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 200 })), RESULT_WAIT_MS));
    }

    assert.equal(suite.markers.read(`entry-parent-${String(worker.pid)}`).kind, "found");

    for (const pid of pids) {
      assert.equal(suite.markers.read(`entry-child-${String(pid)}`).kind, "found", `the entry did not run in child ${String(pid)}`);
    }
  });

  e2eTest("the provider and the onChildStart hook run once in each task child; the provider's value reaches the body", "default", async (suite) => {
    await suite.worker("recycle-after-2-tasks");

    const pids = new Set<number>();

    for (let index = 0; index < 6; index += 1) {
      const state = await succeeded(await sentHandle("e2e_provided_state", suite.app.send(providedState, {})), RESULT_WAIT_MS);

      assert.equal(state.providerPid, state.bodyPid, "the state came from the provider of the body's own child");
      pids.add(state.bodyPid);
    }

    assert.ok(pids.size >= 3, "replacement children ran bodies too");

    for (const pid of pids) {
      assert.equal(suite.markers.read(`provider-${String(pid)}`).kind, "found");
      assert.equal(suite.markers.read(`hook-${String(pid)}`).kind, "found");
    }
  });

  // Python `test_child_process_hooks.py::test_failing_hook_stops_worker_at_boot`.
  e2eTest("an onChildStart hook that throws stops the worker with CHILD_HOOK_FAILED", "default", async (suite) => {
    await startStopsWithHookFailure(suite, "hook_throws");
  });

  e2eTest("a provider that throws stops the worker with CHILD_HOOK_FAILED", "default", async (suite) => {
    await startStopsWithHookFailure(suite, "provider_throws");
  });

  e2eTest("an onChildStart hook that holds its child past the 10 s bound stops the worker with CHILD_HOOK_FAILED", "default", async (suite) => {
    await startStopsWithHookFailure(suite, "hook_past_bound");
  });

  e2eTest("a throw in a callback that a shared async body scheduled is logged; the run, its neighbour and the child go on", "default", async (suite) => {
    const worker = await suite.worker("shared-only-1x4");
    const neighbour = await sentHandle("the neighbour", suite.app.send(shared.sharedSleep, { milliseconds: 1_000 }));
    const thrower = await sentHandle("e2e_shared_detached_throw", suite.app.send(shared.sharedDetachedThrow, {}));
    const throwerPid = await succeeded(thrower, RESULT_WAIT_MS);
    const neighbourPid = Number((await succeeded(neighbour, RESULT_WAIT_MS)).split(":")[0]);
    const later = await succeeded(await sentHandle("a later task", suite.app.send(shared.sharedDetachedThrow, {})), RESULT_WAIT_MS);

    assert.equal(neighbourPid, throwerPid, "the neighbour ran in the same child");
    assert.equal(later, throwerPid, "the child lived");
    assert.ok(worker.logCount("a callback threw") > 0, worker.report());
  });

  e2eTest("a Promise that no shared async body awaits rejects: it is logged and the child lives", "default", async (suite) => {
    const worker = await suite.worker("shared-only-1x4");
    const first = await succeeded(await sentHandle("e2e_shared_untracked_rejection", suite.app.send(shared.sharedUntrackedRejection, {})), RESULT_WAIT_MS);
    const second = await succeeded(await sentHandle("e2e_shared_untracked_rejection", suite.app.send(shared.sharedUntrackedRejection, {})), RESULT_WAIT_MS);

    assert.equal(second, first, "the child lived");
    assert.ok(worker.logCount("a Promise that no body awaits rejected") > 0, worker.report());
  });

  e2eTest("runtime.spawn work gets its signal with DEADLINE at the deadline of a shared async run", "default", async (suite) => {
    await suite.worker("shared-only-1x4");

    const token = randomUUID();
    const error = await failed(await sentHandle("e2e_shared_spawn_at_deadline", suite.app.send(shared.sharedSpawnAtDeadline, { token })), RESULT_WAIT_MS);

    assert.equal(errorCode(error).code, "TASK_TIMEOUT");
    assert.equal(dataMembers(error)["cancel_end"], "RETURNED_AFTER_SIGNAL");
    assert.deepEqual(suite.markers.read(`${token}-work`), { kind: "found", text: "DEADLINE\n" });
  });

  // Python `test_async_tasks.py::test_an_async_task_completes_only_after_the_work_it_left_ended`.
  e2eTest("an isolated task completes only after the runtime.spawn work it left ended", "default", async (suite) => {
    await suite.worker("isolated-1");

    const token = randomUUID();
    const handle = await sentHandle("e2e_isolated_left_work", suite.app.send(spawned.isolatedLeftWork, { token, workMs: 500 }));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), "returned");

    const workEnd = suite.markers.read(`${token}-work-end`);
    const info = await infoOf(handle);

    assert.equal(workEnd.kind, "found", "the left work ended");
    assert.equal(info.completedAt.kind, "recorded");

    if (workEnd.kind === "found" && info.completedAt.kind === "recorded") {
      assert.ok(Number(workEnd.text) <= info.completedAt.value.epochMilliseconds, "the task completed before its left work ended");
    }
  });

  e2eTest("runtime.spawn work that an isolated body left gets its signal with LEFT_WORK; the task keeps its value", "default", async (suite) => {
    await suite.worker("isolated-1");

    const token = randomUUID();
    const handle = await sentHandle("e2e_isolated_left_work_signal", suite.app.send(spawned.isolatedLeftWorkSignal, { token }));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), "returned");
    assert.deepEqual(suite.markers.read(`${token}-work`), { kind: "found", text: "LEFT_WORK\n" });
  });

  e2eTest("an isolated run gets no signal at its deadline: the worker kills the child", "default", async (suite) => {
    await suite.worker("isolated-1");

    const token = randomUUID();
    const error = await failed(await sentHandle("e2e_isolated_deadline", suite.app.send(spawned.isolatedDeadline, { token })), RESULT_WAIT_MS);

    assert.equal(errorCode(error).code, "TASK_TIMEOUT");
    assert.equal("cancel_end" in dataMembers(error), false);
    assert.equal(suite.markers.read(`${token}-signal`).kind, "absent", "the signal of an isolated run fired at its deadline");
  });

  // Python `test_child_isolation.py::test_child_stops_when_the_parent_dies`.
  e2eTest("a child ends when its worker is killed", "default", async (suite) => {
    const worker = await suite.worker("isolated-1");
    const pid = await childPid(suite);

    worker.signal("SIGKILL");
    await pollUntil(ORPHAN_END_WAIT_MS, `child ${String(pid)} ends after its parent`, async () => !processAlive(pid));
  });

  // Python `test_child_isolation.py::test_sigterm_to_every_process_still_drains`.
  e2eTest("SIGTERM to the worker and to its child drains: the running task completes, the worker exits 0", "default", async (suite) => {
    const worker = await suite.worker("isolated-1");
    const token = randomUUID();
    const handle = await sentHandle("e2e_pid_then_hold", suite.app.send(basic.pidThenHold, { token, durationMs: 2_000 }));

    await pollUntil(RESULT_WAIT_MS, "the body writes its process id", async () => suite.markers.read(`${token}-pid`).kind === "found");

    const marker = suite.markers.read(`${token}-pid`);

    assert.equal(marker.kind, "found");

    if (marker.kind === "found") {
      process.kill(Number(marker.text), "SIGTERM");
    }

    const end = await worker.stop();

    assert.deepEqual(end, { kind: "exitedAfterStop", exit: { kind: "exitedWithCode", code: 0 } }, worker.report());
    assert.equal(await succeeded(handle, RESULT_WAIT_MS), "ended");
  });

  e2eTest("SIGTERM drains a busy worker within its drain timeout; its running tasks complete", "default", async (suite) => {
    const worker = await suite.worker("isolated-2");

    const handles = [
      await sentHandle("e2e_slow", suite.app.send(basic.slow, { durationMs: 1_500 })),
      await sentHandle("e2e_slow", suite.app.send(basic.slow, { durationMs: 1_500 })),
    ];

    for (const handle of handles) {
      await suite.rows.waitForStatus(handle.taskId, "RUNNING", RESULT_WAIT_MS);
    }

    const stoppedAt = performance.now();
    const end = await worker.stop();
    const tookMs = performance.now() - stoppedAt;

    assert.deepEqual(end, { kind: "exitedAfterStop", exit: { kind: "exitedWithCode", code: 0 } }, worker.report());
    assert.ok(tookMs < TEST_DRAIN_TIMEOUT_MS, `the drain took ${String(Math.round(tookMs))} ms`);

    for (const handle of handles) {
      assert.equal(await succeeded(handle, RESULT_WAIT_MS), "slept_1500");
    }
  });
});
