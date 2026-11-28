// Layer 8: the health reads and the pings of the app against live workers (Rust
// `clients/rust/tests/worker/tests/layer8_health.rs`, Python `clients/python/tests/e2e/test_worker_health.py`).
//
// The snapshot reads use the `recovery` instance, whose workers write a state snapshot each second. Run it with
// `run.ts` (see that file).

import assert from "node:assert/strict";
import { describe } from "node:test";

import type { PingWorkersOptions, TaskId, WorkerPong, WorkerStateSnapshot } from "#horsies";
import { loadAddon } from "#horsies";
import * as basic from "../src/tasks/basic.ts";
import * as shared from "../src/tasks/shared.ts";
import { sentHandle } from "../support/outcome.ts";
import { found, NOT_YET, pollFor } from "../support/poll.ts";
import { sqlText } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The longest wait for the snapshots of a test. The `recovery` instance writes one each second. */
const SNAPSHOT_WAIT_MS = 20_000;

/** The window of a ping of a test. */
const PING_WINDOW_MS = 5_000;

/** A ping that ends at its first reply returns well before its window: this bound (Python 4 s of a 5 s window). */
const EARLY_RETURN_BOUND_MS = 4_000;

/** The longest wait for a task to start on a running worker. */
const START_WAIT_MS = 30_000;

/** How long the running task of the class report runs: past the reads of the test. */
const RUNNING_TASK_MS = 20_000;

/** The seats of the shared async child of the split and the shared-only profiles (`instances.ts`). */
const SHARED_SEATS = 4n;

/** The snapshots of every worker that wrote one. Fails the test when the read fails. */
async function workerStates(suite: Suite): Promise<readonly WorkerStateSnapshot[]> {
  const read = await suite.app.monitoring.listWorkerStates();

  if (!read.ok) {
    assert.fail(`list the worker states: ${read.error.code}`);
  }

  return read.value;
}

/** The first snapshot that a worker wrote, once there is one. */
async function firstSnapshot(suite: Suite): Promise<WorkerStateSnapshot> {
  return pollFor(SNAPSHOT_WAIT_MS, "a snapshot", async () => {
    const [first] = await workerStates(suite);

    return first === undefined ? NOT_YET : found(first);
  });
}

/** The pongs of a ping of every worker with `options`. Fails the test when the ping fails. */
async function pongs(suite: Suite, options: PingWorkersOptions): Promise<readonly WorkerPong[]> {
  const ping = await suite.app.pingWorkers(options);

  if (!ping.ok) {
    assert.fail(`ping the workers: ${ping.error.code}`);
  }

  return ping.value;
}

/** Move the due time of PENDING task `taskId` an hour ahead: the row stays PENDING, and no worker claims it. */
async function pendingLater(suite: Suite, taskId: TaskId): Promise<void> {
  const changed = await suite.rows.query(
    `move the due time of task ${taskId}`,
    `UPDATE horsies_tasks SET enqueued_at = now() + interval '1 hour' WHERE id = ${sqlText(taskId)} AND status = 'PENDING'`,
  );

  assert.equal(changed.tag, "UPDATE 1", `task ${taskId} is a PENDING live row`);
}

/** Whether `later` is at or after `earlier`. Pure. */
function atOrAfter(later: Temporal.Instant, earlier: Temporal.Instant): boolean {
  return Temporal.Instant.compare(later, earlier) >= 0;
}

describe("layer 8: health", () => {
  e2eTest("a database ping gives its latency", "default", async (suite) => {
    const ping = await suite.app.pingDatabase();

    assert.ok(ping.ok, `ping the database: ${ping.ok ? "" : ping.error.code}`);
    assert.ok(ping.value.latencyMs >= 0 && ping.value.latencyMs < 10_000, `latency ${String(ping.value.latencyMs)} ms`);
  });

  e2eTest("the worker list holds the latest snapshot of each worker, idle workers included", "recovery", async (suite) => {
    await suite.worker("isolated-1");
    await suite.worker("isolated-1");

    // A snapshot counts the RUNNING rows of its worker at its write, one write each second. The latest snapshot of
    // the worker that ran a readiness task can be one that it wrote while that task was RUNNING, until its next
    // write. So the wait is for a snapshot of each worker that counts no running task.
    const states = await pollFor(SNAPSHOT_WAIT_MS, "an idle snapshot of each worker", async () => {
      const read = await workerStates(suite);

      return read.length === 2 && read.every((state) => state.tasksRunning === 0) ? found(read) : NOT_YET;
    });

    assert.equal(new Set(states.map((state) => state.workerId)).size, 2, "one latest snapshot per worker");
    assert.ok(states.every((state) => state.pid > 0 && state.queues.includes("default")), JSON.stringify(states.map((state) => [state.pid, state.queues])));
    assert.ok(states.every((state) => state.profile.kind === "named" && state.profile.name === "isolated-1"), JSON.stringify(states.map((state) => state.profile)));
  });

  e2eTest("the worker that replies to a ping has a snapshot that reads by its id; an unknown id reads as not found", "recovery", async (suite) => {
    await suite.worker("isolated-1");

    const [pong] = await pongs(suite, { waitMs: PING_WINDOW_MS, minResponses: { kind: "atLeast", count: 1 } });

    assert.ok(pong !== undefined, "the worker replied to the ping");

    const listed = await pollFor(SNAPSHOT_WAIT_MS, "the snapshot of the worker that replied", async () => {
      const snapshot = (await workerStates(suite)).find((state) => state.workerId === pong.workerId);

      return snapshot === undefined ? NOT_YET : found(snapshot);
    });

    const read = await suite.app.monitoring.getWorkerState(listed.workerId);

    assert.ok(read.ok && read.value.kind === "found", `read the worker state: ${read.ok ? read.value.kind : read.error.code}`);
    assert.equal(read.value.snapshot.workerId, listed.workerId);
    assert.deepEqual(read.value.snapshot.profile, { kind: "named", name: "isolated-1" });
    assert.ok(atOrAfter(read.value.snapshot.snapshotAt, listed.snapshotAt), "the read is at least as new as the list");

    const unknown = await suite.app.monitoring.getWorkerState("does-not-exist");

    assert.deepEqual(unknown, { ok: true, value: { kind: "notFound", workerId: "does-not-exist" } });
  });

  e2eTest("the history of a worker reads newest first, and a limit keeps the newest", "recovery", async (suite) => {
    await suite.worker("isolated-1");

    const listed = await firstSnapshot(suite);

    const history = await pollFor(SNAPSHOT_WAIT_MS, "three snapshots", async () => {
      const read = await suite.app.monitoring.getWorkerStateHistory(listed.workerId, { kind: "allRetained" });

      return read.ok && read.value.length >= 3 ? found(read.value) : NOT_YET;
    });

    assert.ok(history.every((snapshot) => snapshot.profile.kind === "named" && snapshot.profile.name === "isolated-1"));

    for (const [index, snapshot] of history.slice(1).entries()) {
      const newer = history[index];

      assert.ok(newer !== undefined && Temporal.Instant.compare(newer.snapshotAt, snapshot.snapshotAt) > 0, "newest first");
    }

    const newest = await suite.app.monitoring.getWorkerStateHistory(listed.workerId, { kind: "newest", count: 2 });

    assert.ok(newest.ok, `read the newest two: ${newest.ok ? "" : newest.error.code}`);
    assert.equal(newest.value.length, 2);

    const [newestFirst] = newest.value;
    const [historyFirst] = history;

    assert.ok(newestFirst !== undefined && historyFirst !== undefined && atOrAfter(newestFirst.snapshotAt, historyFirst.snapshotAt));
  });

  e2eTest("a worker ping with a window of 0 ms is refused as INVALID_INPUT, not retryable", "default", async (suite) => {
    const refused = await suite.app.pingWorkers({ target: { kind: "everyWorker" }, waitMs: 0 });

    assert.ok(!refused.ok, "a window of 0 ms is refused");

    const error = refused.error;

    if (error.code !== "INVALID_INPUT") {
      assert.fail(`a window of 0 ms is refused with ${error.code}`);
    }

    assert.equal(error.retryable, false, "a bad argument is not retryable");
  });

  e2eTest("a ping that asks for one reply returns early; a ping collects each worker's pong; a target gets one pong", "default", async (suite) => {
    await suite.worker("isolated-1");

    const startedAt = performance.now();
    const early = await pongs(suite, { waitMs: PING_WINDOW_MS, minResponses: { kind: "atLeast", count: 1 } });
    const tookMs = performance.now() - startedAt;

    assert.equal(early.length, 1, JSON.stringify(early));
    assert.ok(tookMs < EARLY_RETURN_BOUND_MS, `the ping of one reply took ${String(Math.round(tookMs))} ms`);

    await suite.worker("isolated-1");

    const both = await pongs(suite, { waitMs: PING_WINDOW_MS, minResponses: { kind: "atLeast", count: 2 } });

    assert.equal(both.length, 2, `both workers reply: ${JSON.stringify(both)}`);
    assert.equal(new Set(both.map((pong) => pong.workerId)).size, 2);

    const [pong] = both;

    assert.ok(pong !== undefined);
    assert.ok(pong.workerId !== "" && pong.hostname !== "", JSON.stringify(pong));
    assert.ok(pong.pid > 0 && pong.roundTripMs >= 0, JSON.stringify(pong));

    const targeted = await pongs(suite, { target: { kind: "worker", workerId: pong.workerId }, waitMs: PING_WINDOW_MS });

    assert.deepEqual(
      targeted.map((reply) => reply.workerId),
      [pong.workerId],
      "only the target replies",
    );
  });

  e2eTest("the class report gives the isolated budget, the shared budget of every seat with its run, and the pending shared stream", "default", async (suite) => {
    const pending = await sentHandle("the pending shared task", suite.app.send(shared.sharedSleep, { milliseconds: 0 }));

    await pendingLater(suite, pending.taskId);
    await suite.worker("split-isolated-1-shared-1x4");

    const running = await sentHandle("the running shared task", suite.app.send(shared.sharedSleep, { milliseconds: RUNNING_TASK_MS }));

    await suite.rows.waitForStatus(running.taskId, "RUNNING", START_WAIT_MS);

    const workerId = (await suite.rows.taskRow(running.taskId)).workerId;
    const report = await suite.app.monitoring.getExecutionClassReport();

    assert.ok(report.ok, `read the class report: ${report.ok ? "" : report.error.code}`);

    const [worker, ...otherWorkers] = report.value.workers;

    assert.ok(worker !== undefined && otherWorkers.length === 0, JSON.stringify(report.value.workers.map((load) => load.workerId)));
    assert.deepEqual(
      { workerId: worker.workerId, budgets: worker.budgets },
      {
        workerId,
        budgets: [
          { budget: "ISOLATED", capacity: 1n, serves: ["ISOLATED", "SHARED_THREAD"], inFlight: 0n },
          { budget: "SHARED_ASYNC", capacity: SHARED_SEATS, serves: ["SHARED_ASYNC"], inFlight: 1n },
        ],
      },
    );
    assert.deepEqual(
      report.value.streams.map((stream) => ({ queue: stream.queue, execution: stream.execution, coverage: stream.coverage })),
      [{ queue: "default", execution: "SHARED_ASYNC", coverage: { kind: "served", workers: 1n, capacity: SHARED_SEATS } }],
    );
  });

  e2eTest("a worker with only shared children serves the shared stream of its queue, and no ISOLATED row", "default", async (suite) => {
    const unserved = await sentHandle("the isolated task", suite.app.send(basic.healthcheck, {}));
    const pending = await sentHandle("the pending shared task", suite.app.send(shared.sharedSleep, { milliseconds: 0 }));

    await pendingLater(suite, pending.taskId);
    await suite.worker("shared-only-1x4");

    const coverage = await suite.app.monitoring.getExecutionClassCoverage();

    assert.ok(coverage.ok, `read the class coverage: ${coverage.ok ? "" : coverage.error.code}`);
    assert.equal(await suite.rows.taskStatus(unserved.taskId), "PENDING");
    assert.deepEqual(coverage.value.streams, [
      { queue: "default", execution: "ISOLATED", coverage: { kind: "unserved" } },
      { queue: "default", execution: "SHARED_ASYNC", coverage: { kind: "served", workers: 1n, capacity: SHARED_SEATS } },
    ]);
  });
});
