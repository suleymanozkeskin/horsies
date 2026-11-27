// Layer 2: several workers, caps, crash recovery and soft-cap leases (Rust
// `clients/rust/tests/worker/tests/layer2_cluster.rs`, Python `clients/python/tests/e2e/test_layer2_cluster.py`).

import assert from "node:assert/strict";
import { describe } from "node:test";

import type { TaskHandle, TaskId } from "#horsies";
import { loadAddon } from "#horsies";
import { CLUSTER_WIDE_CAP, CUSTOM_QUEUES, type Profile } from "../src/instances.ts";
import * as basic from "../src/tasks/basic.ts";
import * as custom from "../src/tasks/custom.ts";
import * as retry from "../src/tasks/retry.ts";
import { failedCode, infoOf, sentHandle, succeeded } from "../support/outcome.ts";
import { pause, type TestProcess } from "../support/process.ts";
import { maxDuring, pollUntil, setTime, sqlText } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 30_000;

/** The worker id that no worker has: the owner of a claim whose worker died. */
const DEAD_WORKER_ID = "dead_worker_00000000";

/** The longest wait for the exit of a worker whose group was killed. */
const KILLED_EXIT_WAIT_MS = 5_000;

/** The cap of the custom queue `name`. */
function queueCap(name: string): number {
  const queue = CUSTOM_QUEUES.find((entry) => entry.name === name);

  assert.ok(queue !== undefined, `no custom queue ${name}`);

  return queue.cap;
}

/** The largest number of RUNNING tasks of `queue` during `durationMs`. */
async function mostRunning(suite: Suite, queue: string | "anyQueue", durationMs: number): Promise<number> {
  return maxDuring(durationMs, 50, async () => suite.rows.liveCount("RUNNING", queue));
}

/** Make `taskId` a claim of a dead worker, claimed 10 s ago, whose lease ends at `leaseEnd` (an SQL interval from now). */
async function claimForDeadWorker(suite: Suite, taskId: TaskId, leaseEnd: "5 minutes" | "-5 seconds"): Promise<void> {
  const changed = await suite.rows.query(
    `make task ${taskId} a dead claim`,
    `UPDATE horsies_tasks SET status = 'CLAIMED', attempt_claims = 1, first_claimed_at = NOW() - INTERVAL '10 seconds',
     claimed_at = NOW() - INTERVAL '10 seconds', claimed_by_worker_id = ${sqlText(DEAD_WORKER_ID)},
     claim_expires_at = NOW() + INTERVAL '${leaseEnd}' WHERE id = ${sqlText(taskId)}`,
  );

  assert.equal(changed.tag, "UPDATE 1", `task ${taskId} is a PENDING live row`);
}

/** Start `count` workers of `profile`, each ready. */
async function workers(suite: Suite, count: number, profile: Profile): Promise<readonly TestProcess[]> {
  const started = [];

  for (let index = 0; index < count; index += 1) {
    started.push(await suite.worker(profile));
  }

  return started;
}

/** The handles of `count` sends of `e2e_slow` of `durationMs`. */
async function slowTasks(suite: Suite, count: number, durationMs: number): Promise<readonly TaskHandle<string, string>[]> {
  const handles = [];

  for (let index = 0; index < count; index += 1) {
    handles.push(await sentHandle("e2e_slow", suite.app.send(basic.slow, { durationMs })));
  }

  return handles;
}

async function allSucceed(handles: readonly TaskHandle<string, string>[], limitMs: number): Promise<void> {
  for (const handle of handles) {
    await succeeded(handle, limitMs);
  }
}

/** Kill the group of `worker` (the crash of a worker with its children) and wait for its exit. */
async function crash(worker: TestProcess): Promise<void> {
  worker.killGroup();
  await worker.waitExit(KILLED_EXIT_WAIT_MS);
}

describe("layer 2: cluster", () => {
  e2eTest("two workers share ten tasks", "default", async (suite) => {
    await workers(suite, 2, "isolated-1");

    const handles = await slowTasks(suite, 10, 500);

    await allSucceed(handles, RESULT_WAIT_MS);

    const owners = new Set<string>();

    for (const handle of handles) {
      const row = await suite.rows.taskRow(handle.taskId);

      assert.equal(row.status, "COMPLETED");
      assert.ok(row.workerId !== null, "the last owner is recorded");
      owners.add(row.workerId);
    }

    assert.ok(owners.size >= 2, `one worker ran every task: ${[...owners].join(", ")}`);
  });

  e2eTest("two workers run each of 20 tasks once", "default", async (suite) => {
    await workers(suite, 2, "isolated-2");
    await idempotentRuns(suite, "multiworker_task_", 20, RESULT_WAIT_MS);
  });

  e2eTest("a RUNNING task of a crashed worker fails with WORKER_CRASHED", "recovery", async (suite) => {
    const crashed = await suite.worker("isolated-1");
    const handle = await sentHandle("e2e_slow", suite.app.send(basic.slow, { durationMs: 60_000 }));

    await suite.rows.waitForStatus(handle.taskId, "RUNNING", RESULT_WAIT_MS);
    await crash(crashed);
    await suite.worker("isolated-1");
    assert.equal(await failedCode(handle, RESULT_WAIT_MS), "WORKER_CRASHED");
  });

  e2eTest("failed tasks give their cluster cap back", "cluster_cap", async (suite) => {
    await workers(suite, 2, "isolated-2");

    const failing = [];

    for (let index = 0; index < 4; index += 1) {
      failing.push(await sentHandle("a failing task", suite.app.send(basic.noRetry, {})));
    }

    for (const handle of failing) {
      assert.equal(await failedCode(handle, RESULT_WAIT_MS), "PERMANENT");
    }

    await allSucceed(await slowTasks(suite, 4, 300), RESULT_WAIT_MS);
  });

  e2eTest("retries run across two workers", "default", async (suite) => {
    await workers(suite, 2, "isolated-2");

    const handles = [];

    for (let index = 0; index < 10; index += 1) {
      handles.push(await sentHandle("e2e_retry_exhausted", suite.app.send(retry.retryExhausted, {})));
    }

    for (const handle of handles) {
      assert.equal(await failedCode(handle, 60_000), "TRANSIENT");

      const info = await infoOf(handle);

      assert.deepEqual([info.status, info.retryCount], ["FAILED", 3]);
    }
  });

  e2eTest("sends while a worker runs earlier tasks all complete", "default", async (suite) => {
    await suite.worker("isolated-2");

    const first = await slowTasks(suite, 10, 200);
    const [head] = first;

    assert.ok(head !== undefined);
    await suite.rows.waitForStatus(head.taskId, "COMPLETED", RESULT_WAIT_MS);

    const second = await slowTasks(suite, 10, 200);

    await allSucceed([...first, ...second], RESULT_WAIT_MS);
  });

  e2eTest("the cluster-wide cap bounds the RUNNING tasks of two workers", "cluster_cap", async (suite) => {
    await workers(suite, 2, "isolated-2");

    const handles = await slowTasks(suite, 6, 500);
    const running = await mostRunning(suite, "anyQueue", 4_000);

    await allSucceed(handles, RESULT_WAIT_MS);
    assert.ok(running > 0, "no RUNNING task was seen: the check is vacuous");
    assert.ok(running <= CLUSTER_WIDE_CAP, `${String(running)} tasks ran at once against a cap of ${String(CLUSTER_WIDE_CAP)}`);
  });

  e2eTest("a stale claim of a dead worker is requeued and completed", "recovery", async (suite) => {
    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 5 }));

    await claimForDeadWorker(suite, handle.taskId, "5 minutes");
    await suite.worker("isolated-1");
    assert.equal(await succeeded(handle, RESULT_WAIT_MS), 10);
    assert.notEqual((await suite.rows.taskRow(handle.taskId)).workerId, DEAD_WORKER_ID);
  });

  e2eTest("the crash of one of three workers leaves the others running the tasks", "default", async (suite) => {
    const [killed] = await workers(suite, 3, "isolated-2");
    const handles = await slowTasks(suite, 20, 300);
    const [head] = handles;

    assert.ok(killed !== undefined && head !== undefined);
    await suite.rows.waitForStatus(head.taskId, "RUNNING", RESULT_WAIT_MS);
    await crash(killed);
    await pause(8_000);

    const ids = handles.map((handle) => sqlText(handle.taskId)).join(", ");

    const completed = await suite.rows.count(
      "count the completed tasks",
      `SELECT count(*) FROM horsies_task_history WHERE task_id::text IN (${ids}) AND status = 'COMPLETED'`,
    );

    // The killed worker held at most its two running tasks.
    assert.ok(completed >= 18, `${String(completed)} of 20 tasks completed`);

    const owners = await suite.rows.count(
      "count the owners",
      `SELECT count(DISTINCT last_claimed_worker_id) FROM horsies_task_history WHERE task_id::text IN (${ids}) AND status = 'COMPLETED'`,
    );

    assert.ok(owners >= 2, `the two live workers ran the tasks: ${String(owners)} owners`);
  });

  e2eTest("the cap of a queue bounds its RUNNING tasks", "custom_queues", async (suite) => {
    await suite.worker("isolated-8");

    const handles = [];

    for (let index = 0; index < 10; index += 1) {
      handles.push(await sentHandle("e2e_slow_high", suite.app.send(custom.slowHigh, { durationMs: 400 })));
    }

    const running = await mostRunning(suite, "high", 5_000);

    await allSucceed(handles, RESULT_WAIT_MS);
    assert.ok(running > 0, "no RUNNING task was seen: the check is vacuous");
    assert.ok(running <= queueCap("high"), `${String(running)} tasks of queue high ran at once`);
  });

  e2eTest("the caps of two queues hold each on its own", "custom_queues", async (suite) => {
    await suite.worker("isolated-8");

    const handles = [];

    for (let index = 0; index < 10; index += 1) {
      handles.push(await sentHandle("e2e_slow_high", suite.app.send(custom.slowHigh, { durationMs: 400 })));
      handles.push(await sentHandle("e2e_slow_normal", suite.app.send(custom.slowNormal, { durationMs: 400 })));
    }

    const [mostHigh, mostNormal] = await Promise.all([mostRunning(suite, "high", 5_000), mostRunning(suite, "normal", 5_000)]);

    await allSucceed(handles, RESULT_WAIT_MS);
    assert.ok(mostHigh > 0 && mostNormal > 0, "both queues ran");
    assert.ok(mostHigh <= queueCap("high"), `high: ${String(mostHigh)} at once`);
    assert.ok(mostNormal <= queueCap("normal"), `normal: ${String(mostNormal)} at once`);
  });

  e2eTest("the tasks of a queue with a higher priority end first", "custom_queues", async (suite) => {
    const ids: TaskId[] = [];

    for (let index = 0; index < 5; index += 1) {
      ids.push((await sentHandle("e2e_slow_low", suite.app.send(custom.slowLow, { durationMs: 100 }))).taskId);
    }

    for (let index = 0; index < 5; index += 1) {
      ids.push((await sentHandle("e2e_slow_high", suite.app.send(custom.slowHigh, { durationMs: 100 }))).taskId);
    }

    await suite.worker("isolated-1");
    await suite.rows.waitForTerminal(ids, RESULT_WAIT_MS);

    const ended = [];

    for (const id of ids) {
      const row = await suite.rows.taskRow(id);

      ended.push({ at: setTime(row.terminalAt, `the end of task ${id}`), queue: row.queueName });
    }

    ended.sort((left, right) => (left.at < right.at ? -1 : left.at > right.at ? 1 : 0));

    const highFirst = ended.slice(0, 5).filter((end) => end.queue === "high").length;

    assert.ok(highFirst >= 4, `completion order: ${ended.map((end) => end.queue).join(", ")}`);
  });

  e2eTest("soft-cap leases run each task once on two workers", "soft_cap", async (suite) => {
    await workers(suite, 2, "isolated-1");
    await slowIdempotentRuns(suite, "softcap_task_", 6, 1_500);
  });

  e2eTest("an expired soft-cap claim of a dead worker is requeued and run once", "soft_cap", async (suite) => {
    const token = "softcap_expired_claim";
    const handle = await sentHandle("e2e_slow_idempotent", suite.app.send(basic.slowIdempotent, { token, durationMs: 100 }));

    await claimForDeadWorker(suite, handle.taskId, "-5 seconds");
    await suite.worker("isolated-1");
    assert.equal(await succeeded(handle, RESULT_WAIT_MS), `executed:${token}`);

    const row = await suite.rows.taskRow(handle.taskId);

    assert.ok(row.workerId !== null, "the final owner is recorded");
    assert.notEqual(row.workerId, DEAD_WORKER_ID);
    assert.equal(suite.markers.count("idempotent-"), 1);
  });

  e2eTest("20 soft-cap tasks on two workers each run once", "soft_cap", async (suite) => {
    await workers(suite, 2, "isolated-1");
    await slowIdempotentRuns(suite, "ledger_", 20, 50);
  });

  e2eTest("a prefetched claim of a crashed worker moves to another worker and runs once", "soft_cap", async (suite) => {
    const first = await suite.worker("isolated-1");
    const blocker = await sentHandle("the blocker", suite.app.send(basic.slow, { durationMs: 6_000 }));

    await suite.rows.waitForStatus(blocker.taskId, "RUNNING", RESULT_WAIT_MS);

    const token = "softcap_owner_transition";
    const handle = await sentHandle("e2e_slow_idempotent", suite.app.send(basic.slowIdempotent, { token, durationMs: 100 }));
    let firstOwner = "";

    await pollUntil(RESULT_WAIT_MS, "the prefetched claim", async () => {
      const row = await suite.rows.readTask(handle.taskId);

      firstOwner = row !== "absent" && row.status === "CLAIMED" && row.workerId !== null ? row.workerId : "";

      return firstOwner !== "";
    });
    await pause(2_500);
    await crash(first);
    await suite.worker("isolated-1");
    assert.equal(await succeeded(handle, RESULT_WAIT_MS), `executed:${token}`);
    assert.notEqual((await suite.rows.taskRow(handle.taskId)).workerId, firstOwner);
    assert.equal(suite.markers.count("idempotent-"), 1);
  });
});

/** Send `count` runs of `e2e_idempotent` with the tokens `<prefix><n>`; each body runs once. */
async function idempotentRuns(suite: Suite, prefix: string, count: number, limitMs: number): Promise<void> {
  const handles = [];

  for (let index = 0; index < count; index += 1) {
    handles.push(await sentHandle("e2e_idempotent", suite.app.send(basic.idempotent, { token: `${prefix}${String(index)}` })));
  }

  for (const handle of handles) {
    await succeeded(handle, limitMs);
  }

  assert.equal(suite.markers.count(`idempotent-${prefix}`), count);
}

/** Send `count` runs of `e2e_slow_idempotent` of `durationMs` with the tokens `<prefix><n>`; each body runs once. */
async function slowIdempotentRuns(suite: Suite, prefix: string, count: number, durationMs: number): Promise<void> {
  const handles = [];

  for (let index = 0; index < count; index += 1) {
    handles.push(
      await sentHandle("e2e_slow_idempotent", suite.app.send(basic.slowIdempotent, { token: `${prefix}${String(index)}`, durationMs })),
    );
  }

  for (const handle of handles) {
    await succeeded(handle, 60_000);
  }

  assert.equal(suite.markers.count(`idempotent-${prefix}`), count);
}
