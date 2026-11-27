// The monitoring reads and the operator cancel on rows that sends wrote, with a built addon and a database: a sent
// task read live and from history, each outcome of `cancelTask` that needs a sent task, `getExpiredTasks` after a
// deadline, a started workflow read as a run and as a node, and every monitoring read on seeded rows. No worker runs.
// The cancel of a RUNNING task and the snapshot of a worker are end-to-end cases (`tests/e2e/layers`).
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`) and `HORSIES_TEST_DATABASE_URL` names a
// throwaway PostgreSQL server. The tests run on a new empty database.

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";

import type { Horsies } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { createFreshDatabase, type FreshDatabase } from "./fresh-database.ts";
import {
  cancelPending,
  closeApp,
  nodeTaskId,
  pollUntil,
  sendPending,
  sentApp,
  sentTask,
  sentWorkflow,
  startWorkflow,
} from "./sent-support.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The seconds from the send to the `goodUntil` of the expiring task. */
const EXPIRY_AHEAD_SECONDS = 1;

/** The nanoseconds of one microsecond: the cut of a stored deadline. */
const NANOSECONDS_PER_MICROSECOND = 1_000n;

/** The longest wait for the expiring task to show in the expired list, in milliseconds. */
const EXPIRY_WAIT_MS = 15_000;

let freshDatabase: FreshDatabase | undefined;
let app: Horsies | undefined;

/** The app of the new database of this file. */
function currentApp(): Horsies {
  assert.ok(app !== undefined, "the app of this file is created before the tests");

  return app;
}

describe("the monitoring reads and the cancel on sent tasks", () => {
  before(async () => {
    freshDatabase = await createFreshDatabase();
    app = sentApp(freshDatabase.url, "keep");
  });

  after(async () => {
    if (app !== undefined) {
      await closeApp(app);
    }

    await freshDatabase?.drop();
    app = undefined;
    freshDatabase = undefined;
  });

  test("taskDetail of a sent task is found live, with no attempt", async () => {
    const taskId = await sendPending(currentApp(), 1);
    const read = await currentApp().monitoring.getTaskDetail(taskId);

    assert.ok(read.ok && read.value.kind === "found", JSON.stringify(read));
    assert.equal(read.value.taskDetail.taskRow.taskId, taskId);
    assert.equal(read.value.taskDetail.taskRow.status, "PENDING");
    assert.equal(read.value.taskDetail.taskName, sentTask.name);
    assert.equal(read.value.taskDetail.queue, "default");
    assert.equal(read.value.taskDetail.isWorkflowTask, false);
    assert.deepEqual(read.value.taskDetail.attempts, []);
  });

  test("a cancel of a PENDING task gives wasStatus PENDING, and its taskDetail is then found in history as CANCELLED", async () => {
    const taskId = await sendPending(currentApp(), 2);

    await cancelPending(currentApp(), taskId);

    const read = await currentApp().monitoring.getTaskDetail(taskId);

    assert.ok(read.ok && read.value.kind === "found", JSON.stringify(read));
    assert.equal(read.value.taskDetail.taskRow.status, "CANCELLED");
  });

  test("a second cancel is TASK_NOT_CANCELLABLE with the status CANCELLED", async () => {
    const taskId = await sendPending(currentApp(), 3);

    await cancelPending(currentApp(), taskId);

    const again = await currentApp().monitoring.cancelTask(taskId, { policy: "includeRunning" });

    assert.ok(!again.ok && again.error.code === "TASK_NOT_CANCELLABLE", JSON.stringify(again));
    assert.equal(again.error.currentStatus, "CANCELLED");
  });

  test("a cancel of the task of a workflow node is TASK_IS_WORKFLOW_TASK", async () => {
    const workflowId = await startWorkflow(currentApp(), "cancel");
    const taskId = await nodeTaskId(currentApp(), workflowId);
    const answer = await currentApp().monitoring.cancelTask(taskId, { policy: "pendingOrClaimedOnly" });

    assert.ok(!answer.ok && answer.error.code === "TASK_IS_WORKFLOW_TASK", JSON.stringify(answer));
    assert.equal(answer.error.currentStatus, "PENDING");
  });

  test("expiredTasks holds one row for a task after its goodUntil passed", async () => {
    const goodUntil = Temporal.Now.instant().add({ seconds: EXPIRY_AHEAD_SECONDS });
    const taskId = await sendPending(currentApp(), 4, { goodUntil });

    const rows = await pollUntil(EXPIRY_WAIT_MS, "the expired task", async () => {
      const read = await currentApp().monitoring.getExpiredTasks();

      assert.ok(read.ok && read.value.kind === "complete", JSON.stringify(read));

      return read.value.rows.length === 0 ? undefined : read.value.rows;
    });

    assert.equal(rows.length, 1);
    assert.equal(rows[0]?.taskId, taskId);
    assert.equal(rows[0]?.taskName, sentTask.name);
    assert.equal(rows[0]?.queueName, "default");

    const stored = rows[0]?.goodUntil;

    assert.ok(stored !== undefined, "the row has its goodUntil");
    // The send cuts the deadline to the microsecond, toward the past.
    assert.ok(goodUntil.epochNanoseconds - stored.epochNanoseconds < NANOSECONDS_PER_MICROSECOND);
    assert.ok(goodUntil.epochNanoseconds >= stored.epochNanoseconds);
  });

  test("workflowRun and workflowNode are found for a started workflow", async () => {
    const workflowId = await startWorkflow(currentApp(), "read");
    const run = await currentApp().monitoring.getWorkflowRun(workflowId);

    assert.ok(run.ok && run.value.kind === "found", JSON.stringify(run));
    assert.equal(run.value.run.run.workflowId, workflowId);
    assert.equal(run.value.run.run.workflowName, sentWorkflow.name);
    assert.equal(run.value.run.run.status, "RUNNING");
    assert.equal(run.value.run.nodes.length, 1);

    const node = await currentApp().monitoring.getWorkflowNode(workflowId, 0);

    assert.ok(node.ok && node.value.kind === "found", JSON.stringify(node));
    assert.equal(node.value.node.nodeIndex, 0);
    assert.equal(node.value.node.nodeId, "greet");
    assert.equal(node.value.node.taskName, "sent_workflow_node_probe");
  });

  test("every monitoring read decodes the rows that the sends wrote", async () => {
    const { monitoring } = currentApp();
    const taskId = await sendPending(currentApp(), 5);
    const counts = await monitoring.taskStatusCounts();

    assert.ok(counts.ok, JSON.stringify(counts));
    assert.ok((counts.value.find((entry) => entry.status === "PENDING")?.count ?? 0) >= 1);

    const facets = await monitoring.taskFacets();

    assert.ok(facets.ok, JSON.stringify(facets));
    assert.ok(facets.value.queues.some((queue) => queue.value === "default"));
    assert.ok(facets.value.taskNames.some((name) => name.value === sentTask.name));

    for (const groupBy of ["queue", "taskName", "worker"] as const) {
      const breakdown = await monitoring.taskBreakdown({ groupBy });

      assert.ok(breakdown.ok, `${groupBy}: ${JSON.stringify(breakdown)}`);
      assert.ok(breakdown.value.total.total >= 1, groupBy);
    }

    const page = await monitoring.listTasks({ limit: 200 });

    assert.ok(page.ok, JSON.stringify(page));
    assert.ok(page.value.rows.some((row) => row.taskId === taskId));
    assert.ok(page.value.total >= 1);

    const names = await monitoring.listWorkflowNames();

    assert.ok(names.ok && names.value.includes(sentWorkflow.name), JSON.stringify(names));

    const runs = await monitoring.listWorkflowRuns();

    assert.ok(runs.ok && runs.value.some((run) => run.workflowName === sentWorkflow.name), JSON.stringify(runs));
    assert.deepEqual(await monitoring.listSchedules(), { ok: true, value: [] });
  });
});
