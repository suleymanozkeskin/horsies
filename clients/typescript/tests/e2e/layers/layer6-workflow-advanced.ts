// Layer 6: joins, pause and resume, success policies, recovery and retries of workflows (Rust
// `clients/rust/tests/worker/tests/layer6_workflow_advanced.rs`, Python
// `clients/python/tests/e2e/test_layer6_workflow_advanced.py`).
//
// The recovery tests run on the `recovery` instance: its worker's reaper runs the workflow recovery each second, so a
// workflow that a test puts back to RUNNING is finalized again by that worker (Python runs one recovery pass in the
// test process). The cancel of a running child workflow, which the Rust file holds, is in layer 7 with the other
// cancels of sub-workflows.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe } from "node:test";

import type { WorkflowId } from "#horsies";
import { loadAddon } from "#horsies";
import { errorCode } from "../support/outcome.ts";
import { SERIAL_QUEUE } from "../src/instances.ts";
import { recoveryCrash } from "../src/tasks/recovery-queues.ts";
import * as advanced from "../src/workflows/advanced.ts";
import * as structure from "../src/workflows/structure.ts";
import type { TestProcess } from "../support/process.ts";
import { neverDuring, pollUntil, sqlText } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";
import { handleCall, startedHandle, WORKFLOW_WAIT_MS, workflowCode, workflowValue } from "../support/workflow-reads.ts";
import {
  changeWorkflowRows,
  endedNodes,
  nodeById,
  startedAfterEnd,
  statusesOf,
  taskOfNode,
  waitForNode,
  waitForWorkflowStatus,
  workflowNodes,
  workflowRow,
  workflowStatus,
} from "../support/workflow-rows.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The longest wait for a workflow whose node retries three times, 1 s, 2 s and 4 s apart. */
const RETRY_WAIT_MS = 60_000;

/** The longest wait for the reaper of a worker to finalize a workflow again: one recovery pass each second. */
const RECOVERY_WAIT_MS = 30_000;

/** The longest wait for the end of a workflow whose nodes ran when their worker was killed. */
const CRASH_RECOVERY_WAIT_MS = 60_000;

/** How long a paused workflow must stay paused. */
const PAUSED_WINDOW_MS = 1_000;

/** The longest wait for the exit of a worker whose group was killed. */
const KILLED_EXIT_WAIT_MS = 5_000;

/** The wall bound of the tests that wait for a crash recovery or for retries. */
const LONG_TEST_LIMIT_MS = 180_000;

/** Wait until node a of a `pausable` workflow completed while node d is PENDING: the workflow runs and has work left. */
async function waitForPausePoint(suite: Suite, workflowId: WorkflowId): Promise<void> {
  await pollUntil(WORKFLOW_WAIT_MS, `workflow ${workflowId} completes node a with node d PENDING`, async () => {
    const nodes = await workflowNodes(suite.rows, workflowId);

    return nodes.length === 4 && nodeById(nodes, "a").status === "COMPLETED" && nodeById(nodes, "d").status === "PENDING";
  });
}

/** Put the ended workflow `workflowId` back to RUNNING with its nodes as they are: a finalize that never committed. */
async function unfinalize(suite: Suite, workflowId: WorkflowId): Promise<void> {
  await changeWorkflowRows(
    suite.rows,
    `put workflow ${workflowId} back to RUNNING`,
    `UPDATE horsies_workflows SET status = 'RUNNING', completed_at = NULL WHERE id = ${sqlText(workflowId)}`,
    1,
  );
}

/** Kill the group of `worker` (the crash of a worker with its children) and wait for its exit. */
async function crash(worker: TestProcess): Promise<void> {
  worker.killGroup();
  await worker.waitExit(KILLED_EXIT_WAIT_MS);
}

describe("layer 6: workflow advanced", () => {
  e2eTest("under a quorum join, a node waits for its context source after the quorum was met, and reads it", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("quorum_context_gate", suite.app.start(advanced.quorumContextGate, {}));

    assert.equal(await workflowValue(handle), "completed_c");

    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED"]);
    startedAfterEnd(nodes, "d", ["c"]);
  });

  e2eTest("a quorum that the failed dependencies make impossible skips its node", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("quorum_impossible", suite.app.start(advanced.quorumImpossible, {}));

    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS)), [
      "FAILED",
      "FAILED",
      "COMPLETED",
      "SKIPPED",
    ]);
  });

  e2eTest("an any join whose dependencies all failed skips its node", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("any_join_all_fail", suite.app.start(advanced.anyJoinAllFail, {}));

    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS)), [
      "FAILED",
      "FAILED",
      "SKIPPED",
    ]);
  });

  e2eTest("a paused workflow stays PAUSED and starts no waiting node", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("pausable", suite.app.start(advanced.pausable, {}));

    await waitForPausePoint(suite, handle.workflowId);
    assert.equal(await handleCall("pause", handle.pause()), true);
    assert.equal(await workflowStatus(suite.rows, handle.workflowId), "PAUSED");
    await neverDuring(PAUSED_WINDOW_MS, "the paused workflow left PAUSED or started node d", async () => {
      const nodes = await workflowNodes(suite.rows, handle.workflowId);

      return (await workflowStatus(suite.rows, handle.workflowId)) !== "PAUSED" || nodeById(nodes, "d").status !== "PENDING";
    });
  });

  e2eTest("a resume of a paused workflow runs it to completion", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("pausable", suite.app.start(advanced.pausable, {}));

    await waitForPausePoint(suite, handle.workflowId);
    assert.equal(await handleCall("pause", handle.pause()), true);
    assert.equal(await workflowStatus(suite.rows, handle.workflowId), "PAUSED");
    assert.equal(await handleCall("resume", handle.resume()), true);
    assert.ok(["RUNNING", "COMPLETED"].includes(await workflowStatus(suite.rows, handle.workflowId)), "the workflow left PAUSED");
    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS)), [
      "COMPLETED",
      "COMPLETED",
      "COMPLETED",
      "COMPLETED",
    ]);
  });

  e2eTest("a cancel of a paused workflow skips the node that waited; get gives WORKFLOW_CANCELLED", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("pausable", suite.app.start(advanced.pausable, {}));

    await waitForPausePoint(suite, handle.workflowId);
    assert.equal(await handleCall("pause", handle.pause()), true);
    await handleCall("cancel", handle.cancel());

    const nodes = await endedNodes(suite.rows, handle.workflowId, "CANCELLED", WORKFLOW_WAIT_MS);

    assert.equal(nodeById(nodes, "d").status, "SKIPPED");
    assert.equal(await workflowCode(handle), "WORKFLOW_CANCELLED");
  });

  e2eTest("a second pause of a PAUSED workflow returns false", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("pausable", suite.app.start(advanced.pausable, {}));

    await waitForPausePoint(suite, handle.workflowId);
    assert.equal(await handleCall("pause", handle.pause()), true);
    assert.equal(await handleCall("pause again", handle.pause()), false);
  });

  e2eTest("a resume of a RUNNING workflow returns false", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("pausable", suite.app.start(advanced.pausable, {}));

    await waitForWorkflowStatus(suite.rows, handle.workflowId, "RUNNING", WORKFLOW_WAIT_MS);
    assert.equal(await handleCall("resume", handle.resume()), false);
  });

  e2eTest("under onError pause, a failed node pauses the workflow before the node after it; get gives WORKFLOW_PAUSED", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("pause_on_error", suite.app.start(advanced.pauseOnError, {}));

    await waitForWorkflowStatus(suite.rows, handle.workflowId, "PAUSED", WORKFLOW_WAIT_MS);
    assert.deepEqual(statusesOf(await workflowNodes(suite.rows, handle.workflowId)), ["COMPLETED", "FAILED", "PENDING"]);
    assert.equal(await workflowCode(handle), "WORKFLOW_PAUSED");
  });

  e2eTest("a resume after the pause of onError skips the failed node's dependent and the workflow fails", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("pause_on_error", suite.app.start(advanced.pauseOnError, {}));

    await waitForWorkflowStatus(suite.rows, handle.workflowId, "PAUSED", WORKFLOW_WAIT_MS);
    assert.equal(await handleCall("resume", handle.resume()), true);
    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS)), [
      "COMPLETED",
      "FAILED",
      "SKIPPED",
    ]);
  });

  e2eTest("a policy case whose required node completed completes the workflow; an optional node may fail", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("policy_satisfied", suite.app.start(advanced.policySatisfied, {}));

    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS)), ["COMPLETED", "FAILED"]);
    assert.equal(await workflowValue(handle), "required_A");
  });

  e2eTest("a policy whose only case has a failed required node fails the workflow with that node's error", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("policy_not_met", suite.app.start(advanced.policyNotMet, {}));

    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS)), ["COMPLETED", "FAILED"]);
    assert.equal(await workflowCode(handle), "REQUIRED_FAIL");
  });

  e2eTest("a policy completes the workflow when any one of its cases is met", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("policy_cases", suite.app.start(advanced.policyCases, {}));

    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS)), ["FAILED", "COMPLETED"]);
    assert.equal(await workflowValue(handle), "case2_ok");
  });

  e2eTest("the reaper finalizes a COMPLETED workflow that was put back to RUNNING again, with its output", "recovery", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("linear", suite.app.start(structure.linear, {}));

    await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);
    assert.equal(await workflowValue(handle), "completed_c");
    await unfinalize(suite, handle.workflowId);
    await waitForWorkflowStatus(suite.rows, handle.workflowId, "COMPLETED", RECOVERY_WAIT_MS);
    assert.equal(await workflowValue(handle), "completed_c");
  });

  e2eTest("the reaper finalizes a FAILED workflow that was put back to RUNNING again, with its error", "recovery", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("linear_fail_mid", suite.app.start(structure.linearFailMid, {}));

    await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);
    await unfinalize(suite, handle.workflowId);
    assert.equal(await workflowStatus(suite.rows, handle.workflowId), "RUNNING");
    await waitForWorkflowStatus(suite.rows, handle.workflowId, "FAILED", RECOVERY_WAIT_MS);
    assert.equal(await workflowCode(handle), "DELIBERATE_FAILURE");
  });

  e2eTest("the finalize of the reaper stores the error of the first failed node by index, not the stored one", "recovery", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("two_failures", suite.app.start(advanced.twoFailures, {}));
    const id = sqlText(handle.workflowId);

    await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);
    await changeWorkflowRows(
      suite.rows,
      `store the later error on workflow ${handle.workflowId}`,
      `UPDATE horsies_workflows SET status = 'RUNNING', completed_at = NULL, result = NULL,
       error = (SELECT (result::jsonb -> 'err')::text FROM horsies_workflow_tasks WHERE workflow_id = ${id} AND task_index = 1)
       WHERE id = ${id}`,
      1,
    );
    assert.ok((await workflowRow(suite.rows, handle.workflowId)).error?.includes("SECOND_ERROR"), "the stored error is the later one");
    await waitForWorkflowStatus(suite.rows, handle.workflowId, "FAILED", RECOVERY_WAIT_MS);
    assert.equal(await workflowCode(handle), "FIRST_ERROR");
    assert.ok(!(await workflowRow(suite.rows, handle.workflowId)).error?.includes("SECOND_ERROR"), "the later error was replaced");
  });

  e2eTest(
    "after a worker crash, the next worker fails the running nodes with WORKER_CRASHED and runs the nodes after them on their queue",
    "recovery_queues",
    async (suite) => {
      const crashed = await suite.worker("isolated-2");
      const handle = await startedHandle("recovery_crash", suite.app.start(recoveryCrash, {}));

      await waitForNode(suite.rows, handle.workflowId, "b", WORKFLOW_WAIT_MS, "COMPLETED", (node) => node.status === "COMPLETED");

      for (const nodeId of ["c", "d"]) {
        await suite.rows.waitForStatus(await taskOfNode(suite.rows, handle.workflowId, nodeId, WORKFLOW_WAIT_MS), "RUNNING", WORKFLOW_WAIT_MS);
      }

      await crash(crashed);
      await suite.worker("isolated-2");

      const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", CRASH_RECOVERY_WAIT_MS);

      assert.deepEqual(statusesOf(nodes), ["COMPLETED", "COMPLETED", "FAILED", "FAILED", "COMPLETED", "COMPLETED"]);

      const results = await handleCall("the node results", handle.results());

      for (const nodeId of ["c", "d"]) {
        const result = results.get(nodeId);

        assert.ok(result?.kind === "outcome" && result.outcome.kind === "failed", `node ${nodeId}: ${JSON.stringify(result)}`);
        assert.equal(errorCode(result.outcome.error).code, "WORKER_CRASHED", `node ${nodeId}`);
      }

      for (const nodeId of ["e", "f"]) {
        const task = await suite.rows.taskRow(await taskOfNode(suite.rows, handle.workflowId, nodeId, WORKFLOW_WAIT_MS));

        assert.equal(task.queueName, SERIAL_QUEUE.name, `node ${nodeId}`);
      }
    },
    LONG_TEST_LIMIT_MS,
  );

  e2eTest(
    "a node retries with the policy of its task and completes on its second attempt",
    "default",
    async (suite) => {
      await suite.worker("isolated-1");

      const token = randomUUID();
      const handle = await startedHandle("retrying_node", suite.app.start(advanced.retryingNode, { token, succeedOnAttempt: 2 }));

      assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "COMPLETED", RETRY_WAIT_MS)), ["COMPLETED"]);
      assert.equal(await workflowValue(handle), "succeeded_on_attempt_2");
      assert.equal(suite.markers.count(`${token}-attempt-`), 2);
    },
    LONG_TEST_LIMIT_MS,
  );

  e2eTest(
    "a node that exhausts its retries, one attempt and three retries, fails the workflow with its code",
    "default",
    async (suite) => {
      await suite.worker("isolated-1");

      const token = randomUUID();
      const handle = await startedHandle("retrying_node", suite.app.start(advanced.retryingNode, { token, succeedOnAttempt: 10 }));

      assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "FAILED", RETRY_WAIT_MS)), ["FAILED"]);
      assert.equal(await workflowCode(handle), "RETRY_NEEDED");
      assert.equal(suite.markers.count(`${token}-attempt-`), 4);
    },
    LONG_TEST_LIMIT_MS,
  );

  e2eTest("a sub-workflow node gives its child a static param and the result of a dependency", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("subworkflow_mixed_input", suite.app.start(advanced.subworkflowMixedInput, {}));

    assert.equal(await workflowValue(handle), `count=${String(advanced.MIXED_INPUT_VALUE)}`);
  });
});
