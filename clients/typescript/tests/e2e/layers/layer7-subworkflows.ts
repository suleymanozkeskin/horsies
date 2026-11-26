// Layer 7: sub-workflows on every read surface (Rust `clients/rust/tests/worker/tests/layer7_subworkflows.rs`, Python
// `clients/python/tests/e2e/test_layer7_subworkflows.py` and `test_layer7_subworkflow_priority.py`).
//
// The parents are the workflows of `src/workflows/subworkflows.ts`, their children those of `src/workflows/children.ts`,
// which every process of the instance lists: the start builds a child of a root node in this process, and the worker
// builds every other child when its node is ready. The recovery tests run on the `recovery` instance, whose worker's
// reaper runs the workflow recovery each second (Python runs one recovery pass in the test process); each test reaches
// its stuck state through the client and a change of the workflow rows, never a forged task row.
//
// A workflow has an output node in the TypeScript client: each Python case whose parent has none runs here with an
// output node and reads the other nodes through `results` and `resultFor`. The reads of the results of the terminal
// nodes wait for #2459.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe } from "node:test";

import { type JsonValue, type NodeResult, TaskId, type TaskOutcome } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { FAST_RECOVERY, SERIAL_QUEUE } from "../src/instances.ts";
import { serialBlocker, serialChildTask, serialDirectTask, serialParent } from "../src/tasks/recovery-queues.ts";
import { CHILD_NODE, SUB_OK_VALUE, subFail, subNestedFailMiddle, tenantReport } from "../src/workflows/children.ts";
import * as parents from "../src/workflows/subworkflows.ts";
import { errorText, sentHandle, succeeded } from "../support/outcome.ts";
import { pollUntil, setTime, sqlText } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";
import {
  failedChildOf,
  handleCall,
  nodeOutcome,
  nodeValue,
  startedHandle,
  subWorkflowFailure,
  WORKFLOW_WAIT_MS,
  workflowCode,
  workflowError,
  workflowValue,
} from "../support/workflow-reads.ts";
import {
  changeWorkflowRows,
  childIdOf,
  childWorkflowOf,
  endedNodes,
  nodeById,
  statusesOf,
  waitForNode,
  waitForWorkflowStatus,
  workflowNodes,
  workflowRow,
  workflowStatus,
  workflowTree,
} from "../support/workflow-rows.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest wait for the reaper of a worker to finalize a workflow again: one recovery pass each second. */
const RECOVERY_WAIT_MS = 30_000;

/** The longest wait for a workflow whose child's node retries three times, 1 s, 2 s and 4 s apart. */
const RETRY_WAIT_MS = 75_000;

/** The time on top of the recovery bounds for one reaper step to show: the pass, the write, and the polls of the wait. */
const REAPER_PASS_MARGIN_MS = 10_000;

/** How far in the past the deadline of an expired child is (Python `expires_in_ms=-100`). */
const EXPIRED_BY_MS = 100;

/** How long the blocker holds the one slot of the serial queue: the child's task and the direct task wait behind it. */
const BLOCKER_MS = 6_000;

/** The wall bound of the tests that wait for retries or for the reaper. */
const LONG_TEST_LIMIT_MS = 180_000;

/** The outcome that the results of a workflow hold for node `nodeId`; fails the test when it holds none that reads. */
function resultOutcome(results: ReadonlyMap<string, NodeResult>, nodeId: string): TaskOutcome<JsonValue, string> {
  const result = results.get(nodeId);

  assert.ok(result !== undefined, `the results hold no node ${nodeId}: ${JSON.stringify([...results.keys()])}`);

  switch (result.kind) {
    case "outcome":
      return result.outcome;
    case "unreadable":
      assert.fail(`the result of node ${nodeId} does not read: ${result.cause}`);
  }
}

/** Put the ended workflow `workflowId` and its node at `taskIndex` back to RUNNING, with no result: a step that never committed. */
async function unfinalizeWithNode(suite: Suite, workflowId: string, taskIndex: number): Promise<void> {
  const id = sqlText(workflowId);

  await changeWorkflowRows(
    suite.rows,
    `put workflow ${workflowId} back to RUNNING`,
    `UPDATE horsies_workflows SET status = 'RUNNING', result = NULL, error = NULL, completed_at = NULL, updated_at = NOW()
     WHERE id = ${id}`,
    1,
  );
  await changeWorkflowRows(
    suite.rows,
    `put node ${String(taskIndex)} of workflow ${workflowId} back to RUNNING`,
    `UPDATE horsies_workflow_tasks SET status = 'RUNNING', result = NULL, sub_workflow_summary = NULL, completed_at = NULL
     WHERE workflow_id = ${id} AND task_index = ${String(taskIndex)}`,
    1,
  );
}

/** The instant `milliseconds` from now; a negative value is in the past. */
function fromNow(milliseconds: number): Temporal.Instant {
  return Temporal.Now.instant().add({ milliseconds });
}

describe("layer 7: sub-workflows", () => {
  e2eTest("a completed child reads the same through get, the node row, the node info, resultFor and the child's row", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("success_surface", suite.app.start(parents.successSurface, {}));
    const [node, ...rest] = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.ok(node !== undefined);
    assert.deepEqual(rest, []);
    assert.equal(await workflowValue(handle), SUB_OK_VALUE);
    assert.equal(node.status, "COMPLETED");

    const childId = childIdOf(node);
    const [info, ...otherInfos] = await handleCall("the node infos", handle.tasks());

    assert.ok(info !== undefined);
    assert.deepEqual(otherInfos, []);
    assert.equal(info.status, "COMPLETED");
    assert.deepEqual(info.subWorkflowId, { kind: "recorded", value: childId });
    assert.deepEqual(info.result, { kind: "recorded", value: { kind: "outcome", outcome: { kind: "succeeded", value: SUB_OK_VALUE } } });
    assert.ok(info.subWorkflowSummary.kind === "recorded", "the node info holds the summary of the ended child");
    assert.deepEqual(
      [info.subWorkflowSummary.value.status, info.subWorkflowSummary.value.output, info.subWorkflowSummary.value.totalTasks],
      ["COMPLETED", SUB_OK_VALUE, 1],
    );
    assert.equal(info.subWorkflowSummary.value.completedTasks, 1);
    assert.equal(await nodeValue("the child node", handle.resultFor("e2e_sub_success_child")), SUB_OK_VALUE);

    const child = await workflowRow(suite.rows, childId);

    assert.deepEqual(
      [child.status, child.parentWorkflowId, child.parentTaskIndex, child.depth, child.rootWorkflowId],
      ["COMPLETED", handle.workflowId, node.taskIndex, 1, handle.workflowId],
    );
  });

  e2eTest("a failed child fails its node and the workflow; every read surface holds the child's failure", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("failure_fail_policy", suite.app.start(parents.failureFailPolicy, {}));
    const [node] = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.ok(node !== undefined);
    assert.equal(node.status, "FAILED");

    const childId = childIdOf(node);

    failedChildOf(await nodeOutcome("the child node", handle.resultFor("e2e_sub_failure_child")), childId);
    assert.ok((await workflowRow(suite.rows, handle.workflowId)).error?.includes(childId), "the workflow error names the child");
    subWorkflowFailure(await workflowError(handle), childId);

    const [info] = await handleCall("the node infos", handle.tasks());

    assert.ok(info !== undefined);
    assert.equal(info.status, "FAILED");
    assert.deepEqual(info.subWorkflowId, { kind: "recorded", value: childId });
    assert.ok(info.subWorkflowSummary.kind === "recorded" && info.subWorkflowSummary.value.status === "FAILED");
    assert.ok(info.result.kind === "recorded" && info.result.value.kind === "outcome", "the node info holds a result that reads");
    failedChildOf(info.result.value.outcome, childId);
  });

  e2eTest("under onError pause, a failed child pauses the workflow; its failure holds through the pause and the resume", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("failure_pause_policy", suite.app.start(parents.failurePausePolicy, {}));

    await waitForWorkflowStatus(suite.rows, handle.workflowId, "PAUSED", WORKFLOW_WAIT_MS);
    assert.equal(await workflowCode(handle), "WORKFLOW_PAUSED");

    const node = nodeById(await workflowNodes(suite.rows, handle.workflowId), "e2e_sub_pause_child");
    const childId = childIdOf(node);

    assert.equal(node.status, "FAILED");
    failedChildOf(await nodeOutcome("the child node", handle.resultFor("e2e_sub_pause_child")), childId);
    assert.ok((await workflowRow(suite.rows, handle.workflowId)).error?.includes(childId), "the paused workflow stores the child's failure");
    assert.equal(await handleCall("resume", handle.resume()), true);
    await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);
    subWorkflowFailure(await workflowError(handle), childId);
  });

  e2eTest("a node after a completed child reads its result, its summary and its output from the context", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("success_context_probe", suite.app.start(parents.successContextProbe, {}));

    assert.deepEqual(await workflowValue(handle), {
      resultValue: SUB_OK_VALUE,
      summaryStatus: "COMPLETED",
      summaryOutput: SUB_OK_VALUE,
      summaryCompletedTasks: 1,
    });
  });

  e2eTest("a node after a failed child gets the same failure as a parameter and through its context", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("failure_args_context_probe", suite.app.start(parents.failureArgsContextProbe, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);
    const childId = childIdOf(nodeById(nodes, parents.FAILURE_PROBE_CHILD));

    assert.deepEqual(await workflowValue(handle), {
      argsCode: "SUB_WORKFLOW_FAILED",
      argsChildId: childId,
      ctxChildId: childId,
      summaryStatus: "FAILED",
      summaryFailedTasks: 1,
    });
  });

  e2eTest("a failed optional child leaves the workflow to complete and keeps its failure in the node results", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("optional_failed_child", suite.app.start(parents.optionalFailedChild, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);
    const optional = nodeById(nodes, "e2e_sub_outputless_fail_child");

    assert.equal(await workflowValue(handle), "required_ok");
    assert.equal(optional.status, "FAILED");
    failedChildOf(await nodeOutcome("the optional child", handle.resultFor("e2e_sub_outputless_fail_child")), childIdOf(optional));

    const results = await handleCall("the node results", handle.results());

    assert.deepEqual(resultOutcome(results, "e2e_sub_outputless_ok"), { kind: "succeeded", value: "required_ok" });
    failedChildOf(resultOutcome(results, "e2e_sub_outputless_fail_child"), childIdOf(optional));
  });

  e2eTest("two children side by side keep their own success and failure", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("parallel_children", suite.app.start(parents.parallelChildren, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["COMPLETED", "FAILED"]);

    const okChild = childIdOf(nodeById(nodes, "e2e_sub_parallel_ok_child"));
    const failedChild = childIdOf(nodeById(nodes, "e2e_sub_parallel_fail_child"));

    assert.notEqual(okChild, failedChild);
    assert.equal(await nodeValue("the completed child", handle.resultFor("e2e_sub_parallel_ok_child")), SUB_OK_VALUE);
    failedChildOf(await nodeOutcome("the failed child", handle.resultFor("e2e_sub_parallel_fail_child")), failedChild);
    assert.deepEqual(
      (await handleCall("the node infos", handle.tasks())).map((info) =>
        info.subWorkflowSummary.kind === "recorded" ? info.subWorkflowSummary.value.status : "notRecorded",
      ),
      ["COMPLETED", "FAILED"],
    );
  });

  e2eTest("a nested child records its depth, its parent and its root, and its result reaches the root", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("nested_success", suite.app.start(parents.nestedSuccess, {}));

    assert.equal(await workflowValue(handle), SUB_OK_VALUE);

    const tree = await workflowTree(suite.rows, handle.workflowId);
    const [root, middle, leaf] = tree;

    assert.deepEqual(
      tree.map((row) => row.depth),
      [0, 1, 2],
    );
    assert.ok(root !== undefined && middle !== undefined && leaf !== undefined);
    assert.deepEqual([root.id, root.parentWorkflowId], [handle.workflowId, null]);
    assert.deepEqual([middle.parentWorkflowId, middle.rootWorkflowId, middle.status], [handle.workflowId, handle.workflowId, "COMPLETED"]);
    assert.deepEqual([leaf.parentWorkflowId, leaf.rootWorkflowId, leaf.status], [middle.id, handle.workflowId, "COMPLETED"]);
  });

  e2eTest("a nested failure reaches each level: the root reads the middle child's failure, the middle the leaf's", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("nested_failure", suite.app.start(parents.nestedFailure, {}));
    const [node] = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.ok(node !== undefined);

    const middleId = childIdOf(node);

    assert.equal(subWorkflowFailure(await workflowError(handle), middleId).summary.failedTasks, 1);

    const leafId = childIdOf(nodeById(await workflowNodes(suite.rows, middleId), CHILD_NODE));
    const middle = suite.app.workflowHandle(subNestedFailMiddle, middleId);

    assert.equal(subWorkflowFailure(await workflowError(middle), leafId).summary.failedTasks, 1);
    failedChildOf(await nodeOutcome("the node of the middle child", middle.resultFor(CHILD_NODE, subFail)), leafId);
  });

  e2eTest("the build of a child gets the typed result of the node before it", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("child_from_upstream", suite.app.start(parents.childFromUpstream, {}));

    assert.equal(await workflowValue(handle), parents.UPSTREAM_VALUE);
  });

  e2eTest("a child built from an upstream list has one node per item, and its last node reads each from its context", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("tenant_report", suite.app.start(tenantReport, {}));

    assert.equal(await workflowValue(handle), 30);

    const report = childIdOf(nodeById(await workflowNodes(suite.rows, handle.workflowId), "report"));

    assert.deepEqual(
      (await workflowNodes(suite.rows, report)).map((node) => [node.nodeId, node.status]),
      [
        ["shard_0", "COMPLETED"],
        ["shard_1", "COMPLETED"],
        ["shard_2", "COMPLETED"],
        ["total", "COMPLETED"],
      ],
    );
  });

  e2eTest("a child that allows failed dependencies runs, and its build gets the failure of the node before it", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("child_from_failed_upstream", suite.app.start(parents.childFromFailedUpstream, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["FAILED", "COMPLETED"]);
    assert.deepEqual(await workflowValue(handle), { kind: "failed", code: "E2E_UPSTREAM_FAIL" });
  });

  e2eTest("a static tagged union param keeps its variant through the stored form to the child's build", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("child_with_typed_param", suite.app.start(parents.childWithTypedParam, {}));

    assert.deepEqual(await workflowValue(handle), { kind: "dog", barkVolume: 7 });
  });

  e2eTest("a child under an any join starts after the first success, while the slow dependency runs", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("any_join_first_success", suite.app.start(parents.anyJoinFirstSuccess, {}));
    const childId = await childWorkflowOf(suite.rows, handle.workflowId, "l7_any_child", WORKFLOW_WAIT_MS);
    const slow = nodeById(await workflowNodes(suite.rows, handle.workflowId), "l7_any_slow_success");

    assert.ok(["ENQUEUED", "RUNNING"].includes(slow.status), `the slow dependency is ${slow.status}`);
    assert.equal(await workflowValue(handle), SUB_OK_VALUE);

    const child = nodeById(await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS), "l7_any_child");

    assert.deepEqual([child.status, child.subWorkflowId], ["COMPLETED", childId]);
  });

  e2eTest("a child under an any join whose dependencies all failed is SKIPPED and starts no child", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("any_join_all_fail_child", suite.app.start(parents.anyJoinAllFailChild, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);
    const child = nodeById(nodes, "l7_any_all_fail_child");

    assert.deepEqual(statusesOf(nodes), ["FAILED", "FAILED", "SKIPPED"]);
    assert.equal(child.subWorkflowId, null);
  });

  e2eTest("a child under a met quorum runs; the optional failure leaves the workflow to complete", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("quorum_met_child", suite.app.start(parents.quorumMetChild, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["COMPLETED", "COMPLETED", "FAILED", "COMPLETED"]);
    assert.equal(await workflowValue(handle), SUB_OK_VALUE);
  });

  e2eTest("a child under a quorum that two failures leave unmet is SKIPPED", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("quorum_unmet_child", suite.app.start(parents.quorumUnmetChild, {}));

    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS)), [
      "COMPLETED",
      "FAILED",
      "FAILED",
      "SKIPPED",
    ]);
  });

  e2eTest("a child under a quorum that the failures make impossible is SKIPPED before the slow dependency ends", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("quorum_impossible_child", suite.app.start(parents.quorumImpossibleChild, {}));
    const skipped = await waitForNode(suite.rows, handle.workflowId, "l7_quorum_impossible_child", WORKFLOW_WAIT_MS, "SKIPPED", (node) => node.status === "SKIPPED");
    const slow = nodeById(await workflowNodes(suite.rows, handle.workflowId), "l7_quorum_impossible_slow_c");

    assert.equal(skipped.subWorkflowId, null);
    assert.ok(["ENQUEUED", "RUNNING"].includes(slow.status), `the slow dependency is ${slow.status}`);
    await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);
  });

  e2eTest("a failed task skips the child after it", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("task_fail_skips_child", suite.app.start(parents.taskFailSkipsChild, {}));

    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS)), ["FAILED", "SKIPPED"]);
  });

  e2eTest("a failed child skips the task after it", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("child_fail_skips_task", suite.app.start(parents.childFailSkipsTask, {}));

    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS)), ["FAILED", "SKIPPED"]);
  });

  e2eTest("a skip passes through a chain of children and tasks", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("skip_chain", suite.app.start(parents.skipChain, {}));

    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS)), [
      "FAILED",
      "SKIPPED",
      "SKIPPED",
      "SKIPPED",
    ]);
  });

  e2eTest("a cancel of a workflow cancels its running child", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("cancel_running_child", suite.app.start(parents.cancelRunningChild, {}));
    const childId = await childWorkflowOf(suite.rows, handle.workflowId, "l7_cancel_running_child", WORKFLOW_WAIT_MS);

    await waitForWorkflowStatus(suite.rows, childId, "RUNNING", WORKFLOW_WAIT_MS);
    await handleCall("cancel", handle.cancel());
    await endedNodes(suite.rows, handle.workflowId, "CANCELLED", WORKFLOW_WAIT_MS);
    await waitForWorkflowStatus(suite.rows, childId, "CANCELLED", WORKFLOW_WAIT_MS);
  });

  e2eTest("a cancel before a child's dependency ended skips the child, which never starts", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("cancel_before_child_start", suite.app.start(parents.cancelBeforeChildStart, {}));

    await waitForNode(suite.rows, handle.workflowId, "l7_cancel_before_start_gate", WORKFLOW_WAIT_MS, "ENQUEUED or RUNNING", (node) =>
      ["ENQUEUED", "RUNNING"].includes(node.status),
    );
    await handleCall("cancel", handle.cancel());

    const child = nodeById(await endedNodes(suite.rows, handle.workflowId, "CANCELLED", WORKFLOW_WAIT_MS), "l7_cancel_before_start_child");

    assert.deepEqual([child.status, child.subWorkflowId], ["SKIPPED", null]);
  });

  e2eTest("a cancel of a completed workflow changes nothing", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("success_surface", suite.app.start(parents.successSurface, {}));

    await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);
    assert.equal(await workflowValue(handle), SUB_OK_VALUE);
    await handleCall("cancel", handle.cancel());
    assert.equal(await workflowStatus(suite.rows, handle.workflowId), "COMPLETED");
    assert.equal(await workflowValue(handle), SUB_OK_VALUE);
  });

  e2eTest("a cancel of a workflow cancels its child and the child's own child", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("cancel_nested_child", suite.app.start(parents.cancelNestedChild, {}));
    const middleId = await childWorkflowOf(suite.rows, handle.workflowId, "l7_cancel_nested_middle", WORKFLOW_WAIT_MS);
    const leafId = await childWorkflowOf(suite.rows, middleId, CHILD_NODE, WORKFLOW_WAIT_MS);

    await handleCall("cancel", handle.cancel());
    await endedNodes(suite.rows, handle.workflowId, "CANCELLED", WORKFLOW_WAIT_MS);
    await waitForWorkflowStatus(suite.rows, middleId, "CANCELLED", WORKFLOW_WAIT_MS);
    await waitForWorkflowStatus(suite.rows, leafId, "CANCELLED", WORKFLOW_WAIT_MS);
  });

  e2eTest("a pause of a workflow pauses its running child; get gives WORKFLOW_PAUSED", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("pause_running_child", suite.app.start(parents.pauseRunningChild, {}));
    const childId = await childWorkflowOf(suite.rows, handle.workflowId, "l7_pause_running_child", WORKFLOW_WAIT_MS);

    assert.equal(await handleCall("pause", handle.pause()), true);
    assert.equal(await workflowStatus(suite.rows, handle.workflowId), "PAUSED");
    assert.equal(await workflowStatus(suite.rows, childId), "PAUSED");
    assert.equal(await workflowCode(handle), "WORKFLOW_PAUSED");
  });

  e2eTest("a resume after a pause runs the workflow and its child to completion", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("pause_resume_child", suite.app.start(parents.pauseResumeChild, {}));
    const childId = await childWorkflowOf(suite.rows, handle.workflowId, "l7_pause_resume_child", WORKFLOW_WAIT_MS);

    assert.equal(await handleCall("pause", handle.pause()), true);
    assert.equal(await handleCall("resume", handle.resume()), true);
    await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);
    assert.equal(await workflowStatus(suite.rows, childId), "COMPLETED");
    assert.equal(await workflowValue(handle), parents.SLOW_CHILDREN.resume.value);
  });

  e2eTest("a cancel of a paused workflow cancels its child", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("pause_cancel_child", suite.app.start(parents.pauseCancelChild, {}));
    const childId = await childWorkflowOf(suite.rows, handle.workflowId, "l7_pause_then_cancel_child", WORKFLOW_WAIT_MS);

    assert.equal(await handleCall("pause", handle.pause()), true);
    await handleCall("cancel", handle.cancel());
    await endedNodes(suite.rows, handle.workflowId, "CANCELLED", WORKFLOW_WAIT_MS);
    assert.equal(await workflowStatus(suite.rows, childId), "CANCELLED");
  });

  e2eTest(
    "the reaper starts the child of a READY node that has none, and the workflow completes",
    "recovery",
    async (suite) => {
      await suite.worker("isolated-1");

      const handle = await startedHandle("ready_child_recovery", suite.app.start(parents.readyChildRecovery, {}));
      const id = sqlText(handle.workflowId);

      await waitForNode(suite.rows, handle.workflowId, "l7_recovery_ready_gate", WORKFLOW_WAIT_MS, "RUNNING", (node) => node.status === "RUNNING");
      assert.equal(await handleCall("pause", handle.pause()), true);
      await waitForNode(suite.rows, handle.workflowId, "l7_recovery_ready_gate", WORKFLOW_WAIT_MS, "COMPLETED", (node) => node.status === "COMPLETED");

      const waiting = nodeById(await workflowNodes(suite.rows, handle.workflowId), "l7_recovery_ready_child");

      assert.deepEqual([waiting.status, waiting.subWorkflowId], ["PENDING", null], "the pause kept the child node from its start");
      // One statement: the node becomes READY with no child, and the workflow RUNNING, as a step that promoted the
      // node and never started its child leaves them.
      await changeWorkflowRows(
        suite.rows,
        `make the child node of workflow ${handle.workflowId} READY with no child`,
        `WITH node AS (UPDATE horsies_workflow_tasks SET status = 'READY'
           WHERE workflow_id = ${id} AND node_id = 'l7_recovery_ready_child' AND status = 'PENDING' RETURNING 1)
         UPDATE horsies_workflows SET status = 'RUNNING' WHERE id = ${id} AND status = 'PAUSED' AND EXISTS (SELECT 1 FROM node)`,
        1,
      );
      await waitForWorkflowStatus(suite.rows, handle.workflowId, "COMPLETED", RECOVERY_WAIT_MS);

      const childId = childIdOf(nodeById(await workflowNodes(suite.rows, handle.workflowId), "l7_recovery_ready_child"));

      assert.equal(await workflowStatus(suite.rows, childId), "COMPLETED");
      assert.equal(await workflowValue(handle), SUB_OK_VALUE);
    },
    LONG_TEST_LIMIT_MS,
  );

  e2eTest("the reaper ends a parent node whose child completed but whose parent did not record it", "recovery", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("success_surface", suite.app.start(parents.successSurface, {}));

    await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);
    await unfinalizeWithNode(suite, handle.workflowId, 0);
    await waitForWorkflowStatus(suite.rows, handle.workflowId, "COMPLETED", RECOVERY_WAIT_MS);
    assert.equal(await workflowValue(handle), SUB_OK_VALUE);
  });

  e2eTest("the reaper ends a parent node whose child failed but whose parent did not record it", "recovery", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("failure_fail_policy", suite.app.start(parents.failureFailPolicy, {}));
    const [node] = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.ok(node !== undefined);
    await unfinalizeWithNode(suite, handle.workflowId, node.taskIndex);
    await waitForWorkflowStatus(suite.rows, handle.workflowId, "FAILED", RECOVERY_WAIT_MS);
    subWorkflowFailure(await workflowError(handle), childIdOf(node));
  });

  e2eTest("the reaper finalizes a child whose nodes ended but which did not end, then its parent", "recovery", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("success_surface", suite.app.start(parents.successSurface, {}));
    const [node] = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.ok(node !== undefined);

    const childId = childIdOf(node);

    await changeWorkflowRows(
      suite.rows,
      `put child ${childId} back to RUNNING`,
      `UPDATE horsies_workflows SET status = 'RUNNING', result = NULL, completed_at = NULL WHERE id = ${sqlText(childId)}`,
      1,
    );
    await unfinalizeWithNode(suite, handle.workflowId, node.taskIndex);
    await waitForWorkflowStatus(suite.rows, handle.workflowId, "COMPLETED", RECOVERY_WAIT_MS);
    assert.equal(await workflowStatus(suite.rows, childId), "COMPLETED");
    assert.equal(await workflowValue(handle), SUB_OK_VALUE);
  });

  e2eTest("the reaper finalizes again a parent with a failed optional child, and keeps the child's failure", "recovery", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("optional_failed_child", suite.app.start(parents.optionalFailedChild, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    await changeWorkflowRows(
      suite.rows,
      `put workflow ${handle.workflowId} back to RUNNING`,
      `UPDATE horsies_workflows SET status = 'RUNNING', result = NULL, error = NULL, completed_at = NULL, updated_at = NOW()
       WHERE id = ${sqlText(handle.workflowId)}`,
      1,
    );
    await waitForWorkflowStatus(suite.rows, handle.workflowId, "COMPLETED", RECOVERY_WAIT_MS);
    assert.equal(await workflowValue(handle), "required_ok");
    failedChildOf(
      resultOutcome(await handleCall("the node results", handle.results()), "e2e_sub_outputless_fail_child"),
      childIdOf(nodeById(nodes, "e2e_sub_outputless_fail_child")),
    );
  });

  e2eTest(
    "a child's node retries until it succeeds, and the child completes",
    "default",
    async (suite) => {
      await suite.worker("isolated-1");

      const token = randomUUID();
      const handle = await startedHandle("child_retries", suite.app.start(parents.childRetries, { token, succeedOnAttempt: 2 }));

      await endedNodes(suite.rows, handle.workflowId, "COMPLETED", RETRY_WAIT_MS);
      assert.equal(await workflowValue(handle), "succeeded_on_attempt_2");
      assert.equal(suite.markers.count(`${token}-attempt-`), 2);
    },
    LONG_TEST_LIMIT_MS,
  );

  e2eTest(
    "a child's node that exhausts its retries fails the child and the workflow",
    "default",
    async (suite) => {
      await suite.worker("isolated-1");

      const token = randomUUID();
      const handle = await startedHandle("child_retries", suite.app.start(parents.childRetries, { token, succeedOnAttempt: 10 }));
      const [node] = await endedNodes(suite.rows, handle.workflowId, "FAILED", RETRY_WAIT_MS);

      assert.ok(node !== undefined);
      subWorkflowFailure(await workflowError(handle), childIdOf(node));
      assert.equal(suite.markers.count(`${token}-attempt-`), 4);
    },
    LONG_TEST_LIMIT_MS,
  );

  e2eTest(
    "a child's node whose deadline passed is expired by the reaper; the child and the workflow fail",
    "recovery",
    async (suite) => {
      const handle = await startedHandle("expiring_child", suite.app.start(parents.expiringChild, { goodUntil: fromNow(-EXPIRED_BY_MS) }));
      const childId = await childWorkflowOf(suite.rows, handle.workflowId, "l7_good_until_expired_child", WORKFLOW_WAIT_MS);

      await suite.worker("isolated-1");

      const limit = FAST_RECOVERY.crashedWorkerRecoveryGraceMs + 2 * FAST_RECOVERY.checkIntervalMs + REAPER_PASS_MARGIN_MS;

      await endedNodes(suite.rows, handle.workflowId, "FAILED", limit);
      assert.equal(await workflowStatus(suite.rows, childId), "FAILED");
      assert.equal(nodeById(await workflowNodes(suite.rows, childId), CHILD_NODE).status, "FAILED");
      failedChildOf(await nodeOutcome("the child node", handle.resultFor("l7_good_until_expired_child")), childId);
    },
    LONG_TEST_LIMIT_MS,
  );

  e2eTest("a failed child that the policy requires fails the workflow with the child's failure", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("required_failed_child", suite.app.start(parents.requiredFailedChild, {}));
    const [node] = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.ok(node !== undefined);
    subWorkflowFailure(await workflowError(handle), childIdOf(node));
  });

  e2eTest("a policy whose second case requires a completed child completes the workflow", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("policy_case_children", suite.app.start(parents.policyCaseChildren, {}));

    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS)), ["FAILED", "COMPLETED"]);
    assert.equal(await workflowValue(handle), SUB_OK_VALUE);
  });

  e2eTest("a policy with a required child, a required task and an optional failed child completes; results hold each", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("mixed_required_optional", suite.app.start(parents.mixedRequiredOptional, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);
    const results = await handleCall("the node results", handle.results());

    assert.equal(await workflowValue(handle), "task_ok");
    assert.deepEqual(resultOutcome(results, "l7_policy_mixed_required_sub"), { kind: "succeeded", value: SUB_OK_VALUE });
    failedChildOf(resultOutcome(results, "l7_policy_mixed_optional_sub"), childIdOf(nodeById(nodes, "l7_policy_mixed_optional_sub")));
  });

  e2eTest("a handle built from the stored id reads the status, the outcome, the nodes and the results", "default", async (suite) => {
    await suite.worker("isolated-1");

    const started = await startedHandle("success_surface", suite.app.start(parents.successSurface, {}));

    await endedNodes(suite.rows, started.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    const handle = suite.app.workflowHandle(parents.successSurface, started.workflowId);

    assert.equal(await handleCall("status", handle.status()), "COMPLETED");
    assert.equal(await workflowValue(handle), SUB_OK_VALUE);

    const [info, ...rest] = await handleCall("the node infos", handle.tasks());

    assert.ok(info !== undefined);
    assert.deepEqual(rest, []);
    assert.equal(info.status, "COMPLETED");
    assert.equal(info.subWorkflowId.kind, "recorded");
    assert.ok(info.subWorkflowSummary.kind === "recorded" && info.subWorkflowSummary.value.status === "COMPLETED");
    assert.deepEqual(info.result, { kind: "recorded", value: { kind: "outcome", outcome: { kind: "succeeded", value: SUB_OK_VALUE } } });

    const results = await handleCall("the node results", handle.results());

    assert.deepEqual([...results.keys()], ["e2e_sub_success_child"]);
    assert.deepEqual(resultOutcome(results, "e2e_sub_success_child"), { kind: "succeeded", value: SUB_OK_VALUE });
  });

  e2eTest("an optional child whose own child fails keeps that failure; the workflow completes", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("deep_optional_failure", suite.app.start(parents.deepOptionalFailure, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.equal(await workflowValue(handle), "required_ok");
    failedChildOf(
      resultOutcome(await handleCall("the node results", handle.results()), "l7_deep_outputless_nested_fail"),
      childIdOf(nodeById(nodes, "l7_deep_outputless_nested_fail")),
    );
    assert.ok((await workflowTree(suite.rows, handle.workflowId)).some((row) => row.depth === 2), "the tree has a leaf at depth 2");
  });

  e2eTest("two required children that fail, one through its own child, each keep their failure; the workflow fails", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("deep_required_failures", suite.app.start(parents.deepRequiredFailures, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);
    const results = await handleCall("the node results", handle.results());

    for (const nodeId of ["l7_deep_all_failed_a", "l7_deep_all_failed_b"]) {
      const node = nodeById(nodes, nodeId);

      assert.equal(node.status, "FAILED", `node ${nodeId}`);
      failedChildOf(resultOutcome(results, nodeId), childIdOf(node));
    }

    const error = await workflowError(handle);

    assert.ok(error.subWorkflow !== null, `the workflow error is a sub-workflow failure: ${errorText(error)}`);
  });

  e2eTest("a completed child, a child after a failed task and an optional failed nested child each keep their result", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("deep_mixed_paths", suite.app.start(parents.deepMixedPaths, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);
    const results = await handleCall("the node results", handle.results());

    assert.equal(await workflowValue(handle), SUB_OK_VALUE);
    assert.deepEqual(resultOutcome(results, "l7_deep_mixed_allow_failed_sub"), {
      kind: "succeeded",
      value: { kind: "failed", code: "E2E_UPSTREAM_FAIL" },
    });
    failedChildOf(resultOutcome(results, "l7_deep_mixed_nested_optional_fail"), childIdOf(nodeById(nodes, "l7_deep_mixed_nested_optional_fail")));
  });

  e2eTest(
    "a child's task takes its queue's priority, and it is claimed before a later task of the same priority",
    "recovery_queues",
    async (suite) => {
      await suite.worker("isolated-4");

      const blocker = await sentHandle("the blocker", suite.app.send(serialBlocker, { durationMs: BLOCKER_MS }));

      await suite.rows.waitForStatus(blocker.taskId, "RUNNING", WORKFLOW_WAIT_MS);
      await startedHandle("serial_parent", suite.app.start(serialParent, {}));
      await pollUntil(WORKFLOW_WAIT_MS, "the child's task has a row", async () => (await suite.rows.tasksNamed(serialChildTask.name)).length === 1);

      const direct = await sentHandle("the direct task", suite.app.send(serialDirectTask, {}));

      await succeeded(blocker, WORKFLOW_WAIT_MS);
      await succeeded(direct, WORKFLOW_WAIT_MS);

      const [listed] = await suite.rows.tasksNamed(serialChildTask.name);
      const childTask = TaskId.parse(listed?.taskId ?? "");

      assert.ok(childTask.ok, `the child's task has an id: ${String(listed?.taskId)}`);
      await suite.rows.waitForStatus(childTask.value, "COMPLETED", WORKFLOW_WAIT_MS);

      const child = await suite.rows.taskRow(childTask.value);
      const directRow = await suite.rows.taskRow(direct.taskId);

      assert.deepEqual(
        [child.queueName, child.priority, directRow.status, directRow.priority],
        [SERIAL_QUEUE.name, SERIAL_QUEUE.priority, "COMPLETED", SERIAL_QUEUE.priority],
      );
      assert.ok(
        setTime(child.claimedAt, "the claim of the child's task") <= setTime(directRow.claimedAt, "the claim of the direct task"),
        "the child's task, sent first, was claimed first",
      );
    },
    LONG_TEST_LIMIT_MS,
  );
});
