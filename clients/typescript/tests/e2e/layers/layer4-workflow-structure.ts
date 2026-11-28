// Layer 4: workflow structure (Rust `clients/rust/tests/worker/tests/layer4_workflow_structure.rs`, Python
// `clients/python/tests/e2e/test_layer4_workflow_structure.py`).
//
// The test process starts each workflow with `app.start`; its build runs here, and the worker runs its nodes. The
// workflows are the ones of `src/workflows/structure.ts`. Each node runs at least 50 ms, so the start of a node is
// strictly after the end of the nodes it waits for.

import assert from "node:assert/strict";
import { describe } from "node:test";

import { loadAddon } from "#horsies";
import { FAST_RECOVERY } from "../src/instances.ts";
import * as structure from "../src/workflows/structure.ts";
import { pause } from "../support/process.ts";
import { maxDuring, neverDuring, setTime, sqlText } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";
import { handleCall, startedHandle, WORKFLOW_WAIT_MS, workflowCode, workflowValue } from "../support/workflow-reads.ts";
import {
  endedNodes,
  nodeById,
  type NodeRow,
  nodesText,
  startedAfterEnd,
  statusesOf,
  taskOfNode,
  waitForNode,
  workflowNodes,
  workflowStatus,
} from "../support/workflow-rows.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** How long the test reads the nodes of a fan-out for RUNNING branches: the root and the branches run within it. */
const FANOUT_WATCH_MS = 3_000;

/** The interval of the reads of a watch. */
const WATCH_INTERVAL_MS = 50;

/** How long a fan-in may take to reach a state where some sources ended and the aggregate is PENDING. */
const PENDING_WINDOW_MS = 5_000;

/** How long the test watches a task whose deadline passed before any worker started. */
const EXPIRED_WATCH_MS = 2_000;

/** The time on top of a reaper interval for one reaper step to show: the pass, the write, and the polls of the wait. */
const REAPER_PASS_MARGIN_MS = 10_000;

/** The deadline of a node that expires: past before the node before it ends. */
const SHORT_DEADLINE_MS = 1_000;

/** The deadline of a node that a worker runs well before it. */
const LONG_DEADLINE_MS = 60_000;

/** The deadline of a root node that passes before the worker starts. */
const PASSED_DEADLINE_MS = 500;

/** How long after a deadline the test starts the worker. */
const DEADLINE_MARGIN_MS = 500;

/** The status of the aggregate of `fanin` at a read where one or two of its sources had ended. */
type AggregateRead = { readonly kind: "seen"; readonly status: string } | { readonly kind: "notSeen" };

/** Read the nodes of the `fanin` workflow `workflowId` until one or two sources ended: the aggregate's status then. */
async function aggregateWhileSourcesRun(suite: Suite, workflowId: string): Promise<AggregateRead> {
  const deadline = performance.now() + PENDING_WINDOW_MS;

  while (performance.now() < deadline) {
    const nodes = await workflowNodes(suite.rows, workflowId);
    const ended = ["a", "b", "c"].filter((id) => nodeById(nodes, id).status === "COMPLETED").length;

    if (ended > 0 && ended < 3) {
      return { kind: "seen", status: nodeById(nodes, "aggregate").status };
    }

    await pause(WATCH_INTERVAL_MS);
  }

  return { kind: "notSeen" };
}

/** The instant `milliseconds` from now. */
function fromNow(milliseconds: number): Temporal.Instant {
  return Temporal.Now.instant().add({ milliseconds });
}

/** The ends of the nodes of `ids` in `nodes`, in microseconds. */
function endsOf(nodes: readonly NodeRow[], ids: readonly string[]): readonly bigint[] {
  return ids.map((id) => setTime(nodeById(nodes, id).completedAt, `the end of node ${id}`));
}

/** The starts of the nodes of `ids` in `nodes`, in microseconds. */
function startsOf(nodes: readonly NodeRow[], ids: readonly string[]): readonly bigint[] {
  return ids.map((id) => setTime(nodeById(nodes, id).startedAt, `the start of node ${id}`));
}

function largest(values: readonly bigint[]): bigint {
  return values.reduce((left, right) => (right > left ? right : left));
}

function smallest(values: readonly bigint[]): bigint {
  return values.reduce((left, right) => (right < left ? right : left));
}

describe("layer 4: workflow structure", () => {
  e2eTest("a chain runs each node after the one before; each node records its start and its end", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("linear", suite.app.start(structure.linear, {}));

    assert.equal(await workflowValue(handle), "completed_c");

    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(
      nodes.map((node) => [node.taskIndex, node.nodeId, node.status]),
      [
        [0, "a", "COMPLETED"],
        [1, "b", "COMPLETED"],
        [2, "c", "COMPLETED"],
      ],
    );
    startedAfterEnd(nodes, "b", ["a"]);
    startedAfterEnd(nodes, "c", ["b"]);
  });

  e2eTest("the branches of a fan-out start after their root and run at once on four children", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("fanout", suite.app.start(structure.fanout, {}));

    const running = await maxDuring(FANOUT_WATCH_MS, WATCH_INTERVAL_MS, async () =>
      (await workflowNodes(suite.rows, handle.workflowId)).filter((node) => node.status === "RUNNING").length,
    );

    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.ok(running >= 2, `at most ${String(running)} branches were RUNNING at once`);

    for (const branch of ["b", "c", "d"]) {
      startedAfterEnd(nodes, branch, ["root"]);
    }

    assert.ok(largest(startsOf(nodes, ["b", "c", "d"])) < smallest(endsOf(nodes, ["b", "c", "d"])), `the branches ran at once: ${nodesText(nodes)}`);
  });

  e2eTest("the branches of a fan-out on one child run one after another and all complete", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("fanout", suite.app.start(structure.fanout, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED"]);

    const [first, second, third] = ["b", "c", "d"].toSorted((left, right) => {
      const [leftStart, rightStart] = startsOf(nodes, [left, right]);

      return leftStart !== undefined && rightStart !== undefined && leftStart < rightStart ? -1 : 1;
    });

    assert.ok(first !== undefined && second !== undefined && third !== undefined);
    startedAfterEnd(nodes, second, [first]);
    startedAfterEnd(nodes, third, [second]);
  });

  e2eTest("a fan-in node stays PENDING while some sources run, and starts after the last source ended", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("fanin", suite.app.start(structure.fanin, {}));

    assert.deepEqual(await aggregateWhileSourcesRun(suite, handle.workflowId), { kind: "seen", status: "PENDING" });

    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    startedAfterEnd(nodes, "aggregate", ["a", "b", "c"]);
  });

  e2eTest("the last node of a diamond starts after both branches ended, the slower one too", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("diamond", suite.app.start(structure.diamond, {}));

    assert.equal(await workflowValue(handle), "completed_d");

    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    startedAfterEnd(nodes, "b", ["a"]);
    startedAfterEnd(nodes, "c", ["a"]);
    startedAfterEnd(nodes, "d", ["b", "c"]);
  });

  e2eTest("each level of a seven-node graph starts after the nodes it waits for", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("complex_dag", suite.app.start(structure.complexDag, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(
      nodes.map((node) => node.nodeId),
      ["a", "b", "c", "d", "e", "f", "g"],
    );
    assert.deepEqual(statusesOf(nodes), Array.from({ length: 7 }, () => "COMPLETED"));

    for (const id of ["b", "c", "d"]) {
      startedAfterEnd(nodes, id, ["a"]);
    }

    startedAfterEnd(nodes, "e", ["b", "c"]);
    startedAfterEnd(nodes, "f", ["c", "d"]);
    startedAfterEnd(nodes, "g", ["e", "f"]);
  });

  e2eTest("the result of the output node is the result of the workflow", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("explicit_output", suite.app.start(structure.explicitOutput, {}));

    assert.deepEqual(await workflowValue(handle), { final: "result", count: 42 });
  });

  e2eTest("a workflow with no output node completes with NoWorkflowOutput; results() holds the terminal nodes' outcomes", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("fanout_without_output", suite.app.start(structure.fanoutWithoutOutput, {}));

    assert.deepEqual(await workflowValue(handle), { kind: "noWorkflowOutput" });

    const results = await handleCall("the node results", handle.results());

    for (const nodeId of ["b", "c", "d"]) {
      const result = results.get(nodeId);

      assert.deepEqual(result, { kind: "outcome", outcome: { kind: "succeeded", value: `completed_${nodeId}` } }, `terminal node ${nodeId}`);
    }
  });

  e2eTest("a workflow of one node completes with the node's result", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("single_node", suite.app.start(structure.singleNode, {}));

    assert.equal(await workflowValue(handle), "completed_only");

    const [only, ...rest] = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(rest, []);
    assert.ok(only !== undefined);
    assert.deepEqual([only.taskIndex, only.taskName, only.status], [0, "e2e_wf_step", "COMPLETED"]);
    assert.equal(only.startedAt.kind, "set");
    assert.equal(only.completedAt.kind, "set");
  });

  e2eTest("a failed first node skips the node after it, and that skip skips the next one", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("linear_fail_first", suite.app.start(structure.linearFailFirst, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["FAILED", "SKIPPED", "SKIPPED"]);
  });

  e2eTest("a failed middle node skips the node after it, which never starts; get gives the failed node's code", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("linear_fail_mid", suite.app.start(structure.linearFailMid, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["COMPLETED", "FAILED", "SKIPPED"]);

    const skipped = nodeById(nodes, "c");

    assert.equal(skipped.startedAt.kind, "notSet", "the skipped node never started");
    assert.equal(skipped.completedAt.kind, "set", "the skip records its time");
    assert.equal(await workflowCode(handle), "DELIBERATE_FAILURE");
  });

  e2eTest("a failed branch of a fan-out leaves its sibling branches to complete", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("fanout_one_fail", suite.app.start(structure.fanoutOneFail, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["COMPLETED", "FAILED", "COMPLETED", "COMPLETED"]);
  });

  e2eTest("a fan-in node whose source failed is SKIPPED with no start", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handle = await startedHandle("fanin_one_fail", suite.app.start(structure.faninOneFail, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["FAILED", "COMPLETED", "COMPLETED", "SKIPPED"]);

    const aggregate = nodeById(nodes, "aggregate");

    assert.equal(aggregate.startedAt.kind, "notSet", "the aggregate never started");
    assert.equal(aggregate.completedAt.kind, "set", "the skip records its time");
  });

  e2eTest("a cancel of a running workflow skips its waiting nodes; get gives WORKFLOW_CANCELLED", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("slow_linear", suite.app.start(structure.slowLinear, {}));

    await waitForNode(suite.rows, handle.workflowId, "a", WORKFLOW_WAIT_MS, "COMPLETED", (node) => node.status === "COMPLETED");

    await handleCall("cancel", handle.cancel());

    const nodes = await endedNodes(suite.rows, handle.workflowId, "CANCELLED", WORKFLOW_WAIT_MS);

    for (const node of nodes.slice(1)) {
      assert.ok(["SKIPPED", "COMPLETED", "RUNNING"].includes(node.status), `node ${String(node.nodeId)} is ${node.status}`);
    }

    assert.equal(await workflowCode(handle), "WORKFLOW_CANCELLED");
  });

  e2eTest(
    "a node whose deadline passes while it waits is never claimed: the reaper expires it and the workflow fails with TASK_EXPIRED",
    "recovery",
    async (suite) => {
      await suite.worker("isolated-1");

      const handle = await startedHandle(
        "expiring_node",
        suite.app.start(structure.expiringNode, { goodUntil: fromNow(SHORT_DEADLINE_MS) }),
      );

      const taskId = await taskOfNode(suite.rows, handle.workflowId, "b", WORKFLOW_WAIT_MS);

      await suite.rows.waitForStatus(taskId, "EXPIRED", structure.EXPIRING_FIRST_MS + FAST_RECOVERY.checkIntervalMs + REAPER_PASS_MARGIN_MS);

      const expired = await suite.rows.taskRow(taskId);

      assert.equal(expired.errorCode, "TASK_EXPIRED");
      assert.deepEqual([expired.claimedAt.kind, expired.startedAt.kind, expired.workerId], ["notSet", "notSet", null], "no claim took the task");

      const endedBy = await suite.rows.query(
        `read how task ${taskId} ended`,
        `SELECT terminalization_kind::text FROM horsies_task_history WHERE task_id = ${sqlText(taskId)}`,
      );

      assert.equal(endedBy.rows[0]?.[0], "EXPIRE_PENDING", "the pending expiry of the reaper ended the task");

      const recoveryLimit = FAST_RECOVERY.crashedWorkerRecoveryGraceMs + FAST_RECOVERY.checkIntervalMs + REAPER_PASS_MARGIN_MS;
      const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", recoveryLimit);

      assert.deepEqual(statusesOf(nodes), ["COMPLETED", "FAILED"]);
      assert.equal(await workflowCode(handle), "TASK_EXPIRED");
    },
  );

  e2eTest("a root node whose deadline passed before the worker started is never claimed", "default", async (suite) => {
    const handle = await startedHandle(
      "deadline_node",
      suite.app.start(structure.deadlineNode, { goodUntil: fromNow(PASSED_DEADLINE_MS) }),
    );

    const taskId = await taskOfNode(suite.rows, handle.workflowId, "deadline", WORKFLOW_WAIT_MS);

    await pause(PASSED_DEADLINE_MS + DEADLINE_MARGIN_MS);
    await suite.worker("isolated-1");
    await neverDuring(EXPIRED_WATCH_MS, "the task left PENDING and EXPIRED", async () => {
      const status = await suite.rows.taskStatus(taskId);

      return status !== "PENDING" && status !== "EXPIRED";
    });

    const row = await suite.rows.taskRow(taskId);

    assert.ok(["PENDING", "EXPIRED"].includes(row.status), `the task is ${row.status}`);
    assert.equal(row.claimedAt.kind, "notSet", "no claim took the task");
  });

  e2eTest("a node that a worker runs before its deadline completes", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle(
      "deadline_node",
      suite.app.start(structure.deadlineNode, { goodUntil: fromNow(LONG_DEADLINE_MS) }),
    );

    assert.equal(await workflowValue(handle), "completed_deadline");
    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS)), ["COMPLETED"]);
  });

  e2eTest("a bounded get of a workflow that no worker runs is waitTimeout", "default", async (suite) => {
    const handle = await startedHandle("slow_linear", suite.app.start(structure.slowLinear, {}));
    const read = await handle.get({ waitMs: 200 });

    assert.deepEqual(read, { ok: false, error: { kind: "waitTimeout", code: "WAIT_TIMEOUT" } });
    assert.equal(await workflowStatus(suite.rows, handle.workflowId), "RUNNING", "the workflow still runs");
  });
});
