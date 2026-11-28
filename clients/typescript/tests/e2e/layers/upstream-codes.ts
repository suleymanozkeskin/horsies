// The custom code of an upstream error that a node reads (`h.taskOutcome`). The child's task registry holds the
// `errorCodes` of each contract, so the core refuses a stored custom code that the upstream task does not declare
// before the body runs: the task fails with `WORKER_SERIALIZATION_ERROR`, and so does its node.
//
// Each test holds the reader back with the node `held` (`GATE_MARKER`), changes the stored code of the node
// `upstream` after it failed, then lets `held` end, so the reader's input is built from the changed result.

import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { describe } from "node:test";

import { loadAddon } from "#horsies";
import { GATE_MARKER, READER_RAN_MARKER, readFailWith } from "../src/tasks/workflow-nodes.ts";
import { upstreamCodeDeclared, upstreamCodeOpen } from "../src/workflows/upstream-codes.ts";
import { errorCode, infoOf } from "../support/outcome.ts";
import { e2eTest, type Suite } from "../support/suite.ts";
import { nodeError, startedHandle, WORKFLOW_WAIT_MS, workflowValue } from "../support/workflow-reads.ts";
import { changeWorkflowRows, endedNodes, nodeById, statusesOf, taskOfNode, waitForNode } from "../support/workflow-rows.ts";
import { sqlText } from "../support/rows.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The code of a run that the worker refuses. */
const REFUSAL_CODE = "WORKER_SERIALIZATION_ERROR";

/** The stored result `text` of an upstream error, with `code` as its custom code. Fails the test for another form. */
function resultWithCode(text: string, code: string): string {
  const stored: unknown = JSON.parse(text);

  assert.ok(stored instanceof Object && "err" in stored && stored.err instanceof Object && "error_code" in stored.err, `a stored error: ${text}`);

  stored.err.error_code = code;

  return JSON.stringify(stored);
}

/**
 * Wait until the node `upstream` of `workflowId` failed, write `code` as the custom code of its stored error, then let
 * the node `held` end. The workers run the nodes `upstream` and `held` side by side.
 */
async function changeUpstreamCode(suite: Suite, workflowId: string, code: string): Promise<void> {
  const failedNode = await waitForNode(suite.rows, workflowId, "upstream", WORKFLOW_WAIT_MS, "failed", (found) => found.status === "FAILED");

  assert.ok(failedNode.result !== null, "the failed node holds its stored result");

  await changeWorkflowRows(
    suite.rows,
    `write the code ${code} into the stored error of node upstream`,
    `UPDATE horsies_workflow_tasks SET result = ${sqlText(resultWithCode(failedNode.result, code))}
     WHERE workflow_id = ${sqlText(workflowId)} AND node_id = 'upstream'`,
    1,
  );
  writeFileSync(suite.markers.marker(GATE_MARKER), "open");
}

describe("the custom code of an upstream error", () => {
  e2eTest("a stored code that the upstream task does not declare fails the reader before its body runs", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("upstream_code_declared", suite.app.start(upstreamCodeDeclared, {}));

    await changeUpstreamCode(suite, handle.workflowId, "OLD_CODE");

    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["FAILED", "COMPLETED", "FAILED"]);

    const taskId = await taskOfNode(suite.rows, handle.workflowId, "reader", WORKFLOW_WAIT_MS);
    const info = await infoOf(suite.app.taskHandle(readFailWith, taskId));
    const [attempt, ...rest] = info.attempts;

    assert.deepEqual(rest, []);
    assert.deepEqual([info.status, attempt?.outcome, attempt?.errorCode], ["FAILED", "FAILED", { kind: "recorded", value: REFUSAL_CODE }]);

    const error = await nodeError("node reader", handle.resultFor("reader"));

    assert.equal(errorCode(error).code, REFUSAL_CODE);
    assert.equal(
      error.message,
      "kwarg 'upstream': the upstream outcome of task 'e2e_wf_fail_with' holds the custom code 'OLD_CODE', which the task does not declare",
    );
    assert.deepEqual(suite.markers.read(READER_RAN_MARKER), { kind: "absent" }, "the body of the reader did not run");
    assert.equal(nodeById(nodes, "reader").status, "FAILED");
  });

  e2eTest("a stored code that the upstream task declares reaches the reader", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("upstream_code_declared", suite.app.start(upstreamCodeDeclared, {}));

    await changeUpstreamCode(suite, handle.workflowId, "OPTIONAL_FAIL");

    assert.equal(await workflowValue(handle), "custom_OPTIONAL_FAIL");
    assert.deepEqual(suite.markers.read(READER_RAN_MARKER), { kind: "found", text: "readFailWith\n" });
  });

  e2eTest("an upstream task with no errorCodes lets every stored code reach the reader", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("upstream_code_open", suite.app.start(upstreamCodeOpen, {}));

    await changeUpstreamCode(suite, handle.workflowId, "ANY_CODE");

    assert.equal(await workflowValue(handle), "custom_ANY_CODE");
    assert.deepEqual(suite.markers.read(READER_RAN_MARKER), { kind: "found", text: "readOpenFail\n" });
  });
});
