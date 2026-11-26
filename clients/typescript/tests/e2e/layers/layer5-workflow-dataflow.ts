// Layer 5: workflow data flow (Rust `clients/rust/tests/worker/tests/layer5_workflow_dataflow.rs`, Python
// `clients/python/tests/e2e/test_layer5_workflow_dataflow.py`), and the workflow context of a task run (#2342).
//
// A node gets the result of a dependency as a parameter declared `h.taskOutcome(contract)`, or through its workflow
// context from its context sources (`contextNode` of `src/workflows/node-options.ts`). The workflows are the ones of
// `src/workflows/dataflow.ts`.

import assert from "node:assert/strict";
import { describe } from "node:test";

import { defineWorkflow, h } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { contextReport, produceInt } from "../src/tasks/workflow-nodes.ts";
import * as dataflow from "../src/workflows/dataflow.ts";
import { errorCode, sentHandle, succeeded } from "../support/outcome.ts";
import { e2eTest } from "../support/suite.ts";
import { nodeError, nodeValue, startedHandle, WORKFLOW_WAIT_MS, workflowCode, workflowValue } from "../support/workflow-reads.ts";
import { endedNodes, nodeById, startedAfterEnd, statusesOf } from "../support/workflow-rows.ts";
import { node } from "../src/workflows/node-options.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/**
 * A workflow whose node input has the key `extra`, which `e2e_wf_produce_int` does not take. The value comes from a
 * variable, so the static types do not see it; the start's checks refuse it. Only the app of its test lists it: the
 * check of every other app would refuse it too.
 */
const extraInputKey = defineWorkflow(
  {
    name: "e2e_extra_input_key",
    definitionKey: "tests.e2e.extra_input_key.v1",
    params: { extra: h.int() },
    output: h.int(),
    checkCases: [{ extra: 2 }],
  },
  (w, { extra }) => {
    const input = { value: 1, extra };

    return { output: w.task(produceInt, input, node("a")) };
  },
);

describe("layer 5: workflow data flow", () => {
  e2eTest("a node gets its dependency's result as a parameter", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("args_from_single", suite.app.start(dataflow.argsFromSingle, {}));

    assert.equal(await workflowValue(handle), 10);
  });

  e2eTest("a node gets the results of two dependencies, one per parameter, in its strict input", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("args_from_multiple", suite.app.start(dataflow.argsFromMultiple, {}));

    assert.equal(await workflowValue(handle), 7);
  });

  e2eTest("a result passes through two nodes in a chain", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("args_from_chain", suite.app.start(dataflow.argsFromChain, {}));

    assert.equal(await workflowValue(handle), 12);
  });

  e2eTest("an object result keeps its members and its nested object through a parameter", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("args_from_object", suite.app.start(dataflow.argsFromObject, {}));

    assert.deepEqual(await workflowValue(handle), { key: "test_value", count: 42, nested: { inner: true } });
  });

  e2eTest("a node that allows failed dependencies runs with the failure and passes its code on", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("failure_passed_on", suite.app.start(dataflow.failurePassedOn, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["FAILED", "FAILED"]);
    assert.equal(nodeById(nodes, "b").startedAt.kind, "set", "node b ran with the failure");
    assert.equal(errorCode(await nodeError("node b", handle.resultFor("b"))).code, "DELIBERATE_FAILURE");
    assert.equal(await workflowCode(handle), "DELIBERATE_FAILURE");
  });

  e2eTest("a node that allows failed dependencies gets the original error; a policy that requires it completes the workflow", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("failure_reported", suite.app.start(dataflow.failureReported, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["FAILED", "COMPLETED"]);
    assert.equal(await workflowValue(handle), "received_error_DELIBERATE_FAILURE");
  });

  e2eTest("a node that allows failed dependencies gets UPSTREAM_SKIPPED for a skipped dependency", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("skip_reported", suite.app.start(dataflow.skipReported, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["FAILED", "SKIPPED", "COMPLETED"]);
    assert.equal(await nodeValue("node c", handle.resultFor("c")), "received_upstream_skipped");
  });

  e2eTest("a failed dependency skips a node that does not allow it; the workflow fails with the dependency's code", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("failure_skips", suite.app.start(dataflow.failureSkips, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["FAILED", "SKIPPED"]);
    assert.equal(nodeById(nodes, "b").startedAt.kind, "notSet", "the skipped node never ran");
    assert.equal(await workflowCode(handle), "DELIBERATE_FAILURE");
  });

  e2eTest("a node reads the result of its context source from its workflow context", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("context_single", suite.app.start(dataflow.contextSingle, {}));

    assert.equal(await workflowValue(handle), 7);
  });

  e2eTest("a node reads the results of its two context sources from its workflow context", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("context_multiple", suite.app.start(dataflow.contextMultiple, {}));

    assert.equal(await workflowValue(handle), 7);
  });

  e2eTest("a node gets one dependency as a parameter and another through its context", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("mixed_sources", suite.app.start(dataflow.mixedSources, {}));

    assert.deepEqual(await workflowValue(handle), { fromArgs: 14, fromCtx: 3 });
  });

  e2eTest("one dependency reaches a node as a parameter and through its context, the same value on both paths", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("same_source_twice", suite.app.start(dataflow.sameSourceTwice, {}));

    assert.deepEqual(await workflowValue(handle), { fromArgs: 77, fromCtx: 77 });
  });

  e2eTest("under an any join, a node waits for its context source, which ends last, and reads it", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("any_join_context_gate", suite.app.start(dataflow.anyJoinContextGate, {}));

    assert.equal(await workflowValue(handle), "completed_b");

    const nodes = await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS);

    assert.deepEqual(statusesOf(nodes), ["COMPLETED", "COMPLETED", "COMPLETED"]);
    startedAfterEnd(nodes, "c", ["b"]);
  });

  e2eTest("under an any join with one failed dependency, the node runs and reads the one that succeeded", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await startedHandle("any_join_reads_success", suite.app.start(dataflow.anyJoinReadsSuccess, {}));
    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", WORKFLOW_WAIT_MS);

    assert.equal(nodeById(nodes, "reader").status, "COMPLETED");
    assert.equal(await nodeValue("the reader", handle.resultFor("reader")), 9);
  });

  e2eTest("a node input with a key that the task does not take is refused at the start; nothing is written", "default", async (suite) => {
    const app = suite.otherApp([produceInt], [extraInputKey]);
    const started = await app.start(extraInputKey, { extra: 2 });

    assert.ok(!started.ok, "the start was refused");
    assert.equal(started.error.code, "VALIDATION_FAILED");
    assert.ok(started.error.message.includes("extra"), `the refusal names the key: ${started.error.message}`);
    assert.equal(await suite.rows.count("count the workflows", "SELECT count(*) FROM horsies_workflows"), 0);
  });

  e2eTest("a context task in a node with context sources gets its workflow id, node index, task name and the source's result", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("context_of_node", suite.app.start(dataflow.contextOfNode, {}));

    assert.deepEqual(await workflowValue(handle), {
      kind: "context",
      workflowId: handle.workflowId,
      taskIndex: 1,
      taskName: "e2e_wf_context_report",
      sourceValue: dataflow.REPORTED_SOURCE_VALUE,
    });
  });

  e2eTest("a context task in a node with no context sources gets noContext; a task with no context in the same workflow runs", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await startedHandle("context_of_node_without_sources", suite.app.start(dataflow.contextOfNodeWithoutSources, {}));

    assert.deepEqual(await workflowValue(handle), { kind: "noContext" });
    assert.deepEqual(statusesOf(await endedNodes(suite.rows, handle.workflowId, "COMPLETED", WORKFLOW_WAIT_MS)), [
      "COMPLETED",
      "COMPLETED",
      "COMPLETED",
    ]);
  });

  e2eTest("a direct send of a context task gets noContext", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_wf_context_report", suite.app.send(contextReport, { source: "a" }));

    assert.deepEqual(await succeeded(handle, WORKFLOW_WAIT_MS), { kind: "noContext" });
  });
});
