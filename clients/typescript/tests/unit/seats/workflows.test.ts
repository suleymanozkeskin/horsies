// The workflow calls of a seat body past the bound of the relay: each handle call gets `DB_OPERATION_FAILED` of the
// handle catalog, retryable, with the workflow id and a cause that names the bound; a start and a restart get the
// relay's refusal, `TooManyPendingCalls`. Nothing goes to the main thread.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { isObjectLike } from "../../../horsies/src/codec/values.ts";
import type { RawCatalogError } from "../../../horsies/src/native/values/records.ts";
import { IsolateRelay } from "../../../horsies/src/seats/relay.ts";
import { TooManyPendingCalls } from "../../../horsies/src/thrown-errors.ts";
import { relayedWorkflowCore, type StartRequest, type WorkflowCore } from "../../../horsies/src/workflow/core.ts";
import { handleErrorOfRaw } from "../../../horsies/src/workflow/handle-error.ts";
import { TASK_ID } from "../app/support.ts";
import { SPEC_DATA, WORKFLOW_ID } from "../workflow/core-support.ts";
import { TEST_RELAY_BOUNDS } from "./support.ts";

/** The text that names the relay bound and its value. */
const BOUND_TEXT = `its bound (MAX_RUN_WORK = ${String(TEST_RELAY_BOUNDS.opsWaiting)})`;

/** A workflow core of a seat isolate whose relay holds its whole waiting bound, and the operations that it sent. */
function fullCore(): { readonly core: WorkflowCore; readonly sent: string[] } {
  const sent: string[] = [];

  const relay = new IsolateRelay((message) => {
    if (message.kind === "op") {
      sent.push(message.request.area);
    }
  }, TEST_RELAY_BOUNDS);

  for (let index = 0; index < TEST_RELAY_BOUNDS.opsInFlight + TEST_RELAY_BOUNDS.opsWaiting; index += 1) {
    void relay.request("sends", { kind: "taskInfo", taskId: TASK_ID });
  }

  sent.length = 0;

  return { core: relayedWorkflowCore(relay), sent };
}

/** The facts of a handle failure that the tests compare. */
function handleFailure(raw: RawCatalogError): { readonly code: string; readonly retryable: boolean; readonly workflowId: string; readonly bound: boolean } {
  const error = handleErrorOfRaw(raw);

  return { code: error.code, retryable: error.retryable, workflowId: error.workflowId, bound: error.message.includes(BOUND_TEXT) };
}

const BOUND_FAILURE = { code: "DB_OPERATION_FAILED", retryable: true, workflowId: WORKFLOW_ID, bound: true };

/** The error of a `failed` read, or the fail of the test. */
function failedError(read: { readonly kind: string }): RawCatalogError {
  const error: unknown = Reflect.get(read, "error");

  if (read.kind !== "failed" || !isObjectLike(error)) {
    return assert.fail(`the call past the bound gave ${read.kind}`);
  }

  // SAFETY: a `failed` variant of a workflow read holds the raw record of its catalog.
  return error as RawCatalogError;
}

const START_REQUEST: StartRequest = {
  spec: {
    name: "nightly",
    definitionKey: "nightly",
    onError: "FAIL",
    nodes: [],
    output: 0,
    successPolicy: { kind: "noPolicy" },
  },
  workflowId: WORKFLOW_ID,
  sentAtMicros: 1n,
  trace: { kind: "noCarrier" },
};

describe("each workflow call kind past the bound of the relay", () => {
  test("a status read: DB_OPERATION_FAILED, retryable, with the workflow id", async () => {
    const { core, sent } = fullCore();

    assert.deepEqual(handleFailure(failedError(await core.status(WORKFLOW_ID))), BOUND_FAILURE);
    assert.deepEqual(sent, []);
  });

  test("a node results read: DB_OPERATION_FAILED", async () => {
    const { core, sent } = fullCore();

    assert.deepEqual(handleFailure(failedError(await core.nodeResults(WORKFLOW_ID))), BOUND_FAILURE);
    assert.deepEqual(sent, []);
  });

  test("a nodes read: DB_OPERATION_FAILED", async () => {
    const { core, sent } = fullCore();

    assert.deepEqual(handleFailure(failedError(await core.nodes(WORKFLOW_ID))), BOUND_FAILURE);
    assert.deepEqual(sent, []);
  });

  test("a node result read: DB_OPERATION_FAILED", async () => {
    const { core, sent } = fullCore();

    assert.deepEqual(handleFailure(failedError(await core.nodeResult(WORKFLOW_ID, "count"))), BOUND_FAILURE);
    assert.deepEqual(sent, []);
  });

  test("a cancel: DB_OPERATION_FAILED", async () => {
    const { core, sent } = fullCore();

    assert.deepEqual(handleFailure(failedError(await core.cancel(WORKFLOW_ID))), BOUND_FAILURE);
    assert.deepEqual(sent, []);
  });

  test("a pause: DB_OPERATION_FAILED", async () => {
    const { core, sent } = fullCore();

    assert.deepEqual(handleFailure(failedError(await core.pause(WORKFLOW_ID))), BOUND_FAILURE);
    assert.deepEqual(sent, []);
  });

  test("a resume: DB_OPERATION_FAILED", async () => {
    const { core, sent } = fullCore();

    assert.deepEqual(handleFailure(failedError(await core.resume(WORKFLOW_ID))), BOUND_FAILURE);
    assert.deepEqual(sent, []);
  });

  test("a start: the relay's refusal, TooManyPendingCalls with the bound", async () => {
    const { core, sent } = fullCore();

    await assert.rejects(core.start(START_REQUEST), (cause: unknown) => cause instanceof TooManyPendingCalls && cause.bound === TEST_RELAY_BOUNDS.opsWaiting);
    assert.deepEqual(sent, []);
  });

  test("a restart: the relay's refusal, TooManyPendingCalls with the bound", async () => {
    const { core, sent } = fullCore();

    await assert.rejects(
      core.restart({ specData: SPEC_DATA, workflowId: WORKFLOW_ID, sentAtMicros: 1n, trace: { kind: "noCarrier" } }),
      (cause: unknown) => cause instanceof TooManyPendingCalls && cause.bound === TEST_RELAY_BOUNDS.opsWaiting,
    );
    assert.deepEqual(sent, []);
  });
});
