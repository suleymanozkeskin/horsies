// The cancel with a fake addon: the cancelled task, each core error, and the refusal of an id before the call.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { cancelTask, type CancelTarget } from "../../../horsies/src/monitoring/cancel.ts";
import type { CancelAddon, TaskCancelledRecord } from "../../../horsies/src/native/facades/cancel.ts";
import type { NativeOutcome, RawCatalogError } from "../../../horsies/src/native/values/records.ts";
import {
  causeSlot,
  codeSlot,
  connectionCause,
  fakeHandle,
  flagSlot,
  taskIdOf,
  textSlot,
  uuidSlot,
  valueEnvelope,
} from "./support/fakes.ts";

const TASK_ID = taskIdOf("01890a5d-ac96-774b-bcce-b302099a8057");

/** A fake addon that answers `outcome` and logs the id and the policy of each call. */
function fakeTarget(outcome: NativeOutcome<TaskCancelledRecord>): { readonly target: CancelTarget; readonly calls: string[][] } {
  const calls: string[][] = [];

  const addon: CancelAddon = {
    cancelTask: async (_handle, taskId, policy) => {
      calls.push([taskId, policy]);

      return valueEnvelope(outcome);
    },
  };

  return { target: { addon, handle: fakeHandle() }, calls };
}

function refused(error: RawCatalogError): NativeOutcome<TaskCancelledRecord> {
  return { kind: "failed", error };
}

describe("cancelTask", () => {
  test("a cancelled task gives its id and the status it left, with the policy passed", async () => {
    const { target, calls } = fakeTarget({ kind: "done", record: { taskId: TASK_ID, wasStatus: "PENDING" } });
    const cancelled = await cancelTask(target, TASK_ID, { policy: "pendingOrClaimedOnly" });

    assert.deepEqual(cancelled, { ok: true, value: { taskId: TASK_ID, wasStatus: "PENDING" } });
    assert.deepEqual(calls, [[TASK_ID, "pendingOrClaimedOnly"]]);
  });

  test("each refusal of the core is its code", async () => {
    const cases: ReadonlyArray<readonly [RawCatalogError, string]> = [
      [{ class: "TaskActionNotFound", slots: [textSlot("message", "m"), uuidSlot("task_id", TASK_ID)] }, "TASK_NOT_FOUND"],
      [
        {
          class: "TaskActionNotCancellable",
          slots: [textSlot("message", "m"), uuidSlot("task_id", TASK_ID), codeSlot("current_status", "TaskStatus", "CANCELLED")],
        },
        "TASK_NOT_CANCELLABLE",
      ],
      [
        {
          class: "TaskActionIsWorkflowTask",
          slots: [textSlot("message", "m"), uuidSlot("task_id", TASK_ID), codeSlot("current_status", "TaskStatus", "PENDING")],
        },
        "TASK_IS_WORKFLOW_TASK",
      ],
      [
        {
          class: "TaskActionDatabaseOperationFailed",
          slots: [textSlot("message", "m"), flagSlot("retryable", true), uuidSlot("task_id", TASK_ID), causeSlot(connectionCause("cancel_task", true))],
        },
        "DATABASE_OPERATION_FAILED",
      ],
      [
        {
          class: "TaskActionOutcomeUnknown",
          slots: [textSlot("message", "m"), flagSlot("retryable", true), uuidSlot("task_id", TASK_ID), causeSlot(connectionCause("cancel_task", true))],
        },
        "OUTCOME_UNKNOWN",
      ],
      [
        {
          class: "TaskActionStoredDataInvalid",
          slots: [
            textSlot("message", "m"),
            uuidSlot("task_id", TASK_ID),
            { name: "cause", value: { kind: "exception", exception: { kind: "generated", class: "OutcomeDecodeError", text: "t" } } },
          ],
        },
        "STORED_DATA_INVALID",
      ],
    ];

    for (const [raw, code] of cases) {
      const answer = await cancelTask(fakeTarget(refused(raw)).target, TASK_ID, { policy: "includeRunning" });

      assert.ok(!answer.ok && answer.error.code === code, code);
    }
  });

  test("a lost reply is outcomeUnknown and retryable, and a failed connect is databaseOperationFailed", async () => {
    const unknown: RawCatalogError = {
      class: "TaskActionOutcomeUnknown",
      slots: [textSlot("message", "m"), flagSlot("retryable", true), uuidSlot("task_id", TASK_ID), causeSlot(connectionCause("cancel_task", true))],
    };

    const failed: RawCatalogError = {
      class: "TaskActionDatabaseOperationFailed",
      slots: [textSlot("message", "m"), flagSlot("retryable", false), uuidSlot("task_id", TASK_ID), causeSlot(connectionCause("cancel_task", false))],
    };

    const lost = await cancelTask(fakeTarget(refused(unknown)).target, TASK_ID, { policy: "includeRunning" });
    const notSent = await cancelTask(fakeTarget(refused(failed)).target, TASK_ID, { policy: "includeRunning" });

    assert.ok(!lost.ok && lost.error.kind === "outcomeUnknown" && lost.error.retryable && lost.error.taskId === TASK_ID);
    assert.ok(!notSent.ok && notSent.error.kind === "databaseOperationFailed" && !notSent.error.retryable);
  });

  test("a second cancel names the status CANCELLED, so a repeat tells a committed cancel", async () => {
    const raw: RawCatalogError = {
      class: "TaskActionNotCancellable",
      slots: [textSlot("message", "m"), uuidSlot("task_id", TASK_ID), codeSlot("current_status", "TaskStatus", "CANCELLED")],
    };

    const answer = await cancelTask(fakeTarget(refused(raw)).target, TASK_ID, { policy: "includeRunning" });

    assert.ok(!answer.ok && answer.error.code === "TASK_NOT_CANCELLABLE" && answer.error.currentStatus === "CANCELLED");
  });

  test("a status that cannot move to CANCELLED is a defect of the core", async () => {
    const { target } = fakeTarget({ kind: "done", record: { taskId: TASK_ID, wasStatus: "COMPLETED" } });

    await assert.rejects(cancelTask(target, TASK_ID, { policy: "includeRunning" }), { name: "ClientDefect" });
  });
});
