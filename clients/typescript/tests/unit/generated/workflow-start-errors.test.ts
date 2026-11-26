import { describe, expect, test } from "bun:test";

import { workflowStartCoreErrorFromNative } from "../../../horsies/src/_catalog/workflow_start_errors.ts";
import { CONNECTION_FAILURE, expectConverted, NO_CAUSE, WORKFLOW_ID } from "./support/catalog-errors.ts";

const MESSAGE = "the start failed";

const WORKFLOW_NAME = "fulfil_order";

describe("workflowStartCoreErrorFromNative", () => {
  test("WorkflowStartValidationFailed", () => {
    const value = workflowStartCoreErrorFromNative({
      class: "WorkflowStartValidationFailed",
      slots: {
        message: MESSAGE,
        workflow_name: WORKFLOW_NAME,
        workflow_id: WORKFLOW_ID,
        exception: NO_CAUSE,
      },
    });

    expectConverted(value, {
      shown: {
        code: "VALIDATION_FAILED",
        retryable: false,
        workflowName: WORKFLOW_NAME,
        workflowId: WORKFLOW_ID,
      },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: NO_CAUSE,
    });
  });

  test("WorkflowStartIdConflict names the other workflow", () => {
    const value = workflowStartCoreErrorFromNative({
      class: "WorkflowStartIdConflict",
      slots: {
        message: MESSAGE,
        workflow_name: WORKFLOW_NAME,
        workflow_id: WORKFLOW_ID,
        existing_workflow_name: "refund_order",
      },
    });

    expectConverted(value, {
      shown: {
        code: "WORKFLOW_ID_CONFLICT",
        retryable: false,
        workflowName: WORKFLOW_NAME,
        workflowId: WORKFLOW_ID,
        existingWorkflowName: "refund_order",
      },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("WorkflowStartCommandConflict", () => {
    const value = workflowStartCoreErrorFromNative({
      class: "WorkflowStartCommandConflict",
      slots: { message: MESSAGE, workflow_name: WORKFLOW_NAME, workflow_id: WORKFLOW_ID },
    });

    expectConverted(value, {
      shown: {
        code: "WORKFLOW_COMMAND_CONFLICT",
        retryable: false,
        workflowName: WORKFLOW_NAME,
        workflowId: WORKFLOW_ID,
      },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("WorkflowStartEnqueueFailed", () => {
    const value = workflowStartCoreErrorFromNative({
      class: "WorkflowStartEnqueueFailed",
      slots: {
        message: MESSAGE,
        retryable: true,
        workflow_name: WORKFLOW_NAME,
        workflow_id: WORKFLOW_ID,
        exception: CONNECTION_FAILURE,
      },
    });

    expectConverted(value, {
      shown: {
        code: "ENQUEUE_FAILED",
        retryable: true,
        workflowName: WORKFLOW_NAME,
        workflowId: WORKFLOW_ID,
      },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: CONNECTION_FAILURE,
    });
  });

  test("the class that only a blocking form gives is not a class of this build", () => {
    expect(() =>
      workflowStartCoreErrorFromNative(
        JSON.parse('{"class":"WorkflowStartAsyncContext","slots":{"message":"m"}}'),
      ),
    ).toThrow("workflowStartCoreErrorFromNative: unknown class name WorkflowStartAsyncContext");
  });
});
