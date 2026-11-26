import { describe, expect, test } from "bun:test";

import { handleCoreErrorFromNative } from "../../../horsies/src/_catalog/workflow_handle_errors.ts";
import {
  CONNECTION_FAILURE,
  CONTRACT_VIOLATION,
  expectConverted,
  SERVER_FAILURE,
  WORKFLOW_ID,
} from "./support/catalog-errors.ts";

const MESSAGE = "the workflow call failed";

describe("handleCoreErrorFromNative", () => {
  test("WorkflowHandleNotFound", () => {
    const value = handleCoreErrorFromNative({
      class: "WorkflowHandleNotFound",
      slots: { message: MESSAGE, workflow_id: WORKFLOW_ID },
    });

    expectConverted(value, {
      shown: { kind: "notFound", code: "WORKFLOW_NOT_FOUND", retryable: false, workflowId: WORKFLOW_ID },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("WorkflowHandleDatabaseOperationFailed", () => {
    const value = handleCoreErrorFromNative({
      class: "WorkflowHandleDatabaseOperationFailed",
      slots: {
        message: MESSAGE,
        retryable: true,
        workflow_id: WORKFLOW_ID,
        cause: CONNECTION_FAILURE,
      },
    });

    expectConverted(value, {
      shown: { kind: "databaseOperationFailed", code: "DATABASE_OPERATION_FAILED", retryable: true, workflowId: WORKFLOW_ID },
      hidden: ["message", "cause"],
      message: MESSAGE,
      cause: CONNECTION_FAILURE,
    });
  });

  test("WorkflowHandleStoredDataInvalid", () => {
    const value = handleCoreErrorFromNative({
      class: "WorkflowHandleStoredDataInvalid",
      slots: { message: MESSAGE, workflow_id: WORKFLOW_ID, cause: CONTRACT_VIOLATION },
    });

    expectConverted(value, {
      shown: { kind: "storedDataInvalid", code: "STORED_DATA_INVALID", retryable: false, workflowId: WORKFLOW_ID },
      hidden: ["message", "cause"],
      message: MESSAGE,
      cause: CONTRACT_VIOLATION,
    });
  });

  test("WorkflowHandleResumeRecoveryFailed", () => {
    const value = handleCoreErrorFromNative({
      class: "WorkflowHandleResumeRecoveryFailed",
      slots: { message: MESSAGE, workflow_id: WORKFLOW_ID, cause: SERVER_FAILURE },
    });

    expectConverted(value, {
      shown: { kind: "resumeRecoveryFailed", code: "RESUME_RECOVERY_FAILED", retryable: false, workflowId: WORKFLOW_ID },
      hidden: ["message", "cause"],
      message: MESSAGE,
      cause: SERVER_FAILURE,
    });
  });

  test("the class that only a blocking form gives is not a class of this build", () => {
    expect(() =>
      handleCoreErrorFromNative(JSON.parse('{"class":"WorkflowHandleAsyncContext","slots":{}}')),
    ).toThrow("handleCoreErrorFromNative: unknown class name WorkflowHandleAsyncContext");
  });
});
