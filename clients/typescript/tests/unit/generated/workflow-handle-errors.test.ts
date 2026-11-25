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
  test("HandleWorkflowNotFound", () => {
    const value = handleCoreErrorFromNative({
      class: "HandleWorkflowNotFound",
      slots: { message: MESSAGE, workflow_id: WORKFLOW_ID },
    });

    expectConverted(value, {
      shown: { code: "WORKFLOW_NOT_FOUND", retryable: false, workflowId: WORKFLOW_ID },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("HandleDbOperationFailed", () => {
    const value = handleCoreErrorFromNative({
      class: "HandleDbOperationFailed",
      slots: {
        message: MESSAGE,
        retryable: true,
        workflow_id: WORKFLOW_ID,
        exception: CONNECTION_FAILURE,
      },
    });

    expectConverted(value, {
      shown: { code: "DB_OPERATION_FAILED", retryable: true, workflowId: WORKFLOW_ID },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: CONNECTION_FAILURE,
    });
  });

  test("HandleStoredDataInvalid", () => {
    const value = handleCoreErrorFromNative({
      class: "HandleStoredDataInvalid",
      slots: { message: MESSAGE, workflow_id: WORKFLOW_ID, exception: CONTRACT_VIOLATION },
    });

    expectConverted(value, {
      shown: { code: "STORED_DATA_INVALID", retryable: false, workflowId: WORKFLOW_ID },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: CONTRACT_VIOLATION,
    });
  });

  test("HandleResumeRecoveryFailed", () => {
    const value = handleCoreErrorFromNative({
      class: "HandleResumeRecoveryFailed",
      slots: { message: MESSAGE, workflow_id: WORKFLOW_ID, exception: SERVER_FAILURE },
    });

    expectConverted(value, {
      shown: { code: "RESUME_RECOVERY_FAILED", retryable: false, workflowId: WORKFLOW_ID },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: SERVER_FAILURE,
    });
  });

  test("the class that only a blocking form gives is not a class of this build", () => {
    expect(() =>
      handleCoreErrorFromNative(JSON.parse('{"class":"HandleAsyncContext","slots":{}}')),
    ).toThrow("handleCoreErrorFromNative: unknown class name HandleAsyncContext");
  });
});
