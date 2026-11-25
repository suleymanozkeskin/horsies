import { describe, expect, test } from "bun:test";

import {
  monitoringQueryCoreErrorFromNative,
  taskActionCoreErrorFromNative,
} from "../../../horsies/src/_catalog/monitoring_errors.ts";
import { TaskStatus } from "../../../horsies/src/_catalog/status.ts";
import {
  CONTRACT_VIOLATION,
  expectConverted,
  HISTORY_CONTRACT,
  SERVER_FAILURE,
  TASK_ID,
} from "./catalog_errors.ts";

const MESSAGE = "the monitoring read failed";

describe("monitoringQueryCoreErrorFromNative", () => {
  test("MonitoringQueryDbOperationFailed", () => {
    const value = monitoringQueryCoreErrorFromNative({
      class: "MonitoringQueryDbOperationFailed",
      slots: { message: MESSAGE, retryable: true, exception: SERVER_FAILURE },
    });

    expectConverted(value, {
      shown: { code: "DB_OPERATION_FAILED", retryable: true },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: SERVER_FAILURE,
    });
  });

  test("MonitoringQueryHistoryContractViolation", () => {
    const value = monitoringQueryCoreErrorFromNative({
      class: "MonitoringQueryHistoryContractViolation",
      slots: { message: MESSAGE, exception: HISTORY_CONTRACT },
    });

    expectConverted(value, {
      shown: { code: "HISTORY_CONTRACT_VIOLATION", retryable: false },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: HISTORY_CONTRACT,
    });
  });

  test("a class name that this build does not know throws", () => {
    expect(() =>
      monitoringQueryCoreErrorFromNative(JSON.parse('{"class":"TaskActionNotFound","slots":{}}')),
    ).toThrow("monitoringQueryCoreErrorFromNative: unknown class name TaskActionNotFound");
  });
});

describe("taskActionCoreErrorFromNative", () => {
  test("TaskActionNotFound", () => {
    const value = taskActionCoreErrorFromNative({
      class: "TaskActionNotFound",
      slots: { message: MESSAGE, task_id: TASK_ID },
    });

    expectConverted(value, {
      shown: { code: "TASK_NOT_FOUND", retryable: false, taskId: TASK_ID },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("TaskActionNotCancellable keeps the current status", () => {
    const value = taskActionCoreErrorFromNative({
      class: "TaskActionNotCancellable",
      slots: { message: MESSAGE, task_id: TASK_ID, current_status: TaskStatus.COMPLETED },
    });

    expectConverted(value, {
      shown: {
        code: "TASK_NOT_CANCELLABLE",
        retryable: false,
        taskId: TASK_ID,
        currentStatus: TaskStatus.COMPLETED,
      },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("TaskActionWorkflowTask", () => {
    const value = taskActionCoreErrorFromNative({
      class: "TaskActionWorkflowTask",
      slots: { message: MESSAGE, task_id: TASK_ID, current_status: TaskStatus.RUNNING },
    });

    expectConverted(value, {
      shown: {
        code: "TASK_IS_WORKFLOW_TASK",
        retryable: false,
        taskId: TASK_ID,
        currentStatus: TaskStatus.RUNNING,
      },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("TaskActionDbOperationFailed", () => {
    const value = taskActionCoreErrorFromNative({
      class: "TaskActionDbOperationFailed",
      slots: { message: MESSAGE, retryable: false, task_id: TASK_ID, exception: SERVER_FAILURE },
    });

    expectConverted(value, {
      shown: { code: "DB_OPERATION_FAILED", retryable: false, taskId: TASK_ID },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: SERVER_FAILURE,
    });
  });

  test("TaskActionStoredDataInvalid", () => {
    const value = taskActionCoreErrorFromNative({
      class: "TaskActionStoredDataInvalid",
      slots: { message: MESSAGE, task_id: TASK_ID, exception: CONTRACT_VIOLATION },
    });

    expectConverted(value, {
      shown: { code: "STORED_DATA_INVALID", retryable: false, taskId: TASK_ID },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: CONTRACT_VIOLATION,
    });
  });

  test("a class name that this build does not know throws", () => {
    expect(() =>
      taskActionCoreErrorFromNative(JSON.parse('{"class":"TaskActionGone","slots":{}}')),
    ).toThrow("taskActionCoreErrorFromNative: unknown class name TaskActionGone");
  });
});
