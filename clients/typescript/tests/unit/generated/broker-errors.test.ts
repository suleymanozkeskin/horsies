import { describe, expect, test } from "bun:test";

import { databaseCoreErrorFromNative } from "../../../horsies/src/_catalog/broker_errors.ts";
import type { DatabaseCoreEnqueueError } from "../../../horsies/src/_catalog/broker_errors.ts";
import {
  CONNECTION_FAILURE,
  expectConverted,
  JSON_UNREADABLE,
  NO_CAUSE,
  OTHER_TASK_ID,
  RULE_REFUSED,
  SERVER_FAILURE,
} from "./support/catalog-errors.ts";

const MESSAGE = "the broker refused the call";

describe("databaseCoreErrorFromNative", () => {
  test("DatabaseOperationFailed keeps the code of its subset", () => {
    const value = databaseCoreErrorFromNative({
      class: "DatabaseOperationFailed",
      slots: { code: "CLOSE_FAILED", message: MESSAGE, retryable: true, cause: SERVER_FAILURE },
    });

    expectConverted(value, {
      shown: { kind: "closeFailed", code: "CLOSE_FAILED", retryable: true },
      hidden: ["message", "cause"],
      message: MESSAGE,
      cause: SERVER_FAILURE,
    });
  });

  test("DatabaseEnqueueFailed", () => {
    const value = databaseCoreErrorFromNative({
      class: "DatabaseEnqueueFailed",
      slots: { message: MESSAGE, retryable: true, cause: CONNECTION_FAILURE },
    });

    expectConverted(value, {
      shown: { kind: "enqueueFailed", code: "ENQUEUE_FAILED", retryable: true },
      hidden: ["message", "cause"],
      message: MESSAGE,
      cause: CONNECTION_FAILURE,
    });
  });

  test("DatabasePayloadMismatch never retries", () => {
    const value = databaseCoreErrorFromNative({
      class: "DatabasePayloadMismatch",
      slots: { message: MESSAGE },
    });

    expectConverted(value, {
      shown: { kind: "payloadMismatch", code: "PAYLOAD_MISMATCH", retryable: false },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("DatabaseIdempotencyKeyConflict names the owner task", () => {
    const value = databaseCoreErrorFromNative({
      class: "DatabaseIdempotencyKeyConflict",
      slots: { message: MESSAGE, owner_task_id: OTHER_TASK_ID },
    });

    expectConverted(value, {
      shown: { kind: "idempotencyKeyConflict", code: "IDEMPOTENCY_KEY_CONFLICT", retryable: false, ownerTaskId: OTHER_TASK_ID },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("DatabaseInvalidJsonPayload", () => {
    const value = databaseCoreErrorFromNative({
      class: "DatabaseInvalidJsonPayload",
      slots: { message: MESSAGE, cause: JSON_UNREADABLE },
    });

    expectConverted(value, {
      shown: { kind: "invalidJsonPayload", code: "INVALID_JSON_PAYLOAD", retryable: false },
      hidden: ["message", "cause"],
      message: MESSAGE,
      cause: JSON_UNREADABLE,
    });
  });

  test("DatabaseInvalidTaskOptions", () => {
    const value = databaseCoreErrorFromNative({
      class: "DatabaseInvalidTaskOptions",
      slots: { message: MESSAGE, cause: NO_CAUSE },
    });

    expectConverted(value, {
      shown: { kind: "invalidTaskOptions", code: "INVALID_TASK_OPTIONS", retryable: false },
      hidden: ["message", "cause"],
      message: MESSAGE,
      cause: NO_CAUSE,
    });
  });

  test("DatabaseInvalidInput", () => {
    const value = databaseCoreErrorFromNative({
      class: "DatabaseInvalidInput",
      slots: { message: MESSAGE, cause: RULE_REFUSED },
    });

    expectConverted(value, {
      shown: { kind: "invalidInput", code: "INVALID_INPUT", retryable: false },
      hidden: ["message", "cause"],
      message: MESSAGE,
      cause: RULE_REFUSED,
    });
  });

  test("DatabaseListenerDisabled", () => {
    const value = databaseCoreErrorFromNative({
      class: "DatabaseListenerDisabled",
      slots: { message: MESSAGE },
    });

    expectConverted(value, {
      shown: { kind: "listenerDisabled", code: "LISTENER_DISABLED", retryable: false },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("DatabaseWaitInsideTask", () => {
    const value = databaseCoreErrorFromNative({
      class: "DatabaseWaitInsideTask",
      slots: { message: MESSAGE },
    });

    expectConverted(value, {
      shown: { kind: "waitInsideTask", code: "WAIT_INSIDE_TASK", retryable: false },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("a class name that this build does not know throws", () => {
    expect(() =>
      databaseCoreErrorFromNative(JSON.parse('{"class":"DatabaseAsyncContext","slots":{"message":"m"}}')),
    ).toThrow("databaseCoreErrorFromNative: unknown class name DatabaseAsyncContext");
  });

  test("an enqueue error narrows on its code", () => {
    const value = databaseCoreErrorFromNative({
      class: "DatabaseInvalidInput",
      slots: { message: MESSAGE, cause: RULE_REFUSED },
    });

    expect(value.code).toBe("INVALID_INPUT");

    if (value.code === "INVALID_INPUT") {
      const enqueueError: DatabaseCoreEnqueueError = value;

      expect(enqueueError.retryable).toBe(false);
    }
  });
});
