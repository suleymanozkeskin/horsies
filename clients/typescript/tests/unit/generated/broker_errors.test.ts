import { describe, expect, test } from "bun:test";

import { brokerCoreErrorFromNative } from "../../../horsies/src/_catalog/broker_errors.ts";
import type { BrokerCoreEnqueueError } from "../../../horsies/src/_catalog/broker_errors.ts";
import {
  CONNECTION_FAILURE,
  expectConverted,
  JSON_UNREADABLE,
  NO_CAUSE,
  OTHER_TASK_ID,
  RULE_REFUSED,
  SERVER_FAILURE,
} from "./catalog_errors.ts";

const MESSAGE = "the broker refused the call";

describe("brokerCoreErrorFromNative", () => {
  test("BrokerOperationFailed keeps the code of its subset", () => {
    const value = brokerCoreErrorFromNative({
      class: "BrokerOperationFailed",
      slots: { code: "CLOSE_FAILED", message: MESSAGE, retryable: true, exception: SERVER_FAILURE },
    });

    expectConverted(value, {
      shown: { code: "CLOSE_FAILED", retryable: true },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: SERVER_FAILURE,
    });
  });

  test("BrokerEnqueueFailed", () => {
    const value = brokerCoreErrorFromNative({
      class: "BrokerEnqueueFailed",
      slots: { message: MESSAGE, retryable: true, exception: CONNECTION_FAILURE },
    });

    expectConverted(value, {
      shown: { code: "ENQUEUE_FAILED", retryable: true },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: CONNECTION_FAILURE,
    });
  });

  test("BrokerPayloadMismatch never retries", () => {
    const value = brokerCoreErrorFromNative({
      class: "BrokerPayloadMismatch",
      slots: { message: MESSAGE },
    });

    expectConverted(value, {
      shown: { code: "PAYLOAD_MISMATCH", retryable: false },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("BrokerIdempotencyKeyConflict names the owner task", () => {
    const value = brokerCoreErrorFromNative({
      class: "BrokerIdempotencyKeyConflict",
      slots: { message: MESSAGE, owner_task_id: OTHER_TASK_ID },
    });

    expectConverted(value, {
      shown: { code: "IDEMPOTENCY_KEY_CONFLICT", retryable: false, ownerTaskId: OTHER_TASK_ID },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("BrokerInvalidJsonPayload", () => {
    const value = brokerCoreErrorFromNative({
      class: "BrokerInvalidJsonPayload",
      slots: { message: MESSAGE, exception: JSON_UNREADABLE },
    });

    expectConverted(value, {
      shown: { code: "INVALID_JSON_PAYLOAD", retryable: false },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: JSON_UNREADABLE,
    });
  });

  test("BrokerInvalidTaskOptions", () => {
    const value = brokerCoreErrorFromNative({
      class: "BrokerInvalidTaskOptions",
      slots: { message: MESSAGE, exception: NO_CAUSE },
    });

    expectConverted(value, {
      shown: { code: "INVALID_TASK_OPTIONS", retryable: false },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: NO_CAUSE,
    });
  });

  test("BrokerInvalidInput", () => {
    const value = brokerCoreErrorFromNative({
      class: "BrokerInvalidInput",
      slots: { message: MESSAGE, exception: RULE_REFUSED },
    });

    expectConverted(value, {
      shown: { code: "INVALID_INPUT", retryable: false },
      hidden: ["message", "exception"],
      message: MESSAGE,
      cause: RULE_REFUSED,
    });
  });

  test("BrokerListenerDisabled", () => {
    const value = brokerCoreErrorFromNative({
      class: "BrokerListenerDisabled",
      slots: { message: MESSAGE },
    });

    expectConverted(value, {
      shown: { code: "LISTENER_DISABLED", retryable: false },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("BrokerWaitInsideTask", () => {
    const value = brokerCoreErrorFromNative({
      class: "BrokerWaitInsideTask",
      slots: { message: MESSAGE },
    });

    expectConverted(value, {
      shown: { code: "WAIT_INSIDE_TASK", retryable: false },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("a class name that this build does not know throws", () => {
    expect(() =>
      brokerCoreErrorFromNative(JSON.parse('{"class":"BrokerAsyncContext","slots":{"message":"m"}}')),
    ).toThrow("brokerCoreErrorFromNative: unknown class name BrokerAsyncContext");
  });

  test("an enqueue error narrows on its code", () => {
    const value = brokerCoreErrorFromNative({
      class: "BrokerInvalidInput",
      slots: { message: MESSAGE, exception: RULE_REFUSED },
    });

    expect(value.code).toBe("INVALID_INPUT");

    if (value.code === "INVALID_INPUT") {
      const enqueueError: BrokerCoreEnqueueError = value;

      expect(enqueueError.retryable).toBe(false);
    }
  });
});
