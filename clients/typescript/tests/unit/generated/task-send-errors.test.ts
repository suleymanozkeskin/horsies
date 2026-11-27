import { describe, expect, test } from "bun:test";

import { DEFAULT_RETENTION_CLASS_KEY } from "../../../horsies/src/_catalog/retention_class.ts";
import {
  taskSendCoreErrorFromNative,
  taskSendPayloadFromNative,
} from "../../../horsies/src/_catalog/task_send_errors.ts";
import type {
  NativeTaskSendPayload,
  TaskSendCoreError,
  TaskSendPayload,
} from "../../../horsies/src/_catalog/task_send_errors.ts";
import {
  CONNECTION_FAILURE,
  expectConverted,
  NO_CAUSE,
  OTHER_TASK_ID,
  RULE_REFUSED,
  TASK_ID,
} from "./support/catalog-errors.ts";

const MESSAGE = "the send failed";

/** 2023-11-14T22:13:20.123456Z in epoch microseconds. */
const SENT_AT_MICROSECONDS = 1_700_000_000_123_456n;

const NATIVE_PAYLOAD: NativeTaskSendPayload = {
  task_name: "report",
  queue_name: "default",
  priority: 50,
  input_json: '{"x":1}',
  sent_at: SENT_AT_MICROSECONDS,
  good_until: null,
  enqueue_delay_seconds: 30,
  task_options: null,
  idempotency_key: "report:1",
  retention_class_key: DEFAULT_RETENTION_CLASS_KEY,
};

/** The facts of a payload with each instant as epoch nanoseconds. */
function payloadFacts(payload: TaskSendPayload): ReadonlyArray<string | number | bigint | null> {
  return [
    payload.taskName,
    payload.queueName,
    payload.priority,
    payload.inputJson,
    payload.sentAt.epochNanoseconds,
    payload.goodUntil === null ? null : payload.goodUntil.epochNanoseconds,
    payload.enqueueDelaySeconds,
    payload.taskOptions,
    payload.idempotencyKey,
    payload.retentionClassKey,
  ];
}

/** The payload of `value`, or `null` for a class without one. */
function payloadOf(value: TaskSendCoreError): TaskSendPayload | null {
  return "payload" in value ? value.payload : null;
}

describe("taskSendPayloadFromNative", () => {
  test("converts the names and the instants and freezes the record", () => {
    const payload = taskSendPayloadFromNative(NATIVE_PAYLOAD);

    expect(payloadFacts(payload)).toEqual([
      "report",
      "default",
      50,
      '{"x":1}',
      1_700_000_000_123_456_000n,
      null,
      30,
      null,
      "report:1",
      "standard_30d",
    ]);
    expect(Object.isFrozen(payload)).toBe(true);
  });

  test("an optional instant converts when it is set", () => {
    const payload = taskSendPayloadFromNative({
      ...NATIVE_PAYLOAD,
      good_until: SENT_AT_MICROSECONDS + 1n,
      retention_class_key: null,
    });

    expect(payload.goodUntil?.epochNanoseconds).toBe(1_700_000_000_123_457_000n);
    expect(payload.retentionClassKey).toBeNull();
  });
});

describe("taskSendCoreErrorFromNative", () => {
  test("TaskSendSuppressed", () => {
    const value = taskSendCoreErrorFromNative({
      class: "TaskSendSuppressed",
      slots: { message: MESSAGE },
    });

    expectConverted(value, {
      shown: { kind: "suppressed", code: "SEND_SUPPRESSED" },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("TaskSendValidationFailed", () => {
    const value = taskSendCoreErrorFromNative({
      class: "TaskSendValidationFailed",
      slots: { message: MESSAGE, cause: RULE_REFUSED },
    });

    expectConverted(value, {
      shown: { kind: "validationFailed", code: "VALIDATION_FAILED" },
      hidden: ["message", "cause"],
      message: MESSAGE,
      cause: RULE_REFUSED,
    });
  });

  test("TaskSendEnqueueFailed hides its payload", () => {
    const value = taskSendCoreErrorFromNative({
      class: "TaskSendEnqueueFailed",
      slots: {
        task_id: TASK_ID,
        payload: NATIVE_PAYLOAD,
        message: MESSAGE,
        retryable: true,
        cause: CONNECTION_FAILURE,
      },
    });

    expectConverted(value, {
      shown: { kind: "enqueueFailed", code: "ENQUEUE_FAILED", taskId: TASK_ID, retryable: true },
      hidden: ["payload", "message", "cause"],
      message: MESSAGE,
      cause: CONNECTION_FAILURE,
    });

    const payload = payloadOf(value);

    expect(payload === null ? null : payloadFacts(payload)).toEqual(
      payloadFacts(taskSendPayloadFromNative(NATIVE_PAYLOAD)),
    );
    expect(JSON.stringify(value)).toBe(
      `{"kind":"enqueueFailed","code":"ENQUEUE_FAILED","taskId":"${TASK_ID}","retryable":true}`,
    );
  });

  test("TaskSendPayloadMismatch", () => {
    const value = taskSendCoreErrorFromNative({
      class: "TaskSendPayloadMismatch",
      slots: { task_id: TASK_ID, payload: NATIVE_PAYLOAD, message: MESSAGE },
    });

    expectConverted(value, {
      shown: { kind: "payloadMismatch", code: "PAYLOAD_MISMATCH", taskId: TASK_ID },
      hidden: ["payload", "message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("TaskSendPayloadTooLarge keeps its sizes as bigint", () => {
    const value = taskSendCoreErrorFromNative({
      class: "TaskSendPayloadTooLarge",
      slots: { message: MESSAGE, size_bytes: 18_446_744_073_709_551_615n, limit_bytes: 1_048_576n },
    });

    expectConverted(value, {
      shown: {
        kind: "payloadTooLarge",
        code: "PAYLOAD_TOO_LARGE",
        sizeBytes: 18_446_744_073_709_551_615n,
        limitBytes: 1_048_576n,
      },
      hidden: ["message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("TaskSendIdempotencyKeyConflict", () => {
    const value = taskSendCoreErrorFromNative({
      class: "TaskSendIdempotencyKeyConflict",
      slots: {
        task_id: TASK_ID,
        payload: NATIVE_PAYLOAD,
        message: MESSAGE,
        owner_task_id: OTHER_TASK_ID,
      },
    });

    expectConverted(value, {
      shown: { kind: "idempotencyKeyConflict", code: "IDEMPOTENCY_KEY_CONFLICT", retryable: false, taskId: TASK_ID, ownerTaskId: OTHER_TASK_ID },
      hidden: ["payload", "message"],
      message: MESSAGE,
      cause: null,
    });
  });

  test("a no-cause value stays a no-cause value", () => {
    const value = taskSendCoreErrorFromNative({
      class: "TaskSendValidationFailed",
      slots: { message: MESSAGE, cause: NO_CAUSE },
    });

    expectConverted(value, {
      shown: { kind: "validationFailed", code: "VALIDATION_FAILED" },
      hidden: ["message", "cause"],
      message: MESSAGE,
      cause: NO_CAUSE,
    });
  });

  test("the class that only a blocking form gives is not a class of this build", () => {
    expect(() =>
      taskSendCoreErrorFromNative(
        JSON.parse('{"class":"TaskSendAsyncContext","slots":{"message":"m"}}'),
      ),
    ).toThrow("taskSendCoreErrorFromNative: unknown class name TaskSendAsyncContext");
  });
});
