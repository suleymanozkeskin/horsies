import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { taskSendCoreErrorFromNative } from "../../../horsies/src/_catalog/task_send_errors.ts";

/** 2023-11-14T22:13:20.123456Z in epoch microseconds. */
const SENT_AT_MICROSECONDS = 1_700_000_000_123_456n;

void describe("taskSendCoreErrorFromNative on this runtime", () => {
  void test("the payload instants keep their microseconds and the hidden slots are not enumerable", () => {
    const value = taskSendCoreErrorFromNative({
      class: "TaskSendEnqueueFailed",
      slots: {
        task_id: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b",
        payload: {
          task_name: "report",
          queue_name: "default",
          priority: 50,
          input_json: null,
          sent_at: SENT_AT_MICROSECONDS,
          good_until: SENT_AT_MICROSECONDS + 1n,
          enqueue_delay_seconds: null,
          task_options: null,
          idempotency_key: null,
          retention_class_key: null,
        },
        message: "connection refused",
        retryable: true,
        cause: { kind: "noCause" },
      },
    });

    assert.equal(value.code, "ENQUEUE_FAILED");
    assert.deepEqual(Object.keys(value), ["kind", "code", "taskId", "retryable"]);
    assert.equal(Object.isFrozen(value), true);
    assert.equal(value.message, "connection refused");

    const payload = "payload" in value ? value.payload : null;

    assert.equal(payload?.sentAt.toString(), "2023-11-14T22:13:20.123456Z");
    assert.equal(payload?.goodUntil?.epochNanoseconds, 1_700_000_000_123_457_000n);
    assert.deepEqual(structuredClone(value), {
      kind: "enqueueFailed",
      code: "ENQUEUE_FAILED",
      taskId: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b",
      retryable: true,
    });
  });
});
