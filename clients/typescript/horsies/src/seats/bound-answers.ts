// How the relay of a seat isolate sends each operation, and the answer to an operation that comes past the bound of
// the operations that wait in its isolate. A bound that is hit is an expected failure: the operation is not sent, so
// nothing is written, and it gets the retryable failure that its own type already has, with a cause that names the
// relay bound (`MAX_SEAT_OPS_WAITING`) and its value:
//
// - `enqueue` waits for the I/O of the main thread: past the bound it gets `ENQUEUE_FAILED`, retryable, with the task
//   id and the payload of the request. A resend of the same task id (`retrySend`, or the resend of
//   `resendOnTransientErr`) is safe: no row exists.
// - `taskInfo` waits for the I/O of the main thread: past the bound it gets the retryable failure of a task read,
//   `TASK_INFO_QUERY_FAILED` (a read has no send failure).
// - `prepareSend` does no I/O: the main thread computes the facts of a send at once and holds nothing. It goes at once,
//   takes no place of the bounds, and is never refused at the bound. Its type has no failure that the relay could fill
//   there (`ENQUEUE_FAILED` needs the task id and the payload that the prepare makes); the send meets the bound at its
//   enqueue and gets `ENQUEUE_FAILED` with both. The prepares that wait are the sends that the body started, held in
//   the isolate's heap under its memory cap.
//
// The records have the forms that the addon gives (`RawCatalogError`), so the area's own converters read them.

import type { SendsSeatOperation, SendsSeatReply } from "../app/core-access.ts";
import type { DeadlineFact, DelayFact, EnqueueRequest, OptionalText, RetentionRecord } from "../native/sends/addon.ts";
import type { ExceptionRecord, RawCatalogError, RawRecordField, RawRecordValue } from "../native/values/records.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { SeatAnswer, SeatRequest } from "./protocol.ts";

/** The subject of the failures that the relay of a seat isolate gives. */
const RELAY_SUBJECT = "seat relay";

/**
 * How the relay sends one operation: at once (the main thread answers it with no I/O), or under the bounds of the
 * operations that wait for the main thread, with the answer that it gets past them.
 */
export type OperationLane =
  | { readonly kind: "answeredAtOnce" }
  | { readonly kind: "waitsForIo"; readonly boundAnswer: (bound: number) => SeatAnswer };

const ANSWERED_AT_ONCE: OperationLane = Object.freeze({ kind: "answeredAtOnce" });

/** The lane of `request`. Pure. */
export function operationLane(request: SeatRequest): OperationLane {
  const area = request.area;

  switch (area) {
    case "sends":
      return sendsLane(request.operation);
    default:
      return unreachable(area, "operationLane");
  }
}

/** The text of a failure at the bound: the relay bound, its value, and that the operation did not run. Pure. */
export function boundMessage(bound: number): string {
  return `the relay of this seat isolate holds ${String(bound)} operations that wait for the main thread of its child, its bound (MAX_SEAT_OPS_WAITING = ${String(bound)}); the operation was not run`;
}

/** The lane of the `sends` operation `operation`. Pure. */
function sendsLane(operation: SendsSeatOperation): OperationLane {
  switch (operation.kind) {
    case "prepareSend":
      return ANSWERED_AT_ONCE;
    case "enqueue":
      return {
        kind: "waitsForIo",
        boundAnswer: (bound) =>
          sendsReply({
            kind: "enqueue",
            outcome: { kind: "failed", error: enqueueFailed(operation.request, `${boundMessage(bound)}; nothing was written`) },
          }),
      };
    case "taskInfo":
      return {
        kind: "waitsForIo",
        boundAnswer: (bound) => sendsReply({ kind: "taskInfo", read: { kind: "failed", error: taskInfoFailed(boundMessage(bound)) } }),
      };
    default:
      return unreachable(operation, "sendsLane");
  }
}

function sendsReply(reply: SendsSeatReply): SeatAnswer {
  return { kind: "reply", reply: { area: "sends", reply } };
}

/** The cause of a failure at the bound: a precondition of the operation that can clear. Pure. */
function boundCause(message: string): ExceptionRecord {
  return { kind: "coreFailure", cause: { kind: "preconditionFailed", subject: RELAY_SUBJECT, message, retryable: true } };
}

/** `ENQUEUE_FAILED` of `request`, retryable, in the form of the task send catalog. Pure. */
export function enqueueFailed(request: EnqueueRequest, message: string): RawCatalogError {
  return {
    class: "TaskSendEnqueueFailed",
    slots: [
      { name: "task_id", value: { kind: "uuid", text: request.taskId } },
      { name: "payload", value: { kind: "sendPayload", fields: payloadFields(request) } },
      { name: "message", value: { kind: "text", text: message } },
      { name: "retryable", value: { kind: "flag", value: true } },
      { name: "exception", value: { kind: "exception", exception: boundCause(message) } },
    ],
  };
}

/** `TASK_INFO_QUERY_FAILED`, retryable, in the form of the broker catalog. Pure. */
export function taskInfoFailed(message: string): RawCatalogError {
  return {
    class: "BrokerOperationFailed",
    slots: [
      { name: "code", value: { kind: "codeMember", catalog: "BrokerOperationFailedCode", value: "TASK_INFO_QUERY_FAILED" } },
      { name: "message", value: { kind: "text", text: message } },
      { name: "retryable", value: { kind: "flag", value: true } },
      { name: "exception", value: { kind: "exception", exception: boundCause(message) } },
    ],
  };
}

/** The fields of the send payload of `request`, by the names of the task send catalog. Pure. */
function payloadFields(request: EnqueueRequest): RawRecordField[] {
  return [
    { name: "task_name", value: text(request.taskName) },
    { name: "queue_name", value: text(request.queueName) },
    { name: "priority", value: { kind: "integer", value: BigInt(request.priority) } },
    { name: "input_json", value: text(request.inputJson) },
    { name: "sent_at", value: { kind: "instant", epochMicros: request.sentAtMicros } },
    { name: "good_until", value: deadlineValue(request.deadline) },
    { name: "enqueue_delay_seconds", value: delayValue(request.delay) },
    { name: "task_options", value: optionalTextValue(request.taskOptions) },
    { name: "idempotency_key", value: optionalTextValue(request.idempotencyKey) },
    { name: "retention_class_key", value: retentionValue(request.retention) },
  ];
}

const ABSENT: RawRecordValue = Object.freeze({ kind: "absent" });

function text(value: string): RawRecordValue {
  return { kind: "text", text: value };
}

function deadlineValue(deadline: DeadlineFact): RawRecordValue {
  switch (deadline.kind) {
    case "noDeadline":
      return ABSENT;
    case "until":
      return { kind: "instant", epochMicros: deadline.epochMicros };
    default:
      return unreachable(deadline, "deadlineValue");
  }
}

function delayValue(delay: DelayFact): RawRecordValue {
  switch (delay.kind) {
    case "noDelay":
      return ABSENT;
    case "seconds":
      return { kind: "integer", value: BigInt(delay.seconds) };
    default:
      return unreachable(delay, "delayValue");
  }
}

function optionalTextValue(optional: OptionalText): RawRecordValue {
  switch (optional.kind) {
    case "none":
      return ABSENT;
    case "text":
      return text(optional.text);
    default:
      return unreachable(optional, "optionalTextValue");
  }
}

function retentionValue(retention: RetentionRecord): RawRecordValue {
  switch (retention.kind) {
    case "forever":
      return { kind: "retainForever" };
    case "classKey":
      return text(retention.key);
    default:
      return unreachable(retention, "retentionValue");
  }
}
