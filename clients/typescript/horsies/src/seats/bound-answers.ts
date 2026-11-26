// The answer to a seat operation that comes past the bound of the operations that wait in its isolate. Nothing reaches
// the main thread, so nothing is written; each operation fails with the transient failure that its own type has:
//
// - `enqueue`: `ENQUEUE_FAILED`, retryable, with the task id and the payload of the request. A resend of the same task
//   id (`retrySend`, or the resend of `resendOnTransientErr`) is safe: no row exists.
// - `taskInfo`: the database failure of a task read, `TASK_INFO_QUERY_FAILED`, retryable.
// - `prepareSend`: its type has no transient failure (it computes the facts of a send and does no I/O). It gets the
//   refusal that every core call of a process gets past the bound of its calls in flight: `TOO_MANY_PENDING_CALLS`.
//
// The records have the forms that the addon gives (`RawCatalogError`), so the area's own converters read them.

import type { SendsSeatOperation, SendsSeatReply } from "../app/core-access.ts";
import type { DeadlineFact, DelayFact, EnqueueRequest, OptionalText, RetentionRecord } from "../native/sends/addon.ts";
import type { ExceptionRecord, RawCatalogError, RawRecordField, RawRecordValue } from "../native/values/records.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { SeatAnswer, SeatRequest } from "./protocol.ts";

/** The subject of the failures that the relay of a seat isolate gives. */
const RELAY_SUBJECT = "seat relay";

/** The answer to `request`, which came when `bound` operations waited in the isolate already. Pure. */
export function boundAnswer(request: SeatRequest, bound: number): SeatAnswer {
  const area = request.area;

  switch (area) {
    case "sends":
      return sendsBoundAnswer(request.operation, bound);
    case "workflows":
      return {
        kind: "refused",
        refusal: { code: "TOO_MANY_PENDING_CALLS", subject: `${RELAY_SUBJECT}: ${request.operation.kind}`, bound },
      };
    default:
      return unreachable(area, "boundAnswer");
  }
}

/** The answer of the `sends` area to `operation` past the bound. Pure. */
function sendsBoundAnswer(operation: SendsSeatOperation, bound: number): SeatAnswer {
  const message = `the seat isolate holds ${String(bound)} operations that wait for the main thread of its child, the most it may hold; the operation was not run`;

  switch (operation.kind) {
    case "prepareSend":
      return {
        kind: "refused",
        refusal: { code: "TOO_MANY_PENDING_CALLS", subject: `${RELAY_SUBJECT}: prepareSend`, bound },
      };
    case "enqueue":
      return sendsReply({
        kind: "enqueue",
        outcome: { kind: "failed", error: enqueueFailed(operation.request, `${message}; nothing was written`) },
      });
    case "taskInfo":
      return sendsReply({ kind: "taskInfo", read: { kind: "failed", error: taskInfoFailed(message) } });
    default:
      return unreachable(operation, "sendsBoundAnswer");
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
