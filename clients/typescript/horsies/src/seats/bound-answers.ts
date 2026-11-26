// How the relay of a seat isolate sends each operation, and the answer to an operation that comes past a bound of the
// relay. A bound that is hit is an expected failure: the operation is not sent, so nothing is read or written, and it
// gets the retryable failure that its own type already has, with a cause that names the bound and its value:
//
// - An operation that waits for I/O of the main thread (an enqueue, a task read, a facade call) takes a place of the
//   in-flight bound and of the waiting bound (`opsWaiting`, the core's `MAX_RUN_WORK`):
//   - `enqueue`: `ENQUEUE_FAILED`, retryable, with the task id and the payload of the request. A resend of the same task
//     id (`retrySend`, or the resend of `resendOnTransientErr`) is safe: no row exists;
//   - `taskInfo`: `TASK_INFO_QUERY_FAILED`, retryable (a read has no send failure);
//   - a facade call: the retryable database failure of its own type (`monitoring/seat-calls.ts`).
// - The prepare of a send does no I/O: the main thread computes the facts of a send at once. It goes at once and takes
//   no place of the in-flight bound, but the prepares that wait for their answer have their own bound of the same value
//   (`opsWaiting`). A prepare past it is refused with `VALIDATION_FAILED`, whose cause (`preconditionFailed`) is
//   retryable: the send failure `ENQUEUE_FAILED` needs the task id and the payload that the prepare makes, and the task
//   send catalog has no other retryable failure. Nothing is written.
//
// The records have the forms that the addon gives (`RawCatalogError`), so the area's own converters read them.

import type { SendsSeatOperation, SendsSeatReply } from "../app/core-access.ts";
import { facadeBoundAnswer } from "../monitoring/seat-calls.ts";
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
  | { readonly kind: "answeredAtOnce"; readonly boundAnswer: (bound: number) => SeatAnswer }
  | { readonly kind: "waitsForIo"; readonly boundAnswer: (bound: number) => SeatAnswer };

/** The lane of `request`. Pure. */
export function operationLane(request: SeatRequest): OperationLane {
  const area = request.area;

  switch (area) {
    case "sends":
      return sendsLane(request.operation);
    case "facades": {
      const operation = request.operation;

      return {
        kind: "waitsForIo",
        boundAnswer: (bound) => ({
          kind: "reply",
          reply: { area: "facades", reply: facadeBoundAnswer(operation, boundMessage("operations", bound)) },
        }),
      };
    }

    default:
      return unreachable(area, "operationLane");
  }
}

/**
 * The text of a failure at a bound of the relay: what waits (`operations` or `prepares`), the bound and its value (the
 * core's `MAX_RUN_WORK`), and that the operation did not run. Pure.
 */
export function boundMessage(waiting: "operations" | "prepares", bound: number): string {
  return `the relay of this seat isolate holds ${String(bound)} ${waiting} that wait for the main thread of its child, its bound (MAX_RUN_WORK = ${String(bound)}); the operation was not run`;
}

/** The lane of the `sends` operation `operation`. Pure. */
function sendsLane(operation: SendsSeatOperation): OperationLane {
  switch (operation.kind) {
    case "prepareSend":
      return {
        kind: "answeredAtOnce",
        boundAnswer: (bound) =>
          sendsReply({
            kind: "prepareSend",
            prepared: { kind: "refused", error: prepareRefused(`${boundMessage("prepares", bound)}; nothing was written`) },
          }),
      };
    case "enqueue":
      return {
        kind: "waitsForIo",
        boundAnswer: (bound) =>
          sendsReply({
            kind: "enqueue",
            outcome: { kind: "failed", error: enqueueFailed(operation.request, `${boundMessage("operations", bound)}; nothing was written`) },
          }),
      };
    case "taskInfo":
      return {
        kind: "waitsForIo",
        boundAnswer: (bound) => sendsReply({ kind: "taskInfo", read: { kind: "failed", error: taskInfoFailed(boundMessage("operations", bound)) } }),
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

/** `VALIDATION_FAILED` with a retryable precondition cause, in the form of the task send catalog. Pure. */
export function prepareRefused(message: string): RawCatalogError {
  return {
    class: "TaskSendValidationFailed",
    slots: [
      { name: "message", value: { kind: "text", text: message } },
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
