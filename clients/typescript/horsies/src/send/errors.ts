// The errors of a send: the task send catalog of the core, generated (`src/_catalog/task_send_errors.ts`), told apart
// by `code`. The two errors that `retrySend` takes carry the contract's types and hold the prepared payload in a
// member that is not enumerable, so `JSON.stringify` of an error never holds the input. A key conflict carries
// `retryable: true`, the core's retry rule of the conflict (`TaskSendError::retryable`): it clears when the key's
// reservation ends.

import type { CoreCause, NoCause } from "../_catalog/error_detail.ts";
import {
  type NativeTaskSendCoreError,
  type TaskSendCoreError,
  taskSendCoreErrorFromNative,
  type TaskSendEnqueueFailed,
  type TaskSendIdempotencyKeyConflict,
  type TaskSendPayloadMismatch,
  type TaskSendPayloadTooLarge,
  type TaskSendSuppressed,
  type TaskSendValidationFailed,
} from "../_catalog/task_send_errors.ts";
import type { RawCatalogError } from "../native/values/records.ts";
import type { SeatRelayFull } from "../seats/relay-full.ts";
import { nativeCatalogValue } from "./catalog-value.ts";

/** Type-only key of the types of a sent task. It does not exist at run time. */
export declare const sentTypes: unique symbol;

/** The output and code types of the task of a send error. */
export type SentTypes<O, E extends string> = { readonly output: O; readonly code: E };

/** A send error that carries the types of its task, for the handle that `retrySend` gives. */
export type WithSentTypes<V, O, E extends string> = V & { readonly [sentTypes]?: SentTypes<O, E> };

/**
 * The send errors that `retrySend` takes: the row may or may not exist (`ENQUEUE_FAILED`), or another task holds the
 * idempotency key (`IDEMPOTENCY_KEY_CONFLICT`; a retry after the key's reservation ends is accepted, a retry before
 * that gives the same error).
 */
export type RetryableSendError<O, E extends string> = WithSentTypes<
  TaskSendEnqueueFailed | (TaskSendIdempotencyKeyConflict & { readonly retryable: true }),
  O,
  E
>;

/** A send error of the task send catalog, with the fixed `retryable` of a key conflict, or the seat relay at its bound. */
export type SendFailure =
  | TaskSendSuppressed
  | TaskSendValidationFailed
  | TaskSendEnqueueFailed
  | TaskSendPayloadMismatch
  | TaskSendPayloadTooLarge
  | (TaskSendIdempotencyKeyConflict & { readonly retryable: true })
  | SeatRelayFull;

/** The error of a send, told apart by `code`. */
export type TaskSendError<O, E extends string> =
  | TaskSendSuppressed
  | TaskSendValidationFailed
  | TaskSendPayloadMismatch
  | TaskSendPayloadTooLarge
  | SeatRelayFull
  | RetryableSendError<O, E>;

/** The public send error of a raw record of the task send catalog. An unknown class throws (a build defect). */
export function sendErrorOfRaw(raw: RawCatalogError): SendFailure {
  // SAFETY: the bridge builds the slots of a task send class from the core's slot list of that class
  // (`ErrorClassValues`), the list from which the codegen renders `NativeTaskSendCoreError`; the generated converter
  // throws for a class name that it does not know.
  const error = taskSendCoreErrorFromNative(nativeCatalogValue(raw) as unknown as NativeTaskSendCoreError);

  return error.code === "IDEMPOTENCY_KEY_CONFLICT" ? retryableConflict(error) : error;
}

/** The key conflict with `retryable: true`; its other members keep their enumerability (the payload stays hidden). */
function retryableConflict(
  error: TaskSendIdempotencyKeyConflict,
): TaskSendIdempotencyKeyConflict & { readonly retryable: true } {
  const members = { ...Object.getOwnPropertyDescriptors(error), retryable: { value: true, enumerable: true } };

  // SAFETY: the object holds every member of the conflict, with the same descriptors, and `retryable: true`.
  return Object.freeze(
    Object.create(Object.prototype, members) as TaskSendIdempotencyKeyConflict & { readonly retryable: true },
  );
}

/** `SEND_SUPPRESSED`: the process does not send now. Nothing is written. Pure. */
export function suppressedError(message: string): TaskSendSuppressed {
  const error = taskSendCoreErrorFromNative({ class: "TaskSendSuppressed", slots: { message } });

  return error.code === "SEND_SUPPRESSED" ? error : wrongCode(error);
}

/** `VALIDATION_FAILED`: the send breaks a rule; nothing is written. Pure. */
export function validationFailed(message: string, exception: CoreCause | NoCause): TaskSendValidationFailed {
  const error = taskSendCoreErrorFromNative({ class: "TaskSendValidationFailed", slots: { message, exception } });

  return error.code === "VALIDATION_FAILED" ? error : wrongCode(error);
}

/** The cause of a value that a rule of the client refuses (Python's `ValueError`). Pure. */
export function ruleRefused(message: string): CoreCause {
  return Object.freeze({ kind: "ruleRefused", message });
}

/** The cause of a send that the app's state refuses: a task that the app does not list. Pure. */
export function preconditionFailed(message: string): CoreCause {
  return Object.freeze({
    kind: "coreFailure",
    failure: "preconditionFailed",
    subject: "send",
    message,
    retryable: false,
  });
}

function wrongCode(error: TaskSendCoreError): never {
  throw new TypeError(`taskSendCoreErrorFromNative: the converter gave the code ${error.code} for another class`);
}
