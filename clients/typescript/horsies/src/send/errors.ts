// The errors of a send: the task send catalog of the core, generated (`src/_catalog/task_send_errors.ts`), told apart
// by `code`. The two errors that `retrySend` takes carry the contract's types and hold the prepared payload in a
// member that is not enumerable, so `JSON.stringify` of an error never holds the input.

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
import { nativeCatalogValue } from "./catalog-value.ts";

/** Type-only key of the types of a sent task. It does not exist at run time. */
export declare const sentTypes: unique symbol;

/** The output and code types of the task of a send error. */
export type SentTypes<O, E extends string> = { readonly output: O; readonly code: E };

/** A send error that carries the types of its task, for the handle that `retrySend` gives. */
export type WithSentTypes<V, O, E extends string> = V & { readonly [sentTypes]?: SentTypes<O, E> };

/** A send error that `retrySend` takes: the row may or may not exist (`ENQUEUE_FAILED`), or the key is taken. */
export type RetryableSendError<O, E extends string> =
  | WithSentTypes<TaskSendEnqueueFailed, O, E>
  | WithSentTypes<TaskSendIdempotencyKeyConflict, O, E>;

/** The error of a send, told apart by `code`. */
export type TaskSendError<O, E extends string> =
  | TaskSendSuppressed
  | TaskSendValidationFailed
  | TaskSendPayloadMismatch
  | TaskSendPayloadTooLarge
  | RetryableSendError<O, E>;

/** The public send error of a raw record of the task send catalog. An unknown class throws (a build defect). */
export function sendErrorOfRaw(raw: RawCatalogError): TaskSendCoreError {
  // SAFETY: the bridge builds the slots of a task send class from the core's slot list of that class
  // (`ErrorClassValues`), the list from which the codegen renders `NativeTaskSendCoreError`; the generated converter
  // throws for a class name that it does not know.
  return taskSendCoreErrorFromNative(nativeCatalogValue(raw) as unknown as NativeTaskSendCoreError);
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
