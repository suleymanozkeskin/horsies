// Why a read of a task failed. A read error is not the task's outcome: the task may have failed, or may still run.

import type { BrokerErrorCode } from "../_catalog/broker.ts";
import { brokerCoreErrorFromNative, type NativeBrokerCoreError } from "../_catalog/broker_errors.ts";
import type { AbsenceReason } from "../native/sends/addon.ts";
import type { RawCatalogError } from "../native/values/records.ts";
import { nativeCatalogValue } from "../send/catalog-value.ts";
import type { LibraryTaskCode } from "../wire/task-error.ts";

/** The status of a task that has not finished. */
export type UnfinishedStatus = "PENDING" | "CLAIMED" | "RUNNING";

/**
 * The status of a finished task that can store no result. A `CANCELLED` task reads as its outcome (`TASK_CANCELLED`),
 * never as a read error.
 */
export type FinishedStatus = "COMPLETED" | "FAILED" | "EXPIRED";

/** Why neither the live table nor the task history holds a task. */
export type TaskAbsence =
  /** The task is older than the oldest history that retention keeps: retention can have removed it. */
  | "predatesRetainedHistory"
  /** No removed history can have held the task: it did not exist, or its history was removed out of band. */
  | "withinRetainedHistory"
  /** The id is not a UUIDv7, so it tells no time of birth. */
  | "notClassified";

/** Why a read of a task failed. */
export type ReadError =
  /** The task had not finished when the wait ended; it may still run. */
  | { readonly kind: "waitTimedOut"; readonly status: UnfinishedStatus }
  | { readonly kind: "taskNotFound"; readonly absence: TaskAbsence }
  /** The task finished and stores no result. */
  | { readonly kind: "resultNotAvailable"; readonly status: FinishedStatus }
  /** The stored result does not read with the contract's types. `cause` holds no value. */
  | { readonly kind: "storedResultUnreadable"; readonly cause: string }
  /** The stored error has a user code that the contract's `errors` does not list. */
  | { readonly kind: "unknownUserCode"; readonly code: string }
  /** A wait in a process that runs task bodies; nothing was read. */
  | { readonly kind: "waitInsideTask" }
  /** The database or the connection failed. */
  | {
      readonly kind: "database";
      readonly code: BrokerErrorCode;
      readonly retryable: boolean;
      readonly message: string;
    }
  /** A `timeoutMs` that is not a whole number of milliseconds from 0 to `maxMs`; nothing was read. */
  | { readonly kind: "invalidWait"; readonly maxMs: number };

/** The library code of a read error, as the Python and Rust clients name it. Pure. */
export function readErrorCode(error: ReadError): LibraryTaskCode {
  switch (error.kind) {
    case "waitTimedOut":
      return "WAIT_TIMEOUT";
    case "taskNotFound":
      return "TASK_NOT_FOUND";
    case "resultNotAvailable":
      return "RESULT_NOT_AVAILABLE";
    case "storedResultUnreadable":
    case "unknownUserCode":
      return "RESULT_DESERIALIZATION_ERROR";
    case "waitInsideTask":
      return "WAIT_INSIDE_TASK";
    case "database":
      return "BROKER_ERROR";
    case "invalidWait":
      return "INVALID_INPUT";
    default: {
      const unexpected: never = error;

      throw new TypeError(`readErrorCode: a read error outside its type: ${String(unexpected)}`);
    }
  }
}

/** The absence of a task, as the core names its reason. Pure. */
export function taskAbsence(reason: AbsenceReason): TaskAbsence {
  switch (reason) {
    case "PREDATES_RETAINED_HISTORY":
      return "predatesRetainedHistory";
    case "WITHIN_RETAINED_HISTORY":
      return "withinRetainedHistory";
    case "NOT_CLASSIFIED":
      return "notClassified";
    default: {
      const unexpected: never = reason;

      throw new TypeError(`taskAbsence: a reason outside its type: ${String(unexpected)}`);
    }
  }
}

/**
 * The read error of a raw record of the broker catalog: `storedResultUnreadable` for a stored text that the core's
 * strict read rule refuses (`INVALID_JSON_PAYLOAD`, a fact of the stored data, as the Python and Rust clients read it),
 * else `database`. An unknown class throws (a build defect).
 */
export function brokerReadError(raw: RawCatalogError): ReadError {
  // SAFETY: the bridge builds the slots of a broker class from the core's slot list of that class
  // (`ErrorClassValues`), the list from which the codegen renders `NativeBrokerCoreError`; the generated converter
  // throws for a class name that it does not know.
  const error = brokerCoreErrorFromNative(nativeCatalogValue(raw) as unknown as NativeBrokerCoreError);

  if (error.code === "INVALID_JSON_PAYLOAD") {
    return Object.freeze({ kind: "storedResultUnreadable", cause: error.message });
  }

  return Object.freeze({ kind: "database", code: error.code, retryable: error.retryable, message: error.message });
}
