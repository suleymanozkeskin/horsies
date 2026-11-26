// Why a read of a task failed. A read error is not the task's outcome: the task may have failed, or may still run.

import type { BrokerErrorCode } from "../_catalog/broker.ts";
import { brokerCoreErrorFromNative, type NativeBrokerCoreError } from "../_catalog/broker_errors.ts";
import type { TaskAbsenceReason } from "../_catalog/history.ts";
import type { SeatRelayCode } from "../_catalog/task_send.ts";
import type { RawCatalogError } from "../native/values/records.ts";
import type { SeatRelayFull } from "../seats/relay-full.ts";
import { nativeCatalogValue } from "../send/catalog-value.ts";
import type { BuiltInTaskCode } from "../wire/task-error.ts";

/** The status of a task that has not finished. */
export type UnfinishedStatus = "PENDING" | "CLAIMED" | "RUNNING";

/**
 * The status of a finished task that can store no result. A `CANCELLED` task reads as its outcome (`TASK_CANCELLED`),
 * never as a read error.
 */
export type FinishedStatus = "COMPLETED" | "FAILED" | "EXPIRED";

/**
 * Why a read of a task failed, told apart by `kind`, which is named after the catalog code in `code`. Two variants have
 * no `code` member, `storedResultUnreadable` and `unknownUserCode` (both `RESULT_DESERIALIZATION_ERROR`), and
 * `database` holds the broker code in `code` (`BROKER_ERROR`).
 */
export type ReadError =
  /** The task had not finished when the wait ended; it may still run. */
  | { readonly kind: "waitTimeout"; readonly code: "WAIT_TIMEOUT"; readonly status: UnfinishedStatus }
  /** Neither the live table nor the task history holds the task; `reason` tells whether retention can have removed it. */
  | { readonly kind: "taskNotFound"; readonly code: "TASK_NOT_FOUND"; readonly reason: TaskAbsenceReason }
  /** The task finished and stores no result. */
  | { readonly kind: "resultNotAvailable"; readonly code: "RESULT_NOT_AVAILABLE"; readonly status: FinishedStatus }
  /** The stored result does not read with the contract's types. `cause` holds no value. */
  | { readonly kind: "storedResultUnreadable"; readonly cause: string }
  /** The stored error has a custom code that the contract's `errorCodes` does not list. */
  | { readonly kind: "unknownUserCode"; readonly code: string }
  /** A wait in a process that runs task bodies; nothing was read. */
  | { readonly kind: "waitInsideTask"; readonly code: "WAIT_INSIDE_TASK" }
  /** The database or the connection failed. */
  | {
      readonly kind: "database";
      readonly code: BrokerErrorCode;
      readonly retryable: boolean;
      readonly message: string;
    }
  /** A `waitMs` that is not a whole number of milliseconds from 0 to `maxMs`; nothing was read. */
  | { readonly kind: "invalidInput"; readonly code: "INVALID_INPUT"; readonly maxMs: number }
  /**
   * In a `SHARED_THREAD` body: the seat relay held its bound of waiting calls; nothing was read; retry. `message` names
   * the bound and its value.
   */
  | {
      readonly kind: "seatRelayFull";
      readonly code: "SEAT_RELAY_FULL";
      readonly retryable: true;
      readonly message: string;
    };

/**
 * The catalog code of a read error, as the Python and Rust clients name it: the `code` member of a variant that has
 * one; `RESULT_DESERIALIZATION_ERROR` for `storedResultUnreadable` and `unknownUserCode`, `BROKER_ERROR` for
 * `database`. `SEAT_RELAY_FULL` is a code that those clients do not have. Pure.
 */
export function readErrorCode(error: ReadError): BuiltInTaskCode | SeatRelayCode {
  switch (error.kind) {
    case "waitTimeout":
    case "taskNotFound":
    case "resultNotAvailable":
    case "waitInsideTask":
    case "invalidInput":
    case "seatRelayFull":
      return error.code;
    case "storedResultUnreadable":
    case "unknownUserCode":
      return "RESULT_DESERIALIZATION_ERROR";
    case "database":
      return "BROKER_ERROR";
    default: {
      const unexpected: never = error;

      throw new TypeError(`readErrorCode: a read error outside its type: ${String(unexpected)}`);
    }
  }
}

/** The read error of a call that the seat relay did not send: the members of `error` but its cause. Pure. */
export function seatRelayFullRead(error: SeatRelayFull): Extract<ReadError, { readonly kind: "seatRelayFull" }> {
  return Object.freeze({ kind: "seatRelayFull", code: error.code, retryable: error.retryable, message: error.message });
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
