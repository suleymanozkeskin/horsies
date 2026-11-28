// Why a read of a task failed. A read error is not the task's outcome: the task may have failed, or may still run.

import type { DatabaseErrorCode } from "../_catalog/broker.ts";
import { databaseCoreErrorFromNative, type NativeDatabaseCoreError } from "../_catalog/broker_errors.ts";
import type { TaskAbsenceReason } from "../_catalog/history.ts";
import { OperationalCode } from "../_catalog/task_codes.ts";
import type { RawCatalogError } from "../native/values/records.ts";
import type { SeatRelayFull } from "../seats/relay-full.ts";
import { nativeCatalogValue } from "../send/catalog-value.ts";

/** The status of a task that has not finished. */
export type UnfinishedStatus = "PENDING" | "CLAIMED" | "RUNNING";

/**
 * The status of a finished task that can store no result. A `CANCELLED` task reads as its outcome (`TASK_CANCELLED`),
 * never as a read error.
 */
export type FinishedStatus = "COMPLETED" | "FAILED" | "EXPIRED";

/**
 * The catalog code of each kind of read error of a task, a workflow and a workflow context: the one table from which
 * each read error variant takes its `kind` and its `code`.
 */
export interface ReadErrorCodes {
  readonly waitTimeout: "WAIT_TIMEOUT";
  readonly taskNotFound: "TASK_NOT_FOUND";
  readonly workflowNotFound: "WORKFLOW_NOT_FOUND";
  readonly nodeNotFound: "NODE_NOT_FOUND";
  readonly resultNotAvailable: "RESULT_NOT_AVAILABLE";
  readonly resultNotReady: "RESULT_NOT_READY";
  readonly storedResultUnreadable: "RESULT_DESERIALIZATION_ERROR";
  readonly unknownCustomCode: "UNKNOWN_CUSTOM_CODE";
  readonly waitInsideTask: "WAIT_INSIDE_TASK";
  readonly noTypeAvailable: "NO_TYPE_AVAILABLE";
  readonly databaseError: "DATABASE_ERROR";
  readonly invalidInput: "INVALID_INPUT";
  readonly seatRelayFull: "SEAT_RELAY_FULL";
}

/** The `kind` and the `code` of the read error `Kind`, as `ReadErrorCodes` gives them. */
export type ReadErrorTag<Kind extends keyof ReadErrorCodes> = {
  readonly kind: Kind;
  readonly code: ReadErrorCodes[Kind];
};

/**
 * Why a read of a task failed, told apart by `kind`; `code` is the catalog code of the kind (`ReadErrorCodes`).
 * `databaseError` holds the code of the failed database operation in `databaseCode`, and `unknownCustomCode` holds the
 * stored custom code in `storedCode`.
 */
export type ReadError =
  /** The task had not finished when the wait ended; it may still run. */
  | (ReadErrorTag<"waitTimeout"> & { readonly status: UnfinishedStatus })
  /** Neither the live table nor the task history holds the task; `reason` tells whether retention can have removed it. */
  | (ReadErrorTag<"taskNotFound"> & { readonly reason: TaskAbsenceReason })
  /** The task finished and stores no result. */
  | (ReadErrorTag<"resultNotAvailable"> & { readonly status: FinishedStatus })
  /** The stored result does not read with the contract's types. `cause` holds no value. */
  | (ReadErrorTag<"storedResultUnreadable"> & { readonly cause: string })
  /** The stored error has a custom code that the contract's `errorCodes` does not list. */
  | (ReadErrorTag<"unknownCustomCode"> & { readonly storedCode: string })
  /** A wait in a process that runs task bodies; nothing was read. */
  | ReadErrorTag<"waitInsideTask">
  /** The database or the connection failed. */
  | (ReadErrorTag<"databaseError"> & {
      readonly databaseCode: DatabaseErrorCode;
      readonly retryable: boolean;
      readonly message: string;
    })
  /** A `waitMs` that is not a whole number of milliseconds from 0 to `maxMs`; nothing was read. */
  | (ReadErrorTag<"invalidInput"> & { readonly maxMs: number })
  /**
   * In a `SHARED_THREAD` body: the seat relay held its bound of waiting calls; nothing was read; retry. `message` names
   * the bound and its value.
   */
  | (ReadErrorTag<"seatRelayFull"> & { readonly retryable: true; readonly message: string });

/** The read error of a call that the seat relay did not send: the members of `error` but its cause. Pure. */
export function seatRelayFullRead(error: SeatRelayFull): ReadErrorTag<"seatRelayFull"> & {
  readonly retryable: true;
  readonly message: string;
} {
  return Object.freeze({ kind: error.kind, code: error.code, retryable: error.retryable, message: error.message });
}

/**
 * The read error of a raw record of the database catalog: `storedResultUnreadable` for a stored text that the core's
 * strict read rule refuses (`INVALID_JSON_PAYLOAD`, a fact of the stored data, as the Python and Rust clients read it),
 * else `databaseError`. An unknown class throws (a build defect).
 */
export function databaseReadError(raw: RawCatalogError): ReadError {
  // SAFETY: the bridge builds the slots of a database class from the core's slot list of that class
  // (`ErrorClassValues`), the list from which the codegen renders `NativeDatabaseCoreError`; the generated converter
  // throws for a class name that it does not know.
  const error = databaseCoreErrorFromNative(nativeCatalogValue(raw) as unknown as NativeDatabaseCoreError);

  if (error.kind === "invalidJsonPayload") {
    return Object.freeze({
      kind: "storedResultUnreadable",
      code: OperationalCode.RESULT_DESERIALIZATION_ERROR,
      cause: error.message,
    });
  }

  return Object.freeze({
    kind: "databaseError",
    code: OperationalCode.DATABASE_ERROR,
    databaseCode: error.code,
    retryable: error.retryable,
    message: error.message,
  });
}
