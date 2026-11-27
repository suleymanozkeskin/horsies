// The error unions of the monitoring reads, the health reads and the cancel.
//
// - The core catalog errors come from the generated converters (`monitoring_errors.ts`, `broker_errors.ts`).
// - `invalidInput` (`INVALID_INPUT`) of a query is the one client variant: an argument that the call refuses before any
//   connect. Nothing ran.
// - `SEAT_RELAY_FULL` comes in a `SHARED_THREAD` body only: the seat relay held its bound and sent nothing.
// - Each health read lists only the database codes that the read can give. A code outside the list is a defect of the
//   core: it crashes loud.

import type {
  DatabaseCoreError,
  DatabaseInvalidInput,
  DatabaseListenerDisabled,
  DatabaseOperationFailed,
  NativeDatabaseCoreError,
} from "../_catalog/broker_errors.ts";
import { databaseCoreErrorFromNative } from "../_catalog/broker_errors.ts";
import { MonitoringQueryRefusalCode } from "../_catalog/monitoring.ts";
import type {
  MonitoringQueryCoreError,
  NativeMonitoringQueryCoreError,
  NativeTaskActionCoreError,
  TaskActionCoreError,
} from "../_catalog/monitoring_errors.ts";
import { monitoringQueryCoreErrorFromNative, taskActionCoreErrorFromNative } from "../_catalog/monitoring_errors.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { unreachable } from "../native/call/never.ts";
import { nativeCatalogValue } from "../send/catalog-value.ts";
import type { RawCatalogError } from "../native/values/records.ts";
import type { SeatRelayFull } from "../seats/relay-full.ts";
import { ClientDefect } from "../thrown-errors.ts";

/** The argument that a call refuses before any connect. */
export type QueryRefusedField =
  | "window"
  | "page"
  | "limit"
  | "taskNames"
  | "queues"
  | "workers"
  | "errorCodes"
  | "workflowName"
  | "nodeIndex"
  | "workerId";

/** An argument that the call refuses before any connect: no statement ran. `reason` names the rule. */
export interface QueryRefused {
  readonly kind: "invalidInput";
  readonly code: typeof MonitoringQueryRefusalCode.INVALID_INPUT;
  readonly retryable: false;
  readonly field: QueryRefusedField;
  readonly reason: string;
}

/** The error of a monitoring read with no query to refuse. */
export type MonitoringQueryError = MonitoringQueryCoreError | SeatRelayFull;

/** The error of a monitoring read with a query that the read can refuse. */
export type MonitoringReadError = MonitoringQueryError | QueryRefused;

/** The error of a cancel. */
export type CancelTaskError = TaskActionCoreError | SeatRelayFull;

/** The error of a database ping. */
export type PingDatabaseError = DatabaseOperationFailed<"DATABASE_PING_FAILED"> | SeatRelayFull;

/** The error of a worker ping. */
export type PingWorkersError =
  | DatabaseInvalidInput
  | DatabaseListenerDisabled
  | DatabaseOperationFailed<"WORKER_PING_FAILED">
  | QueryRefused
  | SeatRelayFull;

/** The error of a health read with no argument to refuse. */
export type HealthReadError = DatabaseOperationFailed<"MONITORING_QUERY_FAILED"> | SeatRelayFull;

/** The error of a health read of one worker. */
export type WorkerReadError = HealthReadError | QueryRefused;

/** The error of a health read with a count or a threshold that the core checks: `staleTasks`. */
export type HealthInputError = DatabaseInvalidInput | DatabaseOperationFailed<"MONITORING_QUERY_FAILED"> | SeatRelayFull;

/** The error of the history read of one worker: its limit, and its worker id. */
export type WorkerHistoryError = HealthInputError | QueryRefused;

/** The frozen `invalidInput` refusal of `field`. Pure. */
export function queryRefused(field: QueryRefusedField, reason: string): QueryRefused {
  return Object.freeze({
    kind: "invalidInput",
    code: MonitoringQueryRefusalCode.INVALID_INPUT,
    retryable: false,
    field,
    reason,
  });
}

/** The monitoring query error of a raw record of the core. */
export function monitoringQueryError(raw: RawCatalogError): MonitoringQueryCoreError {
  // SAFETY: the read gives the record of a value of the core's `MonitoringQueryError`; the generated native types
  // name the classes and slots of the same core catalog, and the converter throws for a class it does not know.
  return monitoringQueryCoreErrorFromNative(nativeCatalogValue(raw) as NativeMonitoringQueryCoreError);
}

/** The task action error of a raw record of the core. */
export function taskActionError(raw: RawCatalogError): TaskActionCoreError {
  // SAFETY: the cancel gives the record of a value of the core's `TaskActionError`; the generated native types name
  // the classes and slots of the same core catalog, and the converter throws for a class it does not know.
  return taskActionCoreErrorFromNative(nativeCatalogValue(raw) as NativeTaskActionCoreError);
}

/** The database error of a raw record of the core. */
export function databaseError(raw: RawCatalogError): DatabaseCoreError {
  // SAFETY: the health reads give the record of a value of the core's `DatabaseOperationError`; the generated native
  // types name the classes and slots of the same core catalog, and the converter throws for a class it does not know.
  return databaseCoreErrorFromNative(nativeCatalogValue(raw) as NativeDatabaseCoreError);
}

/** The error of a database ping. Another code is a defect. */
export function pingDatabaseError(raw: RawCatalogError): PingDatabaseError {
  return readError("pingDatabase", databaseError(raw), PING_DATABASE_CODES);
}

/** The error of a worker ping. Another code is a defect. */
export function pingWorkersError(raw: RawCatalogError): PingWorkersError {
  const error = readError("pingWorkers", databaseError(raw), PING_WORKERS_CODES);

  switch (error.code) {
    case "INVALID_INPUT":
    case "LISTENER_DISABLED":
    case "WORKER_PING_FAILED":
      return error;
    default:
      return unreachable(error);
  }
}

/** The error of a health read with no argument to refuse. Another code is a defect. */
export function healthReadError(read: string, raw: RawCatalogError): HealthReadError {
  return readError(read, databaseError(raw), HEALTH_READ_CODES);
}

/** The error of a health read with a count or a threshold. Another code is a defect. */
export function healthInputError(read: string, raw: RawCatalogError): HealthInputError {
  const error = readError(read, databaseError(raw), HEALTH_INPUT_CODES);

  switch (error.code) {
    case "INVALID_INPUT":
    case "MONITORING_QUERY_FAILED":
      return error;
    default:
      return unreachable(error);
  }
}

/** A code of the core's database catalog. */
type DatabaseCode = DatabaseCoreError["code"];

/**
 * The database errors whose code is one of `Code`. `DatabaseOperationFailed` is a union by code, so it keeps only the
 * operation codes of `Code`.
 */
type DatabaseErrorWith<Code extends DatabaseCode> = Extract<DatabaseCoreError, { readonly code: Code }>;

/** The one code that a database ping can give. */
const PING_DATABASE_CODES: readonly "DATABASE_PING_FAILED"[] = ["DATABASE_PING_FAILED"];

/** The codes that a worker ping can give. */
const PING_WORKERS_CODES: readonly ("INVALID_INPUT" | "LISTENER_DISABLED" | "WORKER_PING_FAILED")[] = [
  "INVALID_INPUT",
  "LISTENER_DISABLED",
  "WORKER_PING_FAILED",
];

/** The one code that a health read with no argument to refuse can give. */
const HEALTH_READ_CODES: readonly "MONITORING_QUERY_FAILED"[] = ["MONITORING_QUERY_FAILED"];

/** The codes that a health read with a count or a threshold can give. */
const HEALTH_INPUT_CODES: readonly ("INVALID_INPUT" | "MONITORING_QUERY_FAILED")[] = [
  "INVALID_INPUT",
  "MONITORING_QUERY_FAILED",
];

/** `error` narrowed to the codes `codes` that the read `read` can give; another code is a defect. */
function readError<Code extends DatabaseCode>(
  read: string,
  error: DatabaseCoreError,
  codes: readonly Code[],
): DatabaseErrorWith<Code> {
  return hasCodeIn(error, codes) ? error : unexpectedCode(read, error);
}

/** Whether the code of `error` is one of `codes`. Pure. */
function hasCodeIn<Code extends DatabaseCode>(error: DatabaseCoreError, codes: readonly Code[]): error is DatabaseErrorWith<Code> {
  return codes.some((code) => code === error.code);
}

/** The defect of a database code that the read `read` cannot give. */
function unexpectedCode(read: string, error: DatabaseCoreError): never {
  return raiseDefect(
    new ClientDefect({ subject: read, rule: `the core answered ${error.code}, which this read cannot give` }),
  );
}
