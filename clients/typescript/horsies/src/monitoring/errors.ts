// The error unions of the monitoring reads, the health reads and the cancel.
//
// - The core catalog errors come from the generated converters (`monitoring_errors.ts`, `broker_errors.ts`).
// - `QUERY_REFUSED` is the one client code: an argument that the call refuses before any connect. Nothing ran.
// - Each health read lists only the broker codes that the read can give. A code outside the list is a defect of the
//   core: it crashes loud.

import type {
  BrokerCoreError,
  BrokerInvalidInput,
  BrokerListenerDisabled,
  BrokerOperationFailed,
  NativeBrokerCoreError,
} from "../_catalog/broker_errors.ts";
import { brokerCoreErrorFromNative } from "../_catalog/broker_errors.ts";
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
  | "taskIndex"
  | "workerId";

/** An argument that the call refuses before any connect: no statement ran. `reason` names the rule. */
export interface QueryRefused {
  readonly code: typeof MonitoringQueryRefusalCode.QUERY_REFUSED;
  readonly retryable: false;
  readonly field: QueryRefusedField;
  readonly reason: string;
}

/** The error of a monitoring read with a query that the read can refuse. */
export type MonitoringReadError = MonitoringQueryCoreError | QueryRefused;

/** The error of a cancel. */
export type CancelTaskError = TaskActionCoreError;

/** `BrokerOperationFailed` with one code of its subset. */
export type BrokerOperationFailedWith<Code extends BrokerOperationFailed["code"]> = BrokerOperationFailed & {
  readonly code: Code;
};

/** The error of a database ping. */
export type PingDatabaseError = BrokerOperationFailedWith<"DB_PING_FAILED">;

/** The error of a worker ping. */
export type PingWorkersError =
  | BrokerInvalidInput
  | BrokerListenerDisabled
  | BrokerOperationFailedWith<"WORKER_PING_FAILED">
  | QueryRefused;

/** The error of a health read with no argument to refuse. */
export type HealthReadError = BrokerOperationFailedWith<"MONITORING_QUERY_FAILED">;

/** The error of a health read of one worker. */
export type WorkerReadError = BrokerOperationFailedWith<"MONITORING_QUERY_FAILED"> | QueryRefused;

/** The error of a health read with a count or a threshold that the core checks: `staleTasks`. */
export type HealthInputError = BrokerInvalidInput | BrokerOperationFailedWith<"MONITORING_QUERY_FAILED">;

/** The error of the history read of one worker: its limit, and its worker id. */
export type WorkerHistoryError = HealthInputError | QueryRefused;

/** The frozen `QUERY_REFUSED` of `field`. Pure. */
export function queryRefused(field: QueryRefusedField, reason: string): QueryRefused {
  return Object.freeze({ code: MonitoringQueryRefusalCode.QUERY_REFUSED, retryable: false, field, reason });
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

/** The broker error of a raw record of the core. */
export function brokerError(raw: RawCatalogError): BrokerCoreError {
  // SAFETY: the health reads give the record of a value of the core's `BrokerOperationError`; the generated native
  // types name the classes and slots of the same core catalog, and the converter throws for a class it does not know.
  return brokerCoreErrorFromNative(nativeCatalogValue(raw) as NativeBrokerCoreError);
}

/** The error of a database ping. Another code is a defect. */
export function pingDatabaseError(raw: RawCatalogError): PingDatabaseError {
  return readError("pingDatabase", brokerError(raw), PING_DATABASE_CODES);
}

/** The error of a worker ping. Another code is a defect. */
export function pingWorkersError(raw: RawCatalogError): PingWorkersError {
  const error = readError("pingWorkers", brokerError(raw), PING_WORKERS_CODES);

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
  return readError(read, brokerError(raw), HEALTH_READ_CODES);
}

/** The error of a health read with a count or a threshold. Another code is a defect. */
export function healthInputError(read: string, raw: RawCatalogError): HealthInputError {
  const error = readError(read, brokerError(raw), HEALTH_INPUT_CODES);

  switch (error.code) {
    case "INVALID_INPUT":
    case "MONITORING_QUERY_FAILED":
      return error;
    default:
      return unreachable(error);
  }
}

/** A code of the core's broker catalog. */
type BrokerCode = BrokerCoreError["code"];

/**
 * The broker errors whose code is one of `Code`: each class of the union with its code narrowed to `Code`, so
 * `BrokerOperationFailed` keeps only the operation codes of `Code`. A class with no code in `Code` drops out.
 */
type BrokerErrorWith<Code extends BrokerCode> = BrokerCoreError extends infer Error_
  ? Error_ extends { readonly code: infer Member }
    ? [Extract<Member, Code>] extends [never]
      ? never
      : Error_ & { readonly code: Extract<Member, Code> }
    : never
  : never;

/** The one code that a database ping can give. */
const PING_DATABASE_CODES: readonly "DB_PING_FAILED"[] = ["DB_PING_FAILED"];

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
function readError<Code extends BrokerCode>(
  read: string,
  error: BrokerCoreError,
  codes: readonly Code[],
): BrokerErrorWith<Code> {
  return hasCodeIn(error, codes) ? error : unexpectedCode(read, error);
}

/** Whether the code of `error` is one of `codes`. Pure. */
function hasCodeIn<Code extends BrokerCode>(error: BrokerCoreError, codes: readonly Code[]): error is BrokerErrorWith<Code> {
  return codes.some((code) => code === error.code);
}

/** The defect of a broker code that the read `read` cannot give. */
function unexpectedCode(read: string, error: BrokerCoreError): never {
  return raiseDefect(
    new ClientDefect({ subject: read, rule: `the core answered ${error.code}, which this read cannot give` }),
  );
}
