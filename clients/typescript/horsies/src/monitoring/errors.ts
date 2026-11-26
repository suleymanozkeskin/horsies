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
import type {
  MonitoringQueryCoreError,
  NativeMonitoringQueryCoreError,
  NativeTaskActionCoreError,
  TaskActionCoreError,
} from "../_catalog/monitoring_errors.ts";
import { monitoringQueryCoreErrorFromNative, taskActionCoreErrorFromNative } from "../_catalog/monitoring_errors.ts";
import { raiseDefect } from "../native/call/defect.ts";
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
  readonly code: "QUERY_REFUSED";
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
  return Object.freeze({ code: "QUERY_REFUSED", retryable: false, field, reason });
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
  const error = brokerError(raw);

  return operationFailedWith(error, "DB_PING_FAILED") ?? unexpectedCode("pingDatabase", error);
}

/** The error of a worker ping. Another code is a defect. */
export function pingWorkersError(raw: RawCatalogError): PingWorkersError {
  const error = brokerError(raw);

  switch (error.code) {
    case "INVALID_INPUT":
    case "LISTENER_DISABLED":
      return error;
    default:
      return operationFailedWith(error, "WORKER_PING_FAILED") ?? unexpectedCode("pingWorkers", error);
  }
}

/** The error of a health read with no argument to refuse. Another code is a defect. */
export function healthReadError(read: string, raw: RawCatalogError): HealthReadError {
  const error = brokerError(raw);

  return operationFailedWith(error, "MONITORING_QUERY_FAILED") ?? unexpectedCode(read, error);
}

/** The error of a health read with a count or a threshold. Another code is a defect. */
export function healthInputError(read: string, raw: RawCatalogError): HealthInputError {
  const error = brokerError(raw);

  switch (error.code) {
    case "INVALID_INPUT":
      return error;
    default:
      return operationFailedWith(error, "MONITORING_QUERY_FAILED") ?? unexpectedCode(read, error);
  }
}

/** `error` when it is `BrokerOperationFailed` with `code`, else `undefined`. Pure. */
function operationFailedWith<Code extends BrokerOperationFailed["code"]>(
  error: BrokerCoreError,
  code: Code,
): BrokerOperationFailedWith<Code> | undefined {
  return isOperationFailedWith(error, code) ? error : undefined;
}

/** Whether `error` is `BrokerOperationFailed` with `code`. Pure. */
function isOperationFailedWith<Code extends BrokerOperationFailed["code"]>(
  error: BrokerCoreError,
  code: Code,
): error is BrokerOperationFailedWith<Code> {
  return error.code === code;
}

/** The defect of a broker code that the read `read` cannot give. */
function unexpectedCode(read: string, error: BrokerCoreError): never {
  return raiseDefect(
    new ClientDefect({ subject: read, rule: `the core answered ${error.code}, which this read cannot give` }),
  );
}
