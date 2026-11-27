// Compile-pass case: every member of the error unions of the monitoring reads, the health reads, the pings and the
// cancel has a `message`, so one log line serves each union.

import type {
  CancelTaskError,
  HealthInputError,
  HealthReadError,
  MonitoringQueryError,
  MonitoringReadError,
  PingDatabaseError,
  PingWorkersError,
  WorkerHistoryError,
  WorkerReadError,
} from "../../../horsies/src/index.ts";

type AnyMonitoringError =
  | CancelTaskError
  | HealthInputError
  | HealthReadError
  | MonitoringQueryError
  | MonitoringReadError
  | PingDatabaseError
  | PingWorkersError
  | WorkerHistoryError
  | WorkerReadError;

export function logLine(error: AnyMonitoringError): string {
  const message: string = error.message;

  return `${error.code}: ${message}`;
}
