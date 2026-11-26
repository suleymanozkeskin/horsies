// The public values of the records that the addon gives: the outcome, the errors and their causes,
// and the caller statement and row in text form.

import type { CoreFailure } from "../_catalog/error_detail.ts";
import { type TaskId, taskIdFromCore } from "../contract/ids.ts";
import { unreachable } from "../native/call/never.ts";
import { coreFailureOf } from "../send/catalog-value.ts";
import type {
  AppRerunFailure,
  CallerRowArg,
  CallerStatementRecord,
  CallerStepRefusal,
  RerunOutcomeRecord,
  SqlTextValue,
} from "../native/facades/rerun.ts";
import type { CallerRow, CallerStatement, SqlText } from "./caller.ts";
import type { RerunError, RerunStepRefused } from "./errors.ts";
import type { RerunOutcome } from "./types.ts";

/** The public outcome of an outcome record: the same fields, each id as a `TaskId`. Pure. */
export function rerunOutcome(record: RerunOutcomeRecord): RerunOutcome {
  const id = (text: string): TaskId => taskIdFromCore(text, "rerun");

  switch (record.kind) {
    case "enqueued":
      return {
        kind: "enqueued",
        newTaskId: id(record.newTaskId),
        sourceTaskId: id(record.sourceTaskId),
        rerunRootTaskId: id(record.rerunRootTaskId),
      };
    case "sourceLive":
      return { kind: "sourceLive", taskId: id(record.taskId) };
    case "sourceAbsent":
      return { kind: "sourceAbsent", taskId: id(record.taskId), reason: record.reason };
    case "sourceCorrupt":
      return { kind: "sourceCorrupt", taskId: id(record.taskId), detail: record.detail };
    case "notEligible":
      return { kind: "notEligible", taskId: id(record.taskId), reason: record.reason };
    case "inputUnavailable":
      return { kind: "inputUnavailable", taskId: id(record.taskId), disposition: record.disposition };
    case "inputCorrupt":
      return { kind: "inputCorrupt", taskId: id(record.taskId), detail: record.detail };
    case "keyConflict":
      return { kind: "keyConflict", taskId: id(record.taskId), reservedByTaskId: id(record.reservedByTaskId) };
    case "keyReplay":
      return { kind: "keyReplay", existingTaskId: id(record.existingTaskId) };
    default:
      return unreachable(record);
  }
}

/** The public error of a failure of the rerun in the app's transaction. Pure. */
export function appRerunError(failure: AppRerunFailure): RerunError {
  switch (failure.code) {
    case "CONNECT_FAILED":
    case "FAMILY_CHECK_FAILED": {
      const exception = coreFailureOf(failure.cause);

      return { code: failure.code, retryable: failureRetryable(exception), message: failure.message, exception };
    }

    case "STATEMENT_FAILED": {
      const exception = coreFailureOf(failure.cause);

      return {
        code: "STATEMENT_FAILED",
        retryable: failureRetryable(exception),
        message: failure.message,
        outcome: failure.outcome,
        exception,
      };
    }

    case "INVALID_POLICY":
      return { code: "INVALID_POLICY", retryable: false, message: failure.message };
    case "CLASS_NOT_REGISTERED":
    case "CALLER_KEY_REFUSED":
    case "ID_MINT_REFUSED":
    case "CLIENT_FAMILY_MISMATCH":
    case "UNKNOWN_STORED_FAMILY":
    case "CONTRACT_VIOLATION":
      return stepRefused(failure);
    default:
      return unreachable(failure);
  }
}

/** The public error of a refusal of the rerun steps. Nothing is retryable: nothing is written. Pure. */
export function stepRefused(refusal: CallerStepRefusal): RerunStepRefused {
  switch (refusal.code) {
    case "CLASS_NOT_REGISTERED":
      return { code: refusal.code, retryable: false, message: refusal.message, classKey: refusal.classKey };
    case "CLIENT_FAMILY_MISMATCH":
      return { code: refusal.code, retryable: false, message: refusal.message, stored: refusal.stored };
    case "UNKNOWN_STORED_FAMILY":
      return { code: refusal.code, retryable: false, message: refusal.message, stored: refusal.stored };
    case "CALLER_KEY_REFUSED":
    case "ID_MINT_REFUSED":
    case "CONTRACT_VIOLATION":
      return { code: refusal.code, retryable: false, message: refusal.message };
    default:
      return unreachable(refusal);
  }
}

/** Whether the cause may clear. A contract violation never clears. Pure. */
function failureRetryable(failure: CoreFailure): boolean {
  switch (failure.failure) {
    case "server":
    case "connection":
    case "preconditionFailed":
      return failure.retryable;
    case "contractViolation":
      return false;
    default:
      return unreachable(failure);
  }
}

/** The public statement of a statement record. Pure. */
export function callerStatement(record: CallerStatementRecord): CallerStatement {
  return {
    name: record.name,
    sql: record.sql,
    parameters: record.parameters.map(sqlText),
    columns: record.columns.map((column) => ({ name: column.name, type: column.valueType })),
  };
}

/** The text of one SQL value. Pure. */
function sqlText(value: SqlTextValue): SqlText {
  switch (value.kind) {
    case "text":
      return value.text;
    case "null":
      return null;
    default:
      return unreachable(value);
  }
}

/** The row record of a caller row. Pure. */
export function callerRowArg(row: CallerRow): CallerRowArg {
  return { cells: row.cells.map((cell): SqlTextValue => (cell === null ? { kind: "null" } : { kind: "text", text: cell })) };
}
