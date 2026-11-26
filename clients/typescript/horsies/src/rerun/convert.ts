// The public values of the records that the addon gives: the outcome, the errors and their causes,
// and the caller statement and row in text form.

import type { CoreFailure } from "../_catalog/error_detail.ts";
import { unreachable } from "../native/call/never.ts";
import type {
  AppRerunFailure,
  CallerRowArg,
  CallerStatementRecord,
  CallerStepRefusal,
  RerunOutcomeRecord,
  SqlTextValue,
} from "../native/facades/rerun.ts";
import type { CauseRecord } from "../native/values/records.ts";
import type { CallerRow, CallerStatement, SqlText } from "./caller.ts";
import type { RerunError, RerunStepRefused } from "./errors.ts";
import type { RerunOutcome } from "./types.ts";

/** The public outcome of an outcome record. The record has the public form already. Pure. */
export function rerunOutcome(record: RerunOutcomeRecord): RerunOutcome {
  return record;
}

/** The public error of a failure of the rerun in the app's transaction. Pure. */
export function appRerunError(failure: AppRerunFailure): RerunError {
  switch (failure.code) {
    case "CONNECT_FAILED":
    case "FAMILY_CHECK_FAILED": {
      const exception = coreFailure(failure.cause);

      return { code: failure.code, retryable: failureRetryable(exception), message: failure.message, exception };
    }

    case "STATEMENT_FAILED": {
      const exception = coreFailure(failure.cause);

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

/** The public cause of a cause record. Pure. */
function coreFailure(cause: CauseRecord): CoreFailure {
  switch (cause.kind) {
    case "server":
      return {
        kind: "coreFailure",
        failure: "server",
        subject: cause.subject,
        message: cause.message,
        retryable: cause.retryable,
        sqlstate: cause.sqlstate,
      };
    case "connection":
    case "preconditionFailed":
      return {
        kind: "coreFailure",
        failure: cause.kind,
        subject: cause.subject,
        message: cause.message,
        retryable: cause.retryable,
      };
    case "contractViolation":
      return { kind: "coreFailure", failure: "contractViolation", subject: cause.subject, message: cause.message };
    default:
      return unreachable(cause);
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
