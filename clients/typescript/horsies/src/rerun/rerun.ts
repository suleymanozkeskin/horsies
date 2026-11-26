// The rerun in the app's transaction and the rerun in the caller's transaction. The core builds each
// statement and decides each outcome; this module parses the request, passes the carrier of the
// caller's active context, and maps the answers to the public values. A rerun opens no span.

import { RerunErrorCode } from "../_catalog/history.ts";
import { unreachable } from "../native/call/never.ts";
import type { CallerStatementRecord } from "../native/facades/rerun.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { TraceArgument } from "../native/values/records.ts";
import type { Result } from "../wire/result.ts";
import { err, ok } from "../wire/result.ts";
import { unreachable as unreachableInput } from "../wire/unreachable.ts";
import type { BridgePreparation, BridgeWrite, RerunBridge } from "./bridge.ts";
import type { CallerConnectionState, CallerQueryFailure, CallerRow, CallerTransaction } from "./caller.ts";
import { driverFailed } from "./caller.ts";
import { appRerunError, callerRowArg, callerStatement, rerunOutcome, stepRefused } from "./convert.ts";
import { contractViolation } from "./errors.ts";
import type {
  CallerRerunError,
  RerunContractViolation,
  RerunDriverFailed,
  RerunError,
  RerunNoOpenTransaction,
  RerunResultNotText,
} from "./errors.ts";
import { parseRerunRequest } from "./parse.ts";
import type { RerunRequest } from "./parse.ts";
import type { RerunCommand, RerunOutcome, RerunPolicy } from "./types.ts";

/** The outcome of a rerun in the app's transaction. */
export type AppRerunResult = Result<RerunOutcome, RerunError>;

/** The outcome of a rerun in the caller's transaction. */
export type CallerRerunResult = Result<RerunOutcome, CallerRerunError>;

/**
 * Rerun `command` in a transaction of `handle`, and commit it. The new task stores `trace`. A refused
 * command or policy answers before any call.
 */
export async function rerunInApp(
  bridge: RerunBridge,
  handle: CoreHandle,
  trace: TraceArgument,
  command: RerunCommand,
  policy: RerunPolicy,
): Promise<AppRerunResult> {
  const request = parseRerunRequest(command, policy);

  if (!request.ok) {
    return request;
  }

  const answer = await bridge.rerunTask(handle, request.value, trace);

  switch (answer.kind) {
    case "done":
      return ok(rerunOutcome(answer.outcome));
    case "failed":
      return err(appRerunError(answer.error));
    case "seatRelayFull":
      return err(answer.error);
    default:
      return unreachable(answer);
  }
}

/**
 * Rerun `command` in `transaction`, which the caller owns: the source read, then for an eligible source
 * the write. Nothing commits here. The new task stores `trace`. A refused command or policy answers
 * before any statement.
 */
export async function rerunInCaller(
  bridge: RerunBridge,
  trace: TraceArgument,
  transaction: CallerTransaction,
  command: RerunCommand,
  policy: RerunPolicy,
): Promise<CallerRerunResult> {
  const request = parseRerunRequest(command, policy);

  if (!request.ok) {
    return request;
  }

  const readRow = await queryOne(transaction, bridge.readStatement(request.value));

  if (!readRow.ok) {
    return readRow;
  }

  return finishPreparation(transaction, preparationOf(bridge, request.value, trace, readRow.value));
}

/** What the source read's row decides. */
function preparationOf(
  bridge: RerunBridge,
  request: RerunRequest,
  trace: TraceArgument,
  readRow: CallerRow,
): BridgePreparation {
  return bridge.prepareWrite(request, trace, callerRowArg(readRow));
}

/** The outcome of a preparation: its answer, its refusal, or the outcome of its write. */
async function finishPreparation(
  transaction: CallerTransaction,
  preparation: BridgePreparation,
): Promise<CallerRerunResult> {
  switch (preparation.kind) {
    case "answered":
      return ok(rerunOutcome(preparation.outcome));
    case "refused":
      return err(stepRefused(preparation.error));
    case "write":
      return runWrite(transaction, preparation.write);
    default:
      return unreachable(preparation);
  }
}

/** Run the write in `transaction` and decode its row. */
async function runWrite(transaction: CallerTransaction, write: BridgeWrite): Promise<CallerRerunResult> {
  const row = await queryOne(transaction, write.statement());

  if (!row.ok) {
    return row;
  }

  const decoded = write.decodeRow(callerRowArg(row.value));

  switch (decoded.kind) {
    case "decoded":
      return ok(rerunOutcome(decoded.outcome));
    case "rowRefused":
      return err(contractViolation(decoded.message));
    default:
      return unreachable(decoded);
  }
}

/** Why the caller's transaction could not give the row of a statement, as a public error. */
type CallerQueryError = RerunDriverFailed | RerunNoOpenTransaction | RerunResultNotText | RerunContractViolation;

/**
 * The one row of `record` from `transaction`. A throw of the transaction is the driver's failure, as a
 * failed result is.
 */
async function queryOne(
  transaction: CallerTransaction,
  record: CallerStatementRecord,
): Promise<Result<CallerRow, CallerQueryError>> {
  const statement = callerStatement(record);
  let answer: Result<CallerRow, CallerQueryFailure>;

  try {
    answer = await transaction.queryOne(statement);
  } catch (cause) {
    answer = err(driverFailed(cause));
  }

  return answer.ok ? answer : err(callerQueryError(statement.name, answer.error));
}

/** The public error of `failure`, the failure of the statement named `statement`. Pure. */
function callerQueryError(statement: string, failure: CallerQueryFailure): CallerQueryError {
  switch (failure.kind) {
    case "driverFailed":
      return {
        kind: "driverFailed",
        code: RerunErrorCode.DRIVER_FAILED,
        message: `statement ${statement} failed in the caller's driver: ${failure.driverError.message}`,
        statement,
        driverError: failure.driverError,
      };
    case "noOpenTransaction":
      return {
        kind: "noOpenTransaction",
        code: RerunErrorCode.NO_OPEN_TRANSACTION,
        retryable: false,
        message: `statement ${statement} did not run: ${noOpenTransactionCause(failure.state)}`,
        statement,
        state: failure.state,
      };
    case "resultNotText":
      return {
        kind: "resultNotText",
        code: RerunErrorCode.RESULT_NOT_TEXT,
        retryable: false,
        message:
          `statement ${statement} gave the column ${failure.column} in ${failure.format} format; the rerun reads ` +
          "text format (a pg client with binary: true gives binary format)",
        statement,
        column: failure.column,
        format: failure.format,
      };
    case "rowRefused":
      return contractViolation(failure.detail);
    default:
      return unreachableInput(failure, "caller query failure");
  }
}

/** Why a connection in `state` runs no statement of the rerun, and what the caller does. Pure. */
function noOpenTransactionCause(state: CallerConnectionState): string {
  switch (state.kind) {
    case "idle":
      return "the caller's connection has no open transaction; run BEGIN first";
    case "failedTransaction":
      return "a statement of the caller's transaction failed; roll it back";
    case "notConnected":
      return "the caller's client is not connected";
    case "unrecognized":
      return `the caller's driver gives the transaction status ${JSON.stringify(state.status)}, not an open transaction`;
    default:
      return unreachableInput(state, "caller connection state");
  }
}
