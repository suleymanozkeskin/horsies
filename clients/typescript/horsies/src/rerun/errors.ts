// The errors of a rerun: a closed union for the rerun in the app's transaction (`RerunError`) and one
// for the rerun in the caller's transaction (`CallerRerunError`), each told apart by `code`.

import type { CoreFailure } from "../_catalog/error_detail.ts";
import { RerunErrorCode } from "../_catalog/history.ts";
import type { SeatRelayFull } from "../seats/relay-full.ts";
import type { CallerConnectionState } from "./caller.ts";

/** The app's handle could not connect, install the schema, or open its closed pools again. Nothing ran. */
export interface RerunConnectFailed {
  readonly code: typeof RerunErrorCode.CONNECT_FAILED;
  readonly retryable: boolean;
  readonly message: string;
  readonly exception: CoreFailure;
}

/** Whether a failed statement of the app's transaction may have committed the rerun. */
export type RerunStatementOutcome = "notCommitted" | "unknown";

/**
 * A statement of the app's transaction failed. `outcome` is `notCommitted`, or `unknown` when the
 * reply to `COMMIT` was lost: then a keyed rerun sent again answers `keyReplay` if the first one
 * committed.
 */
export interface RerunStatementFailed {
  readonly code: typeof RerunErrorCode.STATEMENT_FAILED;
  readonly retryable: boolean;
  readonly message: string;
  readonly outcome: RerunStatementOutcome;
  readonly exception: CoreFailure;
}

/** The check of the database's client family could not read the family row. Nothing was read or written. */
export interface RerunFamilyCheckFailed {
  readonly code: typeof RerunErrorCode.FAMILY_CHECK_FAILED;
  readonly retryable: boolean;
  readonly message: string;
  readonly exception: CoreFailure;
}

/** The command breaks a rule of its fields. Nothing ran. */
export interface RerunInvalidCommand {
  readonly code: typeof RerunErrorCode.INVALID_COMMAND;
  readonly retryable: false;
  readonly message: string;
}

/** The policy breaks a rule of its fields. Nothing ran. */
export interface RerunInvalidPolicy {
  readonly code: typeof RerunErrorCode.INVALID_POLICY;
  readonly retryable: false;
  readonly message: string;
}

/** The retention class of the new task has no class row: register its period first. Nothing is written. */
export interface RerunClassNotRegistered {
  readonly code: typeof RerunErrorCode.CLASS_NOT_REGISTERED;
  readonly retryable: false;
  readonly message: string;
  readonly classKey: string;
}

/** The caller key is not a valid idempotency key: empty, or more than 255 UTF-8 bytes. Nothing is written. */
export interface RerunCallerKeyRefused {
  readonly code: typeof RerunErrorCode.CALLER_KEY_REFUSED;
  readonly retryable: false;
  readonly message: string;
}

/** The task id source refused to mint the new task's id. Nothing is written. */
export interface RerunIdMintRefused {
  readonly code: typeof RerunErrorCode.ID_MINT_REFUSED;
  readonly retryable: false;
  readonly message: string;
}

/** A client family, by its stored value. */
export type RerunClientFamily = "PYTHON" | "RUST" | "TYPESCRIPT";

/** What the database records about the client family that installed it. */
export type RerunStoredFamily =
  | { readonly kind: "recorded"; readonly family: RerunClientFamily }
  | { readonly kind: "notRecorded" };

/** Another client family installed the database, or it records none. Nothing is written. */
export interface RerunClientFamilyMismatch {
  readonly code: typeof RerunErrorCode.CLIENT_FAMILY_MISMATCH;
  readonly retryable: false;
  readonly message: string;
  readonly stored: RerunStoredFamily;
}

/** The family row holds a value that is not a client family: corrupt stored data. Nothing is written. */
export interface RerunUnknownStoredFamily {
  readonly code: typeof RerunErrorCode.UNKNOWN_STORED_FAMILY;
  readonly retryable: false;
  readonly message: string;
  readonly stored: string;
}

/** A row or stored data broke the rerun's contract. Nothing committed; the same call meets it again. */
export interface RerunContractViolation {
  readonly code: typeof RerunErrorCode.CONTRACT_VIOLATION;
  readonly retryable: false;
  readonly message: string;
}

/**
 * A statement of the rerun failed in the caller's driver. The caller's transaction is aborted: roll it
 * back. Whether to run the transaction again is the rule of `driverError`, for example its SQLSTATE.
 */
export interface RerunDriverFailed {
  readonly code: typeof RerunErrorCode.DRIVER_FAILED;
  readonly message: string;
  /** The name of the core statement that failed. */
  readonly statement: string;
  readonly driverError: Error;
}

/**
 * The caller's connection had no open transaction for a statement of the rerun, so the statement did
 * not run: without a transaction it would commit by itself. `state` tells why: run `BEGIN` first, or
 * roll back a failed transaction.
 */
export interface RerunNoOpenTransaction {
  readonly code: typeof RerunErrorCode.NO_OPEN_TRANSACTION;
  readonly retryable: false;
  readonly message: string;
  /** The name of the core statement that did not run. */
  readonly statement: string;
  readonly state: CallerConnectionState;
}

/**
 * The caller's driver gave a result column in a format other than text, for example a `pg` client
 * with `binary: true`; the rerun reads text. The statement ran: roll the caller's transaction back,
 * and use a client that reads text.
 */
export interface RerunResultNotText {
  readonly code: typeof RerunErrorCode.RESULT_NOT_TEXT;
  readonly retryable: false;
  readonly message: string;
  /** The name of the core statement whose result is not text. */
  readonly statement: string;
  /** The result column that is not text. */
  readonly column: string;
  /** The format that the driver gave the column, for example `binary`. */
  readonly format: string;
}

/** A refusal of the command or the policy, before any call. */
export type RerunRequestRefused = RerunInvalidCommand | RerunInvalidPolicy;

/** A refusal of the rerun steps: nothing is written. */
export type RerunStepRefused =
  | RerunClassNotRegistered
  | RerunCallerKeyRefused
  | RerunIdMintRefused
  | RerunClientFamilyMismatch
  | RerunUnknownStoredFamily
  | RerunContractViolation;

/**
 * Why a rerun in the app's transaction failed. `SEAT_RELAY_FULL` comes in a `SHARED_THREAD` body only: the seat relay
 * held its bound and sent nothing.
 */
export type RerunError =
  | RerunConnectFailed
  | RerunStatementFailed
  | RerunFamilyCheckFailed
  | RerunRequestRefused
  | RerunStepRefused
  | SeatRelayFull;

/** Why a rerun in the caller's transaction failed. After it, the caller does not commit. */
export type CallerRerunError =
  | RerunDriverFailed
  | RerunNoOpenTransaction
  | RerunResultNotText
  | RerunRequestRefused
  | RerunStepRefused;

/** The refusal of a command field. Pure. */
export function invalidCommand(message: string): RerunInvalidCommand {
  return { code: RerunErrorCode.INVALID_COMMAND, retryable: false, message };
}

/** The refusal of a policy field. Pure. */
export function invalidPolicy(message: string): RerunInvalidPolicy {
  return { code: RerunErrorCode.INVALID_POLICY, retryable: false, message };
}

/** The contract violation of a row. Pure. */
export function contractViolation(message: string): RerunContractViolation {
  return { code: RerunErrorCode.CONTRACT_VIOLATION, retryable: false, message };
}
