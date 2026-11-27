// The rerun functions of the `facades` area of the addon, their argument and outcome records, and the
// accessor. Hand-kept declarations of `bridge/src/facades/rerun/`.

import type { RerunErrorCode } from "../../_catalog/history.ts";
import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { NativeCoreHandle } from "../handle/addon.ts";
import type { CauseRecord, TraceArgument } from "../values/records.ts";

/** The deadline of the new task: an instant in epoch microseconds, or no deadline. */
export type DeadlineArg = { readonly kind: "until"; readonly epochMicros: bigint } | { readonly kind: "noDeadline" };

/** The caller key of the new task. */
export type CallerKeyArg = { readonly kind: "keyed"; readonly key: string } | { readonly kind: "unkeyed" };

/** One rerun request. `sourceTaskId` is lower-case hyphenated UUID text. */
export interface RerunCommandArg {
  readonly sourceTaskId: string;
  readonly deadline: DeadlineArg;
  readonly callerKey: CallerKeyArg;
}

/** The retention of the new task: a whole number of days, or forever. */
export type RetentionArg = { readonly kind: "days"; readonly days: number } | { readonly kind: "forever" };

/** Whether the new task keeps its input for a later rerun. */
export type RerunInputArg = "keep" | "drop";

/** The enqueue policy of the new task. */
export interface RerunPolicyArg {
  readonly retention: RetentionArg;
  readonly rerunInput: RerunInputArg;
  readonly reservationWindowMicros: bigint;
}

/** One task that the calling app lists, with the custom codes that its contract declares (none for a contract with no list). */
export interface RegisteredTaskArg {
  readonly taskName: string;
  readonly codes: string[];
}

/** The tasks that the calling app lists, each once: a rerun checks the stored retry list of its source against them. */
export interface RegisteredTaskCodesArg {
  readonly tasks: RegisteredTaskArg[];
}

/** The text form of one SQL value: its PostgreSQL text input or output, or SQL NULL. */
export type SqlTextValue = { readonly kind: "text"; readonly text: string } | { readonly kind: "null" };

/** A built-in PostgreSQL type of a caller statement, by its SQL name. */
export type CallerTypeName = "bool" | "int2" | "int4" | "int8" | "text" | "bytea" | "uuid" | "timestamptz";

/** One result column of a caller statement. */
export interface CallerColumnRecord {
  readonly name: string;
  readonly valueType: CallerTypeName;
}

/** One core statement for a transaction that the caller owns. */
export interface CallerStatementRecord {
  readonly name: string;
  readonly sql: string;
  readonly parameters: SqlTextValue[];
  readonly columns: CallerColumnRecord[];
}

/** The one row that the caller's driver read: one cell per result column, in column order. */
export interface CallerRowArg {
  readonly cells: SqlTextValue[];
}

/** Why no live or history record has the source id. */
export type AbsenceReasonName = "PREDATES_RETAINED_HISTORY" | "WITHIN_RETAINED_HISTORY" | "NOT_CLASSIFIED";

/** Why a history source is not eligible for a rerun. */
export type NotEligibleName = "COMPLETED_SOURCE" | "WORKFLOW_TASK";

/** The outcome of one rerun. Task ids are lower-case hyphenated text. Only `enqueued` writes. */
export type RerunOutcomeRecord =
  | {
      readonly kind: "enqueued";
      readonly newTaskId: string;
      readonly sourceTaskId: string;
      readonly rerunRootTaskId: string;
    }
  | { readonly kind: "sourceLive"; readonly taskId: string }
  | { readonly kind: "sourceAbsent"; readonly taskId: string; readonly reason: AbsenceReasonName }
  | { readonly kind: "sourceCorrupt"; readonly taskId: string; readonly detail: string }
  | { readonly kind: "notEligible"; readonly taskId: string; readonly reason: NotEligibleName }
  | { readonly kind: "inputUnavailable"; readonly taskId: string; readonly disposition: string }
  | { readonly kind: "inputCorrupt"; readonly taskId: string; readonly detail: string }
  | { readonly kind: "keyConflict"; readonly taskId: string; readonly reservedByTaskId: string }
  | { readonly kind: "keyReplay"; readonly existingTaskId: string };

/** A client family, by its stored value. */
export type FamilyName = "PYTHON" | "RUST" | "TYPESCRIPT";

/** What a database records about the client family that installed it. */
export type StoredFamilyRecord = { readonly kind: "recorded"; readonly family: FamilyName } | { readonly kind: "notRecorded" };

/** Whether a failed statement of the rerun's own transaction may have committed it. */
export type StatementOutcome = "notCommitted" | "unknown";

/** A failure of the rerun in the app's transaction. `message` is the core's text of the error. */
export type AppRerunFailure =
  | { readonly code: typeof RerunErrorCode.CONNECT_FAILED; readonly message: string; readonly cause: CauseRecord }
  | {
      readonly code: typeof RerunErrorCode.STATEMENT_FAILED;
      readonly message: string;
      readonly cause: CauseRecord;
      readonly outcome: StatementOutcome;
    }
  | { readonly code: typeof RerunErrorCode.CLASS_NOT_REGISTERED; readonly message: string; readonly classKey: string }
  | { readonly code: typeof RerunErrorCode.INVALID_POLICY; readonly message: string }
  | { readonly code: typeof RerunErrorCode.CALLER_KEY_REFUSED; readonly message: string }
  | { readonly code: typeof RerunErrorCode.ID_MINT_REFUSED; readonly message: string }
  | { readonly code: typeof RerunErrorCode.CLIENT_FAMILY_MISMATCH; readonly message: string; readonly stored: StoredFamilyRecord }
  | { readonly code: typeof RerunErrorCode.UNKNOWN_STORED_FAMILY; readonly message: string; readonly stored: string }
  | { readonly code: typeof RerunErrorCode.FAMILY_CHECK_FAILED; readonly message: string; readonly cause: CauseRecord }
  | {
      readonly code: typeof RerunErrorCode.RETRY_CODE_NOT_DECLARED;
      readonly message: string;
      readonly taskName: string;
      readonly retryCode: string;
    }
  | { readonly code: typeof RerunErrorCode.TASK_NOT_REGISTERED; readonly message: string; readonly taskName: string }
  | { readonly code: typeof RerunErrorCode.CONTRACT_VIOLATION; readonly message: string };

/** How one rerun in the app's transaction ended. */
export type AppRerunAnswer =
  | { readonly kind: "done"; readonly outcome: RerunOutcomeRecord }
  | { readonly kind: "failed"; readonly error: AppRerunFailure };

/** A refusal of the rerun steps in the caller's transaction. Nothing is written. */
export type CallerStepRefusal =
  | { readonly code: typeof RerunErrorCode.CLASS_NOT_REGISTERED; readonly message: string; readonly classKey: string }
  | { readonly code: typeof RerunErrorCode.CALLER_KEY_REFUSED; readonly message: string }
  | { readonly code: typeof RerunErrorCode.ID_MINT_REFUSED; readonly message: string }
  | { readonly code: typeof RerunErrorCode.CLIENT_FAMILY_MISMATCH; readonly message: string; readonly stored: StoredFamilyRecord }
  | { readonly code: typeof RerunErrorCode.UNKNOWN_STORED_FAMILY; readonly message: string; readonly stored: string }
  | {
      readonly code: typeof RerunErrorCode.RETRY_CODE_NOT_DECLARED;
      readonly message: string;
      readonly taskName: string;
      readonly retryCode: string;
    }
  | { readonly code: typeof RerunErrorCode.TASK_NOT_REGISTERED; readonly message: string; readonly taskName: string }
  | { readonly code: typeof RerunErrorCode.CONTRACT_VIOLATION; readonly message: string };

/** The outcome of the write's row. */
export type WriteDecodeRecord =
  | { readonly kind: "decoded"; readonly outcome: RerunOutcomeRecord }
  | { readonly kind: "rowRefused"; readonly message: string };

/** The write of one rerun, prepared from the source read's row. */
export interface PreparedRerunWrite {
  statement(): NativeEnvelope<CallerStatementRecord>;
  decodeRow(row: CallerRowArg): NativeEnvelope<WriteDecodeRecord>;
}

/** What the source read's row decides. */
export type RerunPreparationRecord =
  | { readonly kind: "write"; readonly write: PreparedRerunWrite }
  | { readonly kind: "answered"; readonly outcome: RerunOutcomeRecord }
  | { readonly kind: "refused"; readonly error: CallerStepRefusal };

/** The rerun functions of the `facades` area. */
export interface RerunAddon {
  rerunTask(
    handle: NativeCoreHandle,
    command: RerunCommandArg,
    policy: RerunPolicyArg,
    trace: TraceArgument,
    codes: RegisteredTaskCodesArg,
  ): Promise<NativeEnvelope<AppRerunAnswer>>;
  rerunReadStatement(command: RerunCommandArg, policy: RerunPolicyArg): NativeEnvelope<CallerStatementRecord>;
  prepareRerunWrite(
    command: RerunCommandArg,
    policy: RerunPolicyArg,
    trace: TraceArgument,
    readRow: CallerRowArg,
    codes: RegisteredTaskCodesArg,
  ): NativeEnvelope<RerunPreparationRecord>;
}

/** The rerun functions of the addon. Throws as `requireAddon` does. */
export function rerunAddon(): RerunAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as RerunAddon;
}
