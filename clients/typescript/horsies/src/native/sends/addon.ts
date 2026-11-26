// The functions of the `sends` area of the addon, their records, and the accessor.
//
// - The check functions (`checkAppConfig`, `parseDeclaredErrorCodes`, `checkTaskOptions`) and the classes that they
//   give are pure: they start no core, open no connection and read no clock, so `Horsies.create` can call them before
//   a child host shapes the process runtime.
// - `prepareSendFacts` mints a task id and starts the log forwarder of the process, so the payload warning reaches the
//   log sink; it starts no core.
// - `enqueue`, `getResult` and `taskInfo` are operations on a core handle: they start the core of the process at the
//   first call and run on its runtime.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { InstallCoverageArg, NativeCoreHandle } from "../handle/addon.ts";
import type { RawCatalogError, TraceArgument } from "../values/records.ts";

/** The code of a refusal of a core check: a catalog code (`HRS-nnn`), or none (a field bound with no catalog code). */
export type CoreRefusalCode = { readonly kind: "catalog"; readonly code: string } | { readonly kind: "noCode" };

/** One refusal of a core check: the code, the core's message, and its notes and help lines. */
export interface CoreRefusal {
  readonly code: CoreRefusalCode;
  readonly message: string;
  readonly notes: string[];
  readonly help: string[];
}

/**
 * The outcome of the check of an app config (the JSON text of the core's `AppConfig`; a field that the text leaves
 * out takes the core's default):
 * - `accepted`: the send rules of the config, the JSON text of the producer manifest of its broker settings, and the
 *   coverage values of its retention for `CoreHandle.withInstallCoverage`;
 * - `refused`: every refusal, in the core's order. A text that the core's config reader refuses is one refusal with
 *   no code.
 */
export type AppConfigCheck =
  | {
      readonly kind: "accepted";
      readonly rules: NativeSendRules;
      readonly manifestJson: string;
      readonly installCoverage: InstallCoverageArg;
    }
  | { readonly kind: "refused"; readonly refusals: CoreRefusal[] };

/** The outcome of the check of a declared error code list. */
export type DeclaredCodesCheck =
  | { readonly kind: "accepted" }
  | { readonly kind: "refused"; readonly refusals: CoreRefusal[] };

/** The execution class of a task, as the core names it. */
export type ExecutionName = "ISOLATED" | "SHARED_ASYNC" | "SHARED_THREAD";

/** A duration in milliseconds that a task sets, or not. */
export type MillisecondsFact =
  | { readonly kind: "notSet" }
  | { readonly kind: "set"; readonly milliseconds: number };

/** The longest wait between two retries of an exponential policy. */
export type MaxDelayFact = { readonly kind: "noCap" } | { readonly kind: "seconds"; readonly seconds: number };

/** The retry policy of a task. Each code of `autoRetryFor` is a user code or a library code, as its text. */
export type RetryFacts =
  | { readonly kind: "noRetry" }
  | {
      readonly kind: "fixed";
      readonly intervalsSeconds: number[];
      readonly jitter: boolean;
      readonly autoRetryFor: string[];
    }
  | {
      readonly kind: "exponential";
      readonly baseSeconds: number;
      readonly maxRetries: number;
      readonly jitter: boolean;
      readonly maxDelay: MaxDelayFact;
      readonly autoRetryFor: string[];
    };

/** The error codes of a task: any code (`open`), or the declared list. */
export type DeclaredCodesFact = { readonly kind: "open" } | { readonly kind: "declared"; readonly codes: string[] };

/** The facts of a task that its stored options text holds, and the rules of the core check. */
export interface TaskOptionsFacts {
  readonly taskName: string;
  readonly queueName: string;
  readonly execution: ExecutionName;
  readonly timeout: MillisecondsFact;
  readonly cancelGrace: MillisecondsFact;
  readonly retry: RetryFacts;
  readonly declaredCodes: DeclaredCodesFact;
}

/** The outcome of the check of the options of a task. */
export type TaskOptionsCheck =
  | { readonly kind: "accepted"; readonly options: NativeTaskOptions }
  | { readonly kind: "refused"; readonly refusals: CoreRefusal[] };

/** The checked options of one task (the core's `TaskOptions`). Pure. */
export interface NativeTaskOptions {
  /** The stored options text of a send with no deadline. */
  storedText(): NativeEnvelope<string>;
}

/** The retention that a send chooses. */
export type RetentionChoice =
  | { readonly kind: "queueOrDefault" }
  | { readonly kind: "forever" }
  | { readonly kind: "days"; readonly days: number };

/** The retention of a send as the core stores it: forever, or a class key (`standard_30d`). */
export type RetentionRecord = { readonly kind: "forever" } | { readonly kind: "classKey"; readonly key: string };

/** A deadline: none, or an instant in epoch microseconds. */
export type DeadlineFact = { readonly kind: "noDeadline" } | { readonly kind: "until"; readonly epochMicros: bigint };

/** The facts of one send that the send rules judge. */
export interface SendFacts {
  readonly taskName: string;
  readonly queueName: string;
  readonly retention: RetentionChoice;
  readonly deadline: DeadlineFact;
  /** The UTF-8 byte length of the input text. */
  readonly inputBytes: number;
}

/**
 * The prepared facts of one send: a new task id, the stored options text (with the deadline) and the retention, or
 * the send error of a refused fact (`TaskSendValidationFailed` for a retention that the config does not register or a
 * refused id, `TaskSendPayloadTooLarge` for an input above the reject bound).
 */
export type PreparedSend =
  | {
      readonly kind: "prepared";
      readonly taskId: string;
      readonly taskOptions: string;
      readonly retention: RetentionRecord;
    }
  | { readonly kind: "refused"; readonly error: RawCatalogError };

/** The rules of an accepted app config that each send applies: retention and the payload bounds. Pure. */
export interface NativeSendRules {
  /**
   * The prepared facts of one send of a task with the checked `options`. Logs the payload warning of the core once
   * per task name when the input is above the warn bound. Mints a task id.
   */
  prepareSendFacts(facts: SendFacts, options: NativeTaskOptions): NativeEnvelope<PreparedSend>;
}

/** A delay before a worker may start the task. */
export type DelayFact = { readonly kind: "noDelay" } | { readonly kind: "seconds"; readonly seconds: number };

/** A text that a send may leave out. */
export type OptionalText = { readonly kind: "none" } | { readonly kind: "text"; readonly text: string };

/** One send, as the core takes it. A resend of the same task id passes the same values. */
export interface EnqueueRequest {
  readonly taskId: string;
  readonly taskName: string;
  readonly queueName: string;
  readonly priority: number;
  readonly inputJson: string;
  readonly sentAtMicros: bigint;
  readonly deadline: DeadlineFact;
  readonly delay: DelayFact;
  readonly taskOptions: OptionalText;
  readonly idempotencyKey: OptionalText;
  readonly retention: RetentionRecord;
}

/** `FIRST` for the first send of a task id; `REPEAT` for a resend of it. */
export type SendKind = "FIRST" | "REPEAT";

/** The outcome of one send: the task id, or the send error (a class of the task send catalog). */
export type EnqueueOutcome =
  | { readonly kind: "enqueued"; readonly taskId: string }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** How long a result read waits for a task that has not finished. */
export type WaitBound = { readonly kind: "untilEnd" } | { readonly kind: "upTo"; readonly milliseconds: number };

/** The status of a task, as the core names it. */
export type TaskStatusName = "PENDING" | "CLAIMED" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED" | "EXPIRED";

/** Why neither the live table nor the task history holds a task. */
export type AbsenceReason = "PREDATES_RETAINED_HISTORY" | "WITHIN_RETAINED_HISTORY" | "NOT_CLASSIFIED";

/** The stored result envelope of a task: its text, or none when the wait ended before the task. */
export type EnvelopeFact = { readonly kind: "stored"; readonly text: string } | { readonly kind: "notFinished" };

/** The status of one task and its stored result envelope. */
export interface ResultRecord {
  readonly taskId: string;
  readonly taskName: string;
  readonly status: TaskStatusName;
  readonly envelope: EnvelopeFact;
}

/** The outcome of one result read. `failed` holds a class of the broker catalog. */
export type ResultRead =
  | { readonly kind: "found"; readonly record: ResultRecord }
  | { readonly kind: "absent"; readonly reason: AbsenceReason }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** A text fact of a task row that the row may not hold. */
export type RecordedText = { readonly kind: "notRecorded" } | { readonly kind: "recorded"; readonly text: string };

/** An instant of a task row that the row may not hold, in epoch microseconds. */
export type RecordedInstant =
  | { readonly kind: "notRecorded" }
  | { readonly kind: "recorded"; readonly epochMicros: bigint };

/** A count of a task row that the row may not hold. */
export type RecordedCount = { readonly kind: "notRecorded" } | { readonly kind: "recorded"; readonly value: number };

/** How a worker runs a task: the class of a live task; a task read from history does not keep it. */
export type ExecutionRecord = ExecutionName | "NOT_KEPT_IN_HISTORY";

/** The outcome of one attempt of a task. */
export type AttemptOutcomeName = "COMPLETED" | "FAILED" | "WORKER_FAILURE" | "CANCELLED";

/** One attempt of a task. */
export interface TaskAttemptRecord {
  readonly attempt: number;
  readonly outcome: AttemptOutcomeName;
  readonly willRetry: boolean;
  readonly startedAtMicros: bigint;
  readonly finishedAtMicros: bigint;
  readonly errorCode: RecordedText;
  readonly errorMessage: RecordedText;
  readonly failedReason: RecordedText;
  readonly workerId: RecordedText;
  readonly workerHostname: RecordedText;
  readonly workerPid: RecordedCount;
  readonly workerProcessName: RecordedText;
}

/** The info of one task: its status, times, failed reason and every attempt, newest first. No result. */
export interface TaskInfoRecord {
  readonly taskId: string;
  readonly taskName: string;
  readonly status: TaskStatusName;
  readonly queueName: string;
  readonly priority: number;
  readonly retryCount: number;
  readonly maxRetries: number;
  readonly nextRetryAt: RecordedInstant;
  readonly sentAt: RecordedInstant;
  readonly enqueuedAtMicros: bigint;
  readonly claimedAt: RecordedInstant;
  readonly startedAt: RecordedInstant;
  readonly completedAt: RecordedInstant;
  readonly failedAt: RecordedInstant;
  readonly workerHostname: RecordedText;
  readonly workerPid: RecordedCount;
  readonly workerProcessName: RecordedText;
  readonly errorCode: RecordedText;
  readonly failedReason: RecordedText;
  readonly attempts: TaskAttemptRecord[];
  readonly execution: ExecutionRecord;
}

/** The outcome of one task info read. `failed` holds a class of the broker catalog. */
export type TaskInfoRead =
  | { readonly kind: "found"; readonly record: TaskInfoRecord }
  | { readonly kind: "absent"; readonly reason: AbsenceReason }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** Whether this process runs task bodies: a task child does; a producer, a worker parent or a scheduler does not. */
export type ProcessRoleName = "RUNS_TASK_BODIES" | "RUNS_NO_TASK_BODIES";

/** The code of an error that a body gives: a code that the task chose, or a library code. */
export type ErrorCodePart =
  | { readonly kind: "user"; readonly code: string }
  | { readonly kind: "library"; readonly code: string };

/**
 * The exception record of a body error: none, or the record of a thrown value. `className` is the stored `type` of
 * the record (`error.name`).
 */
export type ExceptionPart =
  | { readonly kind: "noException" }
  | {
      readonly kind: "record";
      readonly className: string;
      readonly module: string;
      readonly message: string;
      readonly repr: string;
      readonly traceback: string;
    };

/** The parts of the error of a body. `dataJson` is the codec's stored `data` text (`null` for no data). */
export interface TaskErrorParts {
  readonly code: ErrorCodePart;
  readonly message: OptionalText;
  readonly dataJson: string;
  readonly exception: ExceptionPart;
}

/** The core's task error of a body, built from its parts. */
export interface NativeTaskError {
  /** The stored error object text, as the core writes it into a result envelope. */
  storedText(): NativeEnvelope<string>;
}

/** The outcome of a task error build: the error, or why the core refuses the parts (a reserved user code). */
export type TaskErrorBuild =
  | { readonly kind: "built"; readonly error: NativeTaskError }
  | { readonly kind: "refused"; readonly cause: string };

/** The functions of the `sends` area. */
export interface SendsAddon {
  checkAppConfig(appConfigJson: string): NativeEnvelope<AppConfigCheck>;
  parseDeclaredErrorCodes(codes: string[]): NativeEnvelope<DeclaredCodesCheck>;
  checkTaskOptions(facts: TaskOptionsFacts): NativeEnvelope<TaskOptionsCheck>;
  enqueue(
    handle: NativeCoreHandle,
    request: EnqueueRequest,
    sendKind: SendKind,
    trace: TraceArgument,
  ): Promise<NativeEnvelope<EnqueueOutcome>>;
  getResult(handle: NativeCoreHandle, taskId: string, wait: WaitBound): Promise<NativeEnvelope<ResultRead>>;
  taskInfo(handle: NativeCoreHandle, taskId: string): Promise<NativeEnvelope<TaskInfoRead>>;
  processRole(): NativeEnvelope<ProcessRoleName>;
  buildTaskError(parts: TaskErrorParts): NativeEnvelope<TaskErrorBuild>;
}

/** The `sends` functions of the addon. Throws as `requireAddon` does. */
export function sendsAddon(): SendsAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as SendsAddon;
}
