// The functions of the `sends` area of the addon, their records, and the accessor.
//
// - The check functions (`checkAppConfig`, `parseDeclaredErrorCodes`, `checkTaskOptions`) and the classes that they
//   give are pure: they start no core, open no connection and read no clock, so `Horsies.create` can call them before
//   a child host shapes the process runtime.
// - `prepareSendFacts` mints a task id and starts the log forwarder of the process, so the payload warning reaches the
//   log sink; it starts no core. `buildTaskError` starts the log forwarder too, so the record of data that the codec
//   cannot write reaches the log sink; it starts no core.
// - `enqueue`, `getResult` and `taskInfo` are operations on a core handle: they start the core of the process at the
//   first call and run on its runtime.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { InstallCoverageArg, NativeCoreHandle } from "../handle/addon.ts";
import type { RawCatalogError, TraceArgument } from "../values/records.ts";

/** How to fix a refused value: no line, or one line, as the core's error holds it. */
export type RefusalHelp = { readonly kind: "noHelp" } | { readonly kind: "help"; readonly line: string };

/** One refusal of a core check: the catalog code, the core's message, its notes and its help. */
export interface CoreRefusal {
  /** A code of the core's catalog, as its text (`HRS-102`). */
  readonly code: string;
  readonly message: string;
  readonly notes: string[];
  readonly help: RefusalHelp;
}

/**
 * The outcome of the check of an app config (the JSON text of the core's `AppConfig`; a field that the text leaves
 * out takes the core's default):
 * - `accepted`: the send rules of the config, the JSON text of the producer manifest of its broker settings, and the
 *   coverage values of its retention for `CoreHandle.withInstallCoverage`;
 * - `refused`: every refusal, in the core's order.
 *
 * A text that the core's config reader refuses throws `ClientDefect`: the package writes the text from a parsed config.
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

/** The retry policy of a task. Each code of `autoRetryFor` is a custom code or a built-in code, as its text. */
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

/** The facts of one send that the send rules judge. The task name and the queue are the checked options' own. */
export interface SendFacts {
  readonly retention: RetentionChoice;
  readonly deadline: DeadlineFact;
  /** The UTF-8 byte length of the input text. */
  readonly inputBytes: number;
}

/**
 * The priority that a send carries: the one queue of the default queue mode (the package holds its priority), or a
 * custom queue with its priority in the core's accepted config (the core's default for a queue that names none).
 */
export type SendPriority = { readonly kind: "defaultQueue" } | { readonly kind: "configured"; readonly priority: number };

/**
 * The prepared facts of one send: a new task id, the stored options text (with the deadline), the retention and the
 * priority, or the send error of a refused fact (`TaskSendValidationFailed` for a retention that the config does not
 * register or a refused id, `TaskSendPayloadTooLarge` for an input above the reject bound).
 */
export type PreparedSend =
  | {
      readonly kind: "prepared";
      readonly taskId: string;
      readonly taskOptions: string;
      readonly retention: RetentionRecord;
      readonly priority: SendPriority;
    }
  | { readonly kind: "refused"; readonly error: RawCatalogError };

/** The rules of an accepted app config that each send applies: retention, the payload bounds and the queues. */
export interface NativeSendRules {
  /**
   * The prepared facts of one send of the task of the checked `options`, on its queue. Logs the payload warning of
   * the core once per task name when the input is above the warn bound. Mints a task id.
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

/** The status of a task that has not finished. */
export type LiveStatusName = "PENDING" | "CLAIMED" | "RUNNING";

/** The status of a finished task. */
export type FinishedStatusName = "COMPLETED" | "FAILED" | "CANCELLED" | "EXPIRED";

/**
 * The stored result of a task when the read ended, with the status of the task:
 * - `stored`: the row stores this result envelope text;
 * - `waitEnded`: the wait ended before the task finished, so no result is stored yet;
 * - `finishedWithoutResult`: the task finished and stores no result (a cancel before a start, an expiry, a terminal
 *   row of the live table).
 */
export type EnvelopeFact =
  | { readonly kind: "stored"; readonly status: TaskStatusName; readonly text: string }
  | { readonly kind: "waitEnded"; readonly status: LiveStatusName }
  | { readonly kind: "finishedWithoutResult"; readonly status: FinishedStatusName };

/** One task and its stored result. */
export interface ResultRecord {
  readonly taskId: string;
  readonly taskName: string;
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

/** The code of an error that a body gives: a custom code that the task chose, or a built-in code. */
export type ErrorCodePart =
  | { readonly kind: "custom"; readonly code: string }
  | { readonly kind: "builtIn"; readonly code: string };

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

/** Why the codec cannot write the `data` of an error: a value that is not JSON-native, or an integer that is too long. */
export type DataRefusedReason = "notJsonNative" | "integerTooLong";

/**
 * The `data` of a body error: none, the codec's stored `data` text, or data that the codec cannot write. The bridge
 * stores the last as `{"data_refused": "<cause>"}` with Python's cause, keeps the code and the message, and logs an
 * ERROR record on `horsies.codec`.
 */
export type ErrorDataPart =
  | { readonly kind: "noData" }
  | { readonly kind: "text"; readonly text: string }
  | { readonly kind: "notStorable"; readonly reason: DataRefusedReason };

/** The parts of the error of a body. */
export interface TaskErrorParts {
  readonly code: ErrorCodePart;
  readonly message: OptionalText;
  readonly data: ErrorDataPart;
  readonly exception: ExceptionPart;
}

/** The core's task error of a body, built from its parts. */
export interface NativeTaskError {
  /** The stored error object text, as the core writes it into a result envelope. */
  storedText(): NativeEnvelope<string>;
}

/** The outcome of a task error build: the error, or why the core refuses the parts (a reserved custom code). */
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
