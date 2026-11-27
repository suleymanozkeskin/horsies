// The info of a task: its status, times, failed reason and every attempt, newest first. The result is read by `get`.

import type { Execution } from "../_catalog/monitoring_models.ts";
import type { TaskAttemptOutcome, TaskStatus } from "../_catalog/status.ts";
import type {
  ExecutionRecord,
  RecordedCount,
  RecordedInstant,
  RecordedText,
  TaskAttemptRecord,
  TaskInfoRecord,
} from "../native/sends/addon.ts";
import { type TaskId, taskIdFromCore } from "../contract/ids.ts";

/** A fact that a task row may not hold. */
export type Recorded<T> = { readonly kind: "recorded"; readonly value: T } | { readonly kind: "notRecorded" };

/** How a worker runs the task: the class of a live task; a task read from history does not keep it. */
export type ExecutionFact = { readonly kind: "class"; readonly execution: Execution } | { readonly kind: "notKeptInHistory" };

/** One attempt of a task. */
export interface TaskAttempt {
  readonly attempt: number;
  readonly outcome: TaskAttemptOutcome;
  readonly willRetry: boolean;
  readonly startedAt: Temporal.Instant;
  readonly finishedAt: Temporal.Instant;
  readonly errorCode: Recorded<string>;
  readonly errorMessage: Recorded<string>;
  readonly failedReason: Recorded<string>;
  readonly workerId: Recorded<string>;
  readonly workerHostname: Recorded<string>;
  readonly workerPid: Recorded<number>;
  readonly workerProcessName: Recorded<string>;
}

/** The info of a task. */
export interface TaskInfo {
  readonly taskId: TaskId;
  readonly taskName: string;
  readonly status: TaskStatus;
  readonly queue: string;
  readonly priority: number;
  readonly retryCount: number;
  readonly maxRetries: number;
  readonly nextRetryAt: Recorded<Temporal.Instant>;
  readonly sentAt: Recorded<Temporal.Instant>;
  readonly dueAt: Temporal.Instant;
  readonly claimedAt: Recorded<Temporal.Instant>;
  readonly startedAt: Recorded<Temporal.Instant>;
  readonly completedAt: Recorded<Temporal.Instant>;
  readonly failedAt: Recorded<Temporal.Instant>;
  readonly workerHostname: Recorded<string>;
  readonly workerPid: Recorded<number>;
  readonly workerProcessName: Recorded<string>;
  readonly errorCode: Recorded<string>;
  readonly failedReason: Recorded<string>;
  /** Every attempt, newest first. */
  readonly attempts: readonly TaskAttempt[];
  readonly execution: ExecutionFact;
}

const NANOSECONDS_PER_MICROSECOND = 1000n;

const NOT_RECORDED: Recorded<never> = Object.freeze({ kind: "notRecorded" });

/** The instant of epoch microseconds. Pure. */
export function instantOfMicros(epochMicros: bigint): Temporal.Instant {
  return Temporal.Instant.fromEpochNanoseconds(epochMicros * NANOSECONDS_PER_MICROSECOND);
}

/** The frozen info of a record of the core. Pure. */
export function taskInfoOf(record: TaskInfoRecord): TaskInfo {
  const info: TaskInfo = {
    taskId: taskIdFromCore(record.taskId, "taskInfo"),
    taskName: record.taskName,
    status: record.status,
    queue: record.queue,
    priority: record.priority,
    retryCount: record.retryCount,
    maxRetries: record.maxRetries,
    nextRetryAt: recordedInstant(record.nextRetryAt),
    sentAt: recordedInstant(record.sentAt),
    dueAt: instantOfMicros(record.dueAtMicros),
    claimedAt: recordedInstant(record.claimedAt),
    startedAt: recordedInstant(record.startedAt),
    completedAt: recordedInstant(record.completedAt),
    failedAt: recordedInstant(record.failedAt),
    workerHostname: recordedText(record.workerHostname),
    workerPid: recordedCount(record.workerPid),
    workerProcessName: recordedText(record.workerProcessName),
    errorCode: recordedText(record.errorCode),
    failedReason: recordedText(record.failedReason),
    attempts: Object.freeze(record.attempts.map((attempt) => attemptOf(attempt))),
    execution: executionFact(record.execution),
  };

  return Object.freeze(info);
}

function attemptOf(record: TaskAttemptRecord): TaskAttempt {
  return Object.freeze({
    attempt: record.attempt,
    outcome: record.outcome,
    willRetry: record.willRetry,
    startedAt: instantOfMicros(record.startedAtMicros),
    finishedAt: instantOfMicros(record.finishedAtMicros),
    errorCode: recordedText(record.errorCode),
    errorMessage: recordedText(record.errorMessage),
    failedReason: recordedText(record.failedReason),
    workerId: recordedText(record.workerId),
    workerHostname: recordedText(record.workerHostname),
    workerPid: recordedCount(record.workerPid),
    workerProcessName: recordedText(record.workerProcessName),
  });
}

function recordedInstant(fact: RecordedInstant): Recorded<Temporal.Instant> {
  return fact.kind === "recorded" ? Object.freeze({ kind: "recorded", value: instantOfMicros(fact.epochMicros) }) : NOT_RECORDED;
}

function recordedText(fact: RecordedText): Recorded<string> {
  return fact.kind === "recorded" ? Object.freeze({ kind: "recorded", value: fact.text }) : NOT_RECORDED;
}

function recordedCount(fact: RecordedCount): Recorded<number> {
  return fact.kind === "recorded" ? Object.freeze({ kind: "recorded", value: fact.value }) : NOT_RECORDED;
}

function executionFact(record: ExecutionRecord): ExecutionFact {
  switch (record) {
    case "NOT_KEPT_IN_HISTORY":
      return Object.freeze({ kind: "notKeptInHistory" });
    case "ISOLATED":
    case "SHARED_ASYNC":
    case "SHARED_THREAD":
      return Object.freeze({ kind: "class", execution: record });
    default: {
      const unexpected: never = record;

      throw new TypeError(`taskInfoOf: an execution outside its type: ${String(unexpected)}`);
    }
  }
}
