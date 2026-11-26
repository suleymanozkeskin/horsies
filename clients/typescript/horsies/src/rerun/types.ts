// The command, the policy and the outcome of a rerun: a new task from the stored input of a retained
// terminal task.

import type { TaskAbsenceReason } from "../_catalog/history.ts";
import type { TaskId } from "../contract/ids.ts";

/** The deadline of the new task: an instant, or the explicit choice of no deadline. */
export type RerunDeadline = { readonly kind: "until"; readonly at: Temporal.Instant } | { readonly kind: "none" };

/** One rerun request. */
export interface RerunCommand {
  /** The task to run again. */
  readonly sourceTaskId: TaskId;
  /** The deadline of the new task. A part below the microsecond is cut toward the past. */
  readonly deadline: RerunDeadline;
  /**
   * The caller key of the new task: a second rerun with the same key and the same command answers
   * `keyReplay`. Omitted: an unkeyed rerun.
   */
  readonly callerKey?: string;
}

/** The retention of the new task: a finite period of whole days, or forever. */
export type RerunRetention = { readonly kind: "days"; readonly days: number } | { readonly kind: "forever" };

/** Whether the new task keeps its input for a later rerun of it. */
export type RerunInputChoice = "keep" | "drop";

/** The enqueue policy of the new task. */
export interface RerunPolicy {
  readonly retention: RerunRetention;
  readonly rerunInput: RerunInputChoice;
  /**
   * How long the caller key holds the rerun: more than 0, at most 30 days, whole microseconds. Days,
   * hours and smaller units only: a year, a month or a week has no fixed length.
   */
  readonly reservationWindow: Temporal.Duration;
}

/** Why a history source is not eligible for a rerun. */
export type RerunNotEligibleReason = "COMPLETED_SOURCE" | "WORKFLOW_TASK";

/** The new task exists. The source task is not changed. */
export interface RerunEnqueued {
  readonly kind: "enqueued";
  readonly newTaskId: TaskId;
  readonly sourceTaskId: TaskId;
  /** The first task of the rerun chain: the source, or the root of the source. */
  readonly rerunRootTaskId: TaskId;
}

/** The source task is live: its own lifecycle owns its retry. Nothing is written. */
export interface RerunSourceLive {
  readonly kind: "sourceLive";
  readonly taskId: TaskId;
}

/** No live or history record has the source id. `reason` says whether retention can explain it. Nothing is written. */
export interface RerunSourceAbsent {
  readonly kind: "sourceAbsent";
  readonly taskId: TaskId;
  readonly reason: TaskAbsenceReason;
}

/** The history row of the source does not decode. Nothing is written; a repeat gives the same answer. */
export interface RerunSourceCorrupt {
  readonly kind: "sourceCorrupt";
  readonly taskId: TaskId;
  readonly detail: string;
}

/** The source is a workflow task, or it completed. Nothing is written. */
export interface RerunNotEligible {
  readonly kind: "notEligible";
  readonly taskId: TaskId;
  readonly reason: RerunNotEligibleReason;
}

/** The source stores no input. `disposition` is the stored reason, for example `DECLINED_BY_POLICY`. Nothing is written. */
export interface RerunInputUnavailable {
  readonly kind: "inputUnavailable";
  readonly taskId: TaskId;
  readonly disposition: string;
}

/** The stored input or its options fail their checks. Nothing is written. */
export interface RerunInputCorrupt {
  readonly kind: "inputCorrupt";
  readonly taskId: TaskId;
  readonly detail: string;
}

/** The caller key holds another command, the source's own included. Nothing is written. */
export interface RerunKeyConflict {
  readonly kind: "keyConflict";
  readonly taskId: TaskId;
  readonly reservedByTaskId: TaskId;
}

/** The caller key holds this rerun command already, as `existingTaskId`. Nothing is written. */
export interface RerunKeyReplay {
  readonly kind: "keyReplay";
  readonly existingTaskId: TaskId;
}

/** The outcome of one rerun. Only `enqueued` writes. */
export type RerunOutcome =
  | RerunEnqueued
  | RerunSourceLive
  | RerunSourceAbsent
  | RerunSourceCorrupt
  | RerunNotEligible
  | RerunInputUnavailable
  | RerunInputCorrupt
  | RerunKeyConflict
  | RerunKeyReplay;
