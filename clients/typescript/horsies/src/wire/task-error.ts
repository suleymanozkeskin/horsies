// The outcome of a task and its stored error: what a task read, an `h.resultOf` parameter and a workflow read give.

import type { BuiltInTaskCode as LibraryTaskCode } from "../_catalog/task_code.ts";
import type { WORKFLOW_STATUSES } from "../codec/bounds.ts";
import type { JsonValue } from "../codec/json-value.ts";

// A task error code that the library records (a timeout, a crash, a cancel, an unhandled exception, and others): the
// core's built-in task codes of the generated catalog, under the one public name of the set. The generated event types
// name the same type.
export type { LibraryTaskCode };

/** The status of a workflow. */
export type WorkflowStatus = (typeof WORKFLOW_STATUSES)[number];

/** The code of a task error: a library code, or a code that the task chose (`E`). */
export type TaskErrorCode<E extends string = string> =
  | { readonly kind: "library"; readonly code: LibraryTaskCode }
  | { readonly kind: "user"; readonly code: E };

/** The stored record of an exception: the class name, its module, the message, the repr and the traceback. */
export type ExceptionRecord = {
  readonly type: string;
  readonly module: string;
  readonly message: string;
  readonly repr: string;
  readonly traceback: string;
};

/** The summary of a child workflow that failed. */
export type SubWorkflowSummary = {
  readonly status: WorkflowStatus;
  /** The child's output, `null` when it has none. */
  readonly output: JsonValue;
  readonly totalTasks: number;
  readonly completedTasks: number;
  readonly failedTasks: number;
  readonly skippedTasks: number;
  /** The message of the child's stored error, `null` when it has none. */
  readonly errorSummary: string | null;
};

/** The child workflow of a sub-workflow node that failed. */
export type SubWorkflowFailure = { readonly workflowId: string; readonly summary: SubWorkflowSummary };

/** The error of a task. `E` is the type of a code that the task chose. */
export type TaskError<E extends string = string> = {
  readonly code: TaskErrorCode<E>;
  /** The message, `null` when the error has none. */
  readonly message: string | null;
  /** The error data, `null` when the error has none. */
  readonly data: JsonValue;
  /** The exception record, `null` when the library recorded none. */
  readonly exception: ExceptionRecord | null;
  /** The failed child workflow, `null` for an error that is not a sub-workflow failure. */
  readonly subWorkflow: SubWorkflowFailure | null;
};

/** The outcome of a task: its value, or its error. */
export type TaskOutcome<O, E extends string = string> =
  | { readonly kind: "succeeded"; readonly value: O }
  | { readonly kind: "failed"; readonly error: TaskError<E> };
