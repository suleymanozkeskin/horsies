// The outcome of a task and its stored error: what a task read, an `h.taskOutcome` parameter and a workflow read give.

import { WorkflowStatus } from "../_catalog/status.ts";
import type { BuiltInTaskCode } from "../_catalog/task_code.ts";
import type { JsonValue } from "../codec/json-value.ts";

// A task error code of horsies (a timeout, a crash, a cancel, an unhandled exception, and others): the core's built-in
// task codes of the generated catalog, the name of the Python and Rust clients. The generated event types name the
// same type.
export type { BuiltInTaskCode };

// The status of a workflow: the core's `WorkflowStatus` of the generated catalog, a value and a type of the same name,
// as `TaskStatus` is.
export { WorkflowStatus };

/** The statuses of a workflow, in the core's order. */
export const WORKFLOW_STATUSES: ReadonlyArray<WorkflowStatus> = Object.freeze(Object.values(WorkflowStatus));

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
  readonly totalNodes: number;
  readonly completedNodes: number;
  readonly failedNodes: number;
  readonly skippedNodes: number;
  /** The message of the child's stored error, `null` when it has none. */
  readonly errorSummary: string | null;
};

/** The child workflow of a sub-workflow node that failed. */
export type SubWorkflowFailure = { readonly workflowId: string; readonly summary: SubWorkflowSummary };

/**
 * The error of a task, told apart by `kind`: `custom` holds a code that the task declares (`E`), `builtIn` a code of
 * horsies (`BuiltInTaskCode`).
 */
export type TaskError<E extends string = string> =
  | {
      readonly kind: "custom";
      readonly code: E;
      /** The message, `null` when the error has none. */
      readonly message: string | null;
      /** The error data, `null` when the error has none. */
      readonly data: JsonValue;
      /** The exception record, `null` when horsies recorded none. */
      readonly exception: ExceptionRecord | null;
      /** The failed child workflow, `null` for an error that is not a sub-workflow failure. */
      readonly subWorkflow: SubWorkflowFailure | null;
    }
  | {
      readonly kind: "builtIn";
      readonly code: BuiltInTaskCode;
      /** The message, `null` when the error has none. */
      readonly message: string | null;
      /** The error data, `null` when the error has none. */
      readonly data: JsonValue;
      /** The exception record, `null` when horsies recorded none. */
      readonly exception: ExceptionRecord | null;
      /** The failed child workflow, `null` for an error that is not a sub-workflow failure. */
      readonly subWorkflow: SubWorkflowFailure | null;
    };

/** The outcome of a task: its value, or its error. */
export type TaskOutcome<O, E extends string = string> =
  | { readonly kind: "succeeded"; readonly value: O }
  | { readonly kind: "failed"; readonly error: TaskError<E> };
