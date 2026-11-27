// The reads of the workflow tests through the client API: the start, the outcome of a workflow and of one node, the
// handle calls, and the sub-workflow failure that a node or a workflow stores. A read that fails, or an outcome of the
// other kind, fails the test with the workflow id and the cause. The codes are read with `errorCode` of
// `outcome.ts`.

import assert from "node:assert/strict";

import type {
  BuildReturn,
  SubWorkflowFailure,
  TaskError,
  TaskOutcome,
  WorkflowHandle,
  WorkflowHandleError,
  WorkflowId,
  WorkflowReadError,
  WorkflowStartError,
} from "../../../horsies/src/index.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import { errorCode, errorText } from "./outcome.ts";

/** The longest wait of a workflow read for a workflow that a running worker runs. */
export const WORKFLOW_WAIT_MS = 30_000;

/** A handle that reads a workflow's outcome: a started handle or a handle by id. */
export type OutcomeReader<O> = {
  readonly workflowId: WorkflowId;
  get(options?: { readonly waitMs?: number }): Promise<Result<TaskOutcome<O, string>, WorkflowReadError>>;
};

/** The text of a workflow read error, for a message. Pure. */
export function workflowReadErrorText(error: WorkflowReadError): string {
  switch (error.kind) {
    case "waitTimeout":
      return "the wait ended before the workflow";
    case "workflowNotFound":
      return "no workflow has the id";
    case "resultNotReady":
      return "the node stores no result yet";
    case "storedResultUnreadable":
      return `the stored result does not read: ${error.cause}`;
    case "unknownCustomCode":
      return `the stored error has the undeclared code ${error.storedCode}`;
    case "waitInsideTask":
      return "a wait inside a task";
    case "noTypeAvailable":
      return `the app does not list the workflow ${error.workflowName}`;
    case "databaseError":
      return `the database read failed: ${error.databaseCode}: ${error.message}`;
    case "invalidInput":
      return `the wait is not 0 to ${String(error.maxMs)} ms`;
    case "seatRelayFull":
      return `the seat relay did not send the read: ${error.message}`;
  }
}

/** The handle of a start of `subject`. Fails the test when the start fails. */
export async function startedHandle<R extends BuildReturn>(
  subject: string,
  start: Promise<Result<WorkflowHandle<R>, WorkflowStartError<R>>>,
): Promise<WorkflowHandle<R>> {
  const result = await start;

  if (!result.ok) {
    assert.fail(`start ${subject}: ${result.error.code}: ${result.error.message}`);
  }

  return result.value;
}

/** The outcome of the workflow of `handle` within `limitMs`. Fails the test when the read fails. */
export async function workflowOutcome<O>(handle: OutcomeReader<O>, limitMs: number = WORKFLOW_WAIT_MS): Promise<TaskOutcome<O, string>> {
  const read = await handle.get({ waitMs: limitMs });

  if (!read.ok) {
    assert.fail(`read the outcome of workflow ${handle.workflowId}: ${workflowReadErrorText(read.error)}`);
  }

  return read.value;
}

/** The value of a workflow that completed within `limitMs`. Fails the test for a workflow that failed, with its error. */
export async function workflowValue<O>(handle: OutcomeReader<O>, limitMs: number = WORKFLOW_WAIT_MS): Promise<O> {
  const outcome = await workflowOutcome(handle, limitMs);

  if (outcome.kind === "failed") {
    assert.fail(`workflow ${handle.workflowId} failed: ${errorText(outcome.error)}`);
  }

  return outcome.value;
}

/** The error of a workflow that failed within `limitMs`. Fails the test for a workflow that completed. */
export async function workflowError<O>(handle: OutcomeReader<O>, limitMs: number = WORKFLOW_WAIT_MS): Promise<TaskError> {
  const outcome = await workflowOutcome(handle, limitMs);

  if (outcome.kind === "succeeded") {
    assert.fail(`workflow ${handle.workflowId} completed with ${JSON.stringify(outcome.value)}`);
  }

  return outcome.error;
}

/** The code text of a workflow that failed within `limitMs`. */
export async function workflowCode<O>(handle: OutcomeReader<O>, limitMs: number = WORKFLOW_WAIT_MS): Promise<string> {
  return errorCode(await workflowError(handle, limitMs)).code;
}

/** The outcome of a node read of `subject` (`resultFor`). Fails the test when the read fails. */
export async function nodeOutcome<O, E extends string>(
  subject: string,
  read: Promise<Result<TaskOutcome<O, E>, WorkflowReadError>>,
): Promise<TaskOutcome<O, E>> {
  const result = await read;

  if (!result.ok) {
    assert.fail(`read ${subject}: ${workflowReadErrorText(result.error)}`);
  }

  return result.value;
}

/** The value of a node that succeeded. Fails the test for a failed node, with its error. */
export async function nodeValue<O, E extends string>(subject: string, read: Promise<Result<TaskOutcome<O, E>, WorkflowReadError>>): Promise<O> {
  const outcome = await nodeOutcome(subject, read);

  if (outcome.kind === "failed") {
    assert.fail(`${subject} failed: ${errorText(outcome.error)}`);
  }

  return outcome.value;
}

/** The error of a node that failed. Fails the test for a node that succeeded. */
export async function nodeError<O, E extends string>(
  subject: string,
  read: Promise<Result<TaskOutcome<O, E>, WorkflowReadError>>,
): Promise<TaskError<E>> {
  const outcome = await nodeOutcome(subject, read);

  if (outcome.kind === "succeeded") {
    assert.fail(`${subject} succeeded with ${JSON.stringify(outcome.value)}`);
  }

  return outcome.error;
}

/** The value of a handle call of `subject` (`status`, `tasks`, `results`, `pause`, `resume`, `cancel`). */
export async function handleCall<T>(subject: string, call: Promise<Result<T, WorkflowHandleError>>): Promise<T> {
  const result = await call;

  if (!result.ok) {
    assert.fail(`${subject}: ${result.error.code}: ${result.error.message}`);
  }

  return result.value;
}

/**
 * The sub-workflow failure of `error` (Rust `subworkflow_failure`, Python `_assert_subworkflow_error`): the code
 * `SUB_WORKFLOW_FAILED`, the child `childId`, which FAILED with at least one failed task. Fails the test otherwise.
 */
export function subWorkflowFailure(error: TaskError, childId: string): SubWorkflowFailure {
  assert.deepEqual(errorCode(error), { source: "builtIn", code: "SUB_WORKFLOW_FAILED" }, errorText(error));

  const failure = error.subWorkflow;

  assert.ok(failure !== null, `the error is not a sub-workflow failure: ${errorText(error)}`);
  assert.equal(failure.workflowId, childId);
  assert.equal(failure.summary.status, "FAILED");
  assert.ok(failure.summary.failedTasks >= 1, `the child has a failed task: ${JSON.stringify(failure.summary)}`);

  return failure;
}

/** The sub-workflow failure of a failed outcome. Fails the test for an outcome that succeeded. */
export function failedChildOf<O>(outcome: TaskOutcome<O>, childId: string): SubWorkflowFailure {
  if (outcome.kind === "succeeded") {
    assert.fail(`expected the failure of child ${childId}, got ${JSON.stringify(outcome.value)}`);
  }

  return subWorkflowFailure(outcome.error, childId);
}
