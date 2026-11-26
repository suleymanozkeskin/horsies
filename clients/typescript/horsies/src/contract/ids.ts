// The id types of tasks and workflows: a UUID text in the lower-case 8-4-4-4-12 form, which only `parse` (or the
// library) makes. A plain string is not an id, so a read from a stored id refuses a bad text before any call.

import { isUuidText } from "../codec/values.ts";
import { err, ok, type Result } from "../wire/result.ts";

declare const taskIdBrand: unique symbol;
declare const workflowIdBrand: unique symbol;

/** The id of a task: a UUID text in the lower-case 8-4-4-4-12 form. */
export type TaskId = string & { readonly [taskIdBrand]: "TaskId" };

/** The id of a workflow: a UUID text in the lower-case 8-4-4-4-12 form. */
export type WorkflowId = string & { readonly [workflowIdBrand]: "WorkflowId" };

/** Why a text is not an id. `detail` names the rule; it never holds the text. */
export type IdRefused = { readonly kind: "notCanonicalUuid"; readonly detail: string };

const NOT_CANONICAL: IdRefused = Object.freeze({
  kind: "notCanonicalUuid",
  detail: "an id is a UUID in the lower-case 8-4-4-4-12 form",
});

/** Parse a task id. Pure. */
function parseTaskId(text: string): Result<TaskId, IdRefused> {
  // SAFETY: the brand is the fact that the text has the canonical UUID form, checked on this line.
  return isUuidText(text) ? ok(text as TaskId) : err(NOT_CANONICAL);
}

/** Parse a workflow id. Pure. */
function parseWorkflowId(text: string): Result<WorkflowId, IdRefused> {
  // SAFETY: the brand is the fact that the text has the canonical UUID form, checked on this line.
  return isUuidText(text) ? ok(text as WorkflowId) : err(NOT_CANONICAL);
}

/** The parser of task ids. */
export const TaskId: { readonly parse: (text: string) => Result<TaskId, IdRefused> } = Object.freeze({
  parse: parseTaskId,
});

/** The parser of workflow ids. */
export const WorkflowId: { readonly parse: (text: string) => Result<WorkflowId, IdRefused> } = Object.freeze({
  parse: parseWorkflowId,
});

/**
 * The task id of a text that the core gave (a minted id, a stored id). The core writes the canonical form; another
 * text is a defect: `TypeError`.
 */
export function taskIdFromCore(text: string, subject: string): TaskId {
  const parsed = parseTaskId(text);

  if (!parsed.ok) {
    throw new TypeError(`${subject}: the core gave a task id that is not a canonical UUID text`);
  }

  return parsed.value;
}
