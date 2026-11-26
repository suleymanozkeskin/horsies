// The two fields of a stored sub-workflow error: `sub_workflow_id` and `sub_workflow_summary` (the core's
// `SubWorkflowFailure` and `SubWorkflowSummary`, `core/src/core/workflow/summary.rs`). A summary key that the core
// does not name is ignored, as the core ignores it.

import { err, ok, type Result } from "../wire/result.ts";
import type { SubWorkflowFailure, SubWorkflowSummary, WorkflowStatus } from "../wire/task-error.ts";
import { SUMMARY_COUNT_MAX, SUMMARY_COUNT_MIN, SUMMARY_OUTPUT_DEPTH_LIMIT, WORKFLOW_STATUSES } from "./bounds.ts";
import { decodeJsonNode } from "./json-value.ts";
import { isTreeObject, type TreeObject, type TreeValue } from "./number-text.ts";
import { type DecodeRefused, type EnvelopeRefused, envelopeRefused, type EnvelopeRule, memberPath } from "./refusal.ts";
import { isNumber, isString } from "./values.ts";

const SUB_WORKFLOW_ID_KEY = "sub_workflow_id";
const SUB_WORKFLOW_SUMMARY_KEY = "sub_workflow_summary";
const COUNT_KEYS = ["total_tasks", "completed_tasks", "failed_tasks", "skipped_tasks"] as const;

const STATUSES: ReadonlySet<string> = new Set(WORKFLOW_STATUSES);

/**
 * The forms of a UUID that the core reads (the `uuid` crate's `Uuid::parse_str`): 32 hex digits, the hyphenated
 * 8-4-4-4-12 form, the hyphenated form in braces, and the hyphenated form after `urn:uuid:` (any case). Hex digits
 * of either case.
 */
const UUID_FORMS: readonly RegExp[] = [
  /^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$/i,
  /^([0-9a-f]{8})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{12})$/i,
  /^\{([0-9a-f]{8})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{12})\}$/i,
  /^urn:uuid:([0-9a-f]{8})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{4})-([0-9a-f]{12})$/i,
];

function refuse(rule: EnvelopeRule, path: string, detail: string): Result<never, EnvelopeRefused> {
  return err(envelopeRefused(rule, path, detail));
}

/** The failed child workflow of a stored sub-workflow error at `path`. Both fields are required. Pure. */
export function readSubWorkflow(
  idNode: TreeValue | undefined,
  summaryNode: TreeValue | undefined,
  path: string,
): Result<SubWorkflowFailure, DecodeRefused> {
  const idPath = memberPath(path, SUB_WORKFLOW_ID_KEY);
  const summaryPath = memberPath(path, SUB_WORKFLOW_SUMMARY_KEY);

  const workflowId = uuidText(idNode, idPath);

  if (!workflowId.ok) {
    return workflowId;
  }

  if (summaryNode === undefined) {
    return refuse("ErrorMissingField", summaryPath, "the summary is missing");
  }

  const summary = readSubWorkflowSummary(summaryNode, summaryPath);

  return summary.ok ? ok(Object.freeze({ workflowId: workflowId.value, summary: summary.value })) : summary;
}

/** The lower-case 8-4-4-4-12 form of a UUID text in a form that the core reads. */
function uuidText(node: TreeValue | undefined, path: string): Result<string, EnvelopeRefused> {
  if (isString(node)) {
    for (const form of UUID_FORMS) {
      const match = form.exec(node);

      if (match !== null) {
        return ok(match.slice(1).join("-").toLowerCase());
      }
    }
  }

  return refuse("ErrorFieldInvalid", path, "not a UUID in a form that the core reads");
}

function isWorkflowStatus(text: string): text is WorkflowStatus {
  return STATUSES.has(text);
}

/** A stored sub-workflow summary at `path` (the core's `SubWorkflowSummary`). Pure. */
export function readSubWorkflowSummary(node: TreeValue, path: string): Result<SubWorkflowSummary, DecodeRefused> {
  if (!isTreeObject(node)) {
    return refuse("ErrorFieldInvalid", path, "the summary is not an object");
  }

  const status = node["status"];

  if (!isString(status) || !isWorkflowStatus(status)) {
    return refuse("ErrorFieldInvalid", memberPath(path, "status"), "not a workflow status");
  }

  const errorSummary = node["error_summary"] ?? null;

  if (errorSummary !== null && !isString(errorSummary)) {
    return refuse("ErrorFieldInvalid", memberPath(path, "error_summary"), "not a string or null");
  }

  const outputPath = memberPath(path, "output");
  const output = decodeJsonNode(node["output"] ?? null, outputPath, 0, SUMMARY_OUTPUT_DEPTH_LIMIT);

  if (!output.ok) {
    return refuse("ErrorDataRefused", outputPath, `the output breaks a rule of a user value: ${output.error.kind}`);
  }

  const counts = readCounts(node, path);

  return counts.ok ? ok(Object.freeze({ status, output: output.value, errorSummary, ...counts.value })) : counts;
}

type Counts = Pick<SubWorkflowSummary, "totalTasks" | "completedTasks" | "failedTasks" | "skippedTasks">;

/** The four counts: each an integer in the range that the core stores (`i32`). */
function readCounts(node: TreeObject, path: string): Result<Counts, EnvelopeRefused> {
  const values: number[] = [];

  for (const key of COUNT_KEYS) {
    const count = node[key];

    if (!isNumber(count) || !Number.isInteger(count) || count < SUMMARY_COUNT_MIN || count > SUMMARY_COUNT_MAX) {
      return refuse("ErrorFieldInvalid", memberPath(path, key), "a count is not an integer of the stored range");
    }

    values.push(count);
  }

  const [totalTasks = 0, completedTasks = 0, failedTasks = 0, skippedTasks = 0] = values;

  return ok({ totalTasks, completedTasks, failedTasks, skippedTasks });
}
