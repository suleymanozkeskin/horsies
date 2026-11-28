// The user-side conversion and check of a kind. A schema front end (the zod adapter) fills them: a conversion is a
// zod codec, a check is a zod refinement. The codec runs them; it does not know where they come from.

import type { Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";

/** The issue codes that horsies makes. zod's own issue codes are not in this set. */
export type HorsiesIssueCode =
  /** A check or a conversion returned a promise: the codec runs them synchronously. */
  | "async_check"
  /** A one-way transform met in the encode direction. */
  | "one_way_transform"
  /** A key of a record that its enum key does not hold. */
  | "missing_key";

/**
 * One reason why a conversion or a check refused a value: where (a path inside the value) and which rule. A part of
 * the path that comes from the value (a record key) must be written `<input>`, as in the codec's own paths: the
 * text of a refusal holds no value. One variant for each source of issues:
 * - `zodIssue`: an issue of a zod check or parse; `code` is zod's issue code, kept as text.
 * - `horsiesIssue`: an issue that the zod adapter makes; `code` is one of `HorsiesIssueCode`.
 * - `notNoWorkflowOutput`: the value is not the `NoWorkflowOutput` value of a workflow with no output node.
 */
export type CheckIssue =
  | { readonly kind: "zodIssue"; readonly path: string; readonly code: string }
  | { readonly kind: "horsiesIssue"; readonly path: string; readonly code: HorsiesIssueCode }
  | { readonly kind: "notNoWorkflowOutput"; readonly path: string };

/** A conversion or a check refused a value. The issues hold no value. */
export type CheckFailure = { readonly issues: readonly CheckIssue[] };

/** The value that a body gets, from the stored value (a zod codec's decode). */
export type DecodeConversion = (stored: unknown) => Result<unknown, CheckFailure>;

/** The stored value, from the value that a sender gives (a zod codec's encode). */
export type EncodeConversion = (value: unknown) => Result<unknown, CheckFailure>;

/**
 * A check of a value (a zod refinement). At encode it runs after the writer wrote the value's kind, on the value with
 * its defaults filled; at decode it runs after the reader read the kind, in the worker form only.
 */
export type ValueCheck = (value: unknown) => Result<true, CheckFailure>;

/** The text of the issues of a failure: `<path> <code>`, joined by `; `. It holds no value. Pure. */
export function issuesText(failure: CheckFailure): string {
  return failure.issues.map((issue) => `${issue.path} ${issueCodeText(issue)}`).join("; ");
}

/** The code of an issue, as text. Pure. */
function issueCodeText(issue: CheckIssue): string {
  switch (issue.kind) {
    case "zodIssue":
    case "horsiesIssue":
      return issue.code;
    case "notNoWorkflowOutput":
      return 'expected "noWorkflowOutput"';
    default:
      return unreachable(issue, "issueCodeText");
  }
}
