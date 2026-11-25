// The user-side conversion and check of a kind. A schema front end (the zod adapter) fills them: a conversion is a
// zod codec, a check is a zod refinement. The codec runs them; it does not know where they come from.

import type { Result } from "../wire/result.ts";

/** One reason why a conversion or a check refused a value: where (a path inside the value) and which rule. */
export type CheckIssue = { readonly path: string; readonly code: string };

/** A conversion or a check refused a value. The issues hold no value. */
export type CheckFailure = { readonly issues: readonly CheckIssue[] };

/** The value that a body gets, from the stored value (a zod codec's decode). */
export type DecodeConversion = (stored: unknown) => Result<unknown, CheckFailure>;

/** The stored value, from the value that a sender gives (a zod codec's encode). */
export type EncodeConversion = (value: unknown) => Result<unknown, CheckFailure>;

/** A check of a value (a zod refinement). It runs before an encode and after a decode in the worker form. */
export type ValueCheck = (value: unknown) => Result<true, CheckFailure>;

/** The text of the issues of a failure: `<path> <code>`, joined by `; `. It holds no value. Pure. */
export function issuesText(failure: CheckFailure): string {
  return failure.issues.map((issue) => `${issue.path} ${issue.code}`).join("; ");
}
