// The code of a task error. This module is the one place where the showcase reads the code shape of an error
// (`error.code.kind` and `error.code.code`); every other module calls `failureCode`, so a change of that shape changes
// this file only.

import type { JsonValue, TaskError } from "horsies";

/** Who chose the code of a task error: the task (`user`) or the library (`library`). */
export type CodeSource = "user" | "library";

/** The code of a task error and who chose it. */
export type FailureCode = { readonly source: CodeSource; readonly code: string };

/** The code of `error`. Pure. */
export function failureCode(error: TaskError): FailureCode {
  return { source: error.code.kind, code: error.code.code };
}

/** The code text of `error`, for example `CARD_DECLINED` or `UNHANDLED_EXCEPTION`. Pure. */
export function failureCodeText(error: TaskError): string {
  return failureCode(error).code;
}

/** `CODE: message`, or the code alone for an error with no message. Pure. */
export function failureText(error: TaskError): string {
  const code = failureCodeText(error);

  return error.message === null ? code : `${code}: ${error.message}`;
}

/** The message and the data of an `UPSTREAM_FAILED` error of a node whose dependency `error` failed. Pure. */
export function upstreamFailure(error: TaskError): { readonly message: string; readonly data: JsonValue } {
  return { message: `a dependency failed: ${failureText(error)}`, data: { upstreamCode: failureCodeText(error) } };
}
