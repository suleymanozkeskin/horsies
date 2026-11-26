// expect: TS2367 at 8
// The tag of a task error is on the error itself, `custom` or `builtIn`: `library` is no tag.

import type { TaskOutcome } from "../../../horsies/src/index.ts";

/** Whether horsies chose the code of a failed outcome. */
export function isBuiltIn(outcome: TaskOutcome<number, "EMPTY_ORDER">): boolean {
  return outcome.kind === "failed" && outcome.error.kind === "library";
}
