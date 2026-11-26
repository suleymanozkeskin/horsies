// The success value of a sub-workflow node whose child has no output node. The core stores that node's result as
// `{"ok": null}` (Python's `_completed_child_node` gives `Ok(None)`); the client reads the stored `null` as the frozen
// `NO_WORKFLOW_OUTPUT` value, so no reader sees `null` as a sentinel.
//
// A workflow with no `outputType` keeps this wire type as the output of its params contract. The node checks, the
// read of a sub-workflow node and an `h.taskOutcome` parameter of the workflow then read and compare it as any output.

import type { CheckFailure } from "../codec/conversion.ts";
import { isObjectLike } from "../codec/values.ts";
import { conversionKind, NULL_KIND } from "../wire/kinds.ts";
import { err, ok } from "../wire/result.ts";
import { type Wire, wireOf } from "../wire/wire-type.ts";
import type { NoWorkflowOutput } from "./node.ts";

/** The one value of `NoWorkflowOutput`. */
export const NO_WORKFLOW_OUTPUT: NoWorkflowOutput = Object.freeze({ kind: "noWorkflowOutput" });

/** The refusal of a value that is not `NoWorkflowOutput`. It holds no value. */
const NOT_NO_WORKFLOW_OUTPUT: CheckFailure = Object.freeze({
  issues: Object.freeze([Object.freeze({ path: "kind", code: 'expected "noWorkflowOutput"' })]),
});

/** The wire type of the success value of a sub-workflow node of a workflow with no output node. */
export const NO_WORKFLOW_OUTPUT_WIRE: Wire<NoWorkflowOutput> = wireOf<NoWorkflowOutput>(
  conversionKind(
    NULL_KIND,
    // The stored kind admits only `null`, so every stored value reads as `NO_WORKFLOW_OUTPUT`.
    () => ok(NO_WORKFLOW_OUTPUT),
    // A `NoWorkflowOutput` value is stored as `null`; another value is refused.
    (value) =>
      isObjectLike(value) && "kind" in value && value.kind === NO_WORKFLOW_OUTPUT.kind
        ? ok(null)
        : err(NOT_NO_WORKFLOW_OUTPUT),
  ),
);
