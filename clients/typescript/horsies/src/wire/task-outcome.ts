// `h.taskOutcome(contract)`: a task input parameter that a workflow node fills with the outcome of an upstream task or
// sub-workflow. The contract gives the type: its output value and its codes. The kind of the parameter holds the
// output kind of the contract (`UpstreamOutput`); a contract whose output has no stored form gives a parameter that
// `checkParameters` refuses.

import type { AnyTaskContract, contractTypes } from "../contract/define-task.ts";
import type { SchemaValue } from "../contract/schema.ts";
import type { AnyWorkflowContract, workflowTypes } from "../workflow/define.ts";
import type { SubWorkflowOutput } from "../workflow/node.ts";
import type { Kind, TaskOutcomeKind, UpstreamCodes, UpstreamOutput } from "./kinds.ts";
import { unreachable } from "./unreachable.ts";
import type { TaskOutcome } from "./task-error.ts";
import type { Wire } from "./wire-type.ts";

/**
 * The wire type of the outcome of the upstream `C`: for a task contract, its output value and its codes; for a
 * workflow contract, the success value of a sub-workflow node of it (its output value, or `NoWorkflowOutput` for a
 * workflow with no `outputType`) and any code (a workflow holds the codes of many tasks). A sender never gives it (its sent type is `never`).
 *
 * The input decode refuses a stored custom code that a task contract with `errorCodes` does not declare. A task
 * contract with no `errorCodes` and a workflow contract allow every custom code (their code type is `string`).
 */
export type TaskOutcomeWire<C extends AnyTaskContract | AnyWorkflowContract> = Wire<
  C extends AnyTaskContract
    ? TaskOutcome<SchemaValue<C[typeof contractTypes]["output"]>, C[typeof contractTypes]["code"]>
    : C extends AnyWorkflowContract
      ? TaskOutcome<SubWorkflowOutput<C[typeof workflowTypes]["build"]>, string>
      : never,
  never,
  "required",
  "taskOutcome"
>;

/** The kind of an upstream outcome whose success value has the output `output`. Pure. */
export function taskOutcomeKind(output: UpstreamOutput, codes: UpstreamCodes): TaskOutcomeKind {
  return Object.freeze({ node: "taskOutcome", output, codes });
}

/**
 * The kind of the success value of an upstream outcome kind. A contract with an upstream output that has no stored
 * form never reaches the codec (`checkParameters` refuses it), so a refused output here is a defect: `TypeError`.
 */
export function outcomeValueKind(kind: TaskOutcomeKind, path: string): Kind {
  const output = kind.output;

  switch (output.state) {
    case "compiled":
      return output.kind;
    case "refused":
      throw new TypeError(
        `horsies: the upstream output of ${path} has no stored form; checkParameters refuses this wire type`,
      );
    default:
      return unreachable(output, "outcomeValueKind");
  }
}

/**
 * The kind of the success value of a parameter kind that holds an upstream outcome at its top (`h.taskOutcome(c)`, or
 * its `.nullable()` form) at `place`. Another kind is a defect: `TypeError`. Pure.
 */
export function upstreamValueKind(kind: Kind, place: string): Kind {
  switch (kind.node) {
    case "taskOutcome":
      return outcomeValueKind(kind, place);
    case "nullable":
      return upstreamValueKind(kind.inner, place);
    case "null":
    case "boolean":
    case "int":
    case "bigint":
    case "float":
    case "string":
    case "uuid":
    case "decimal":
    case "instant":
    case "offsetDateTime":
    case "plainDateTime":
    case "plainDate":
    case "plainTime":
    case "literal":
    case "union":
    case "taggedUnion":
    case "array":
    case "tuple":
    case "record":
    case "object":
    case "json":
    case "conversion":
    case "checked":
    case "lazy":
      throw new TypeError(`horsies: the kind ${kind.node} of ${place} is not an upstream outcome`);
    default:
      return unreachable(kind, "upstreamValueKind");
  }
}

/** Whether a kind holds an upstream outcome at its top: `h.taskOutcome(c)`, or `h.taskOutcome(c).nullable()`. Pure. */
export function isTaskOutcomeKind(kind: Kind): boolean {
  switch (kind.node) {
    case "taskOutcome":
      return true;
    case "nullable":
      return isTaskOutcomeKind(kind.inner);
    case "null":
    case "boolean":
    case "int":
    case "bigint":
    case "float":
    case "string":
    case "uuid":
    case "decimal":
    case "instant":
    case "offsetDateTime":
    case "plainDateTime":
    case "plainDate":
    case "plainTime":
    case "literal":
    case "union":
    case "taggedUnion":
    case "array":
    case "tuple":
    case "record":
    case "object":
    case "json":
    case "conversion":
    case "checked":
    case "lazy":
      return false;
    default:
      return unreachable(kind, "isTaskOutcomeKind");
  }
}
