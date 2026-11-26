// The output kind of the upstream contract of an `h.taskOutcome(contract)` parameter: the declared output of a task
// contract, or of a workflow contract. `h.taskOutcome` reads it when it builds the parameter's kind, so the parameter
// decodes the stored result envelope with the output kind of its upstream.

import { unreachable } from "../wire/unreachable.ts";
import type { UpstreamOutput } from "../wire/kinds.ts";
import { type AnyWorkflowContract, workflowRecord } from "../workflow/define.ts";
import { compileOutput } from "./compile.ts";
import type { AnyTaskContract } from "./define-task.ts";
import { contractRecord } from "./definition.ts";

/**
 * The output kind of `contract`, or why its output has no stored form (the text names the contract and each
 * refusal). Effects: none; a zod output compiles here, with no kept outcome. A value that `defineTask` or
 * `defineWorkflow` did not build is a defect: `TypeError`.
 */
export function upstreamOutputOf(contract: AnyTaskContract | AnyWorkflowContract): UpstreamOutput {
  switch (contract.kind) {
    case "task":
      return outputOf(`the task ${contract.name}`, contract);
    case "workflow":
      return outputOf(`the workflow ${contract.name}`, workflowRecord(contract).paramsContract);
    default:
      return unreachable(contract, "upstreamOutputOf");
  }
}

/** The output of the task contract `holder` (a workflow's params contract holds the workflow's output). */
function outputOf(subject: string, holder: object): UpstreamOutput {
  const compiled = compileOutput(contractRecord(holder).output);

  if (compiled.ok) {
    return Object.freeze({ state: "compiled", kind: compiled.value });
  }

  const causes = compiled.error.map((refused) => `${refused.path}: ${refused.detail}`).join("; ");

  return Object.freeze({ state: "refused", detail: `the output of ${subject} has no stored form: ${causes}` });
}
