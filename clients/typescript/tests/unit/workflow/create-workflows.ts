// The workflows and the task list of the create-check tests, and the codes of the create checks of a workflow list.
// The worker-thread test imports this module in a `worker_threads` isolate.

import { checkContractList, contractLookup } from "../../../horsies/src/app/contract-list.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { checkWorkflowsAtCreate, type WorkflowsChecked } from "../../../horsies/src/workflow/create-checks.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

export const ping = defineTask({ name: "ping", input: {}, output: h.int() });

/** A workflow with no params and one node. */
export const plain = defineWorkflow(
  { name: "plain", definitionKey: "tests.plain.v1", output: h.int(), params: {} },
  (w) => ({
    output: w.task(ping, {}, { id: "ping" }),
  }),
);

/** A workflow with no params whose build has a duplicate node id. */
export const duplicateIds = defineWorkflow(
  { name: "duplicate", definitionKey: "tests.duplicate.v1", output: h.int(), params: {} },
  (w) => {
    w.task(ping, {}, { id: "same" });

    return { output: w.task(ping, {}, { id: "same" }) };
  },
);

/** The create checks of `workflows`, with the task `ping` in the default queue mode. */
export function checkedAtCreate(workflows: readonly object[]): WorkflowsChecked {
  return checkWorkflowsAtCreate({
    workflows,
    tasks: contractLookup(checkContractList([ping], { kind: "default" }).entries),
    queues: { kind: "default" },
  });
}
