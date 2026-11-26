// The read form of a workflow's result, from its stored run: the core stores which node is the output. A task output
// node gives the output kind of its task's contract, found by the stored task name in the app's contract list; a
// sub-workflow output node gives the same rule applied to its child's stored run. No build runs, so the params, the
// check cases and the order of builds do not change the form.

import { type ReadForm, readFormOfCompiled } from "../read/handle.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { WorkflowCore } from "./core.ts";
import { handleErrorOfRaw, readErrorOfHandle, type WorkflowReadError } from "./handle-error.ts";
import { MAX_SUBWORKFLOW_DEPTH, type WorkflowRegistry } from "./registry.ts";

/** The read form of a stored result, or why the stored run gives none. */
export type StoredForm = Result<ReadForm, WorkflowReadError>;

/** One step of the walk down the output nodes: the form, or the child run to read next. */
type OutputStep =
  | { readonly kind: "form"; readonly form: StoredForm }
  | { readonly kind: "child"; readonly workflowId: string };

/**
 * The read form of the result of the workflow `workflowId`: the walk down its stored output nodes to a task node, at
 * most `MAX_SUBWORKFLOW_DEPTH` child levels. A task that the app does not list is `taskNotListed` with its name.
 * Effects: one read per level.
 */
export async function storedOutputForm(
  core: WorkflowCore,
  registry: WorkflowRegistry,
  workflowId: string,
): Promise<StoredForm> {
  let current = workflowId;

  for (let level = 0; level <= MAX_SUBWORKFLOW_DEPTH; level += 1) {
    const step = outputStep(registry, await core.outputNode(current));

    if (step.kind === "form") {
      return step.form;
    }

    current = step.workflowId;
  }

  return unreadable(`the output nodes nest deeper than ${MAX_SUBWORKFLOW_DEPTH} child workflows`);
}

/** The step of one stored output node. Pure. */
function outputStep(registry: WorkflowRegistry, fact: Awaited<ReturnType<WorkflowCore["outputNode"]>>): OutputStep {
  switch (fact.kind) {
    case "task": {
      const listed = registry.taskByName(fact.taskName);

      return {
        kind: "form",
        form:
          listed.kind === "listed"
            ? ok(readFormOfCompiled(listed.task.compiled))
            : err(Object.freeze({ kind: "taskNotListed", taskName: fact.taskName })),
      };
    }

    case "subWorkflow":
      return fact.childWorkflowId.kind === "recorded"
        ? { kind: "child", workflowId: fact.childWorkflowId.text }
        : { kind: "form", form: unreadable("the output node is a sub-workflow node whose child did not start") };
    case "noOutput":
      return { kind: "form", form: err(Object.freeze({ kind: "noOutputNode" })) };
    case "nodeMissing":
      return { kind: "form", form: unreadable(`the stored output index ${fact.taskIndex} names no node`) };
    case "workflowAbsent":
      return { kind: "form", form: err(Object.freeze({ kind: "workflowNotFound" })) };
    case "failed":
      return { kind: "form", form: err(readErrorOfHandle(handleErrorOfRaw(fact.error))) };
    default:
      return unreachable(fact, "outputStep");
  }
}

function unreadable(cause: string): StoredForm {
  return err(Object.freeze({ kind: "storedResultUnreadable", cause }));
}
