// The two read paths that give a stored envelope to the user besides the codec's envelope reader: the result read of a
// task handle (`TaskHandle.get`) and `resultFor` of a workflow handle. Each runs over a fake core that answers the
// text of a vector; the strict check of the core is not part of these paths (the native tests run it).

import assert from "node:assert/strict";

import { checkContractList, contractLookup } from "../../horsies/src/app/contract-list.ts";
import { defineTask } from "../../horsies/src/contract/define-task.ts";
import { WorkflowId } from "../../horsies/src/contract/ids.ts";
import { readFormOf, taskHandleOf } from "../../horsies/src/read/handle.ts";
import type { RequiredWire } from "../../horsies/src/wire/infer.ts";
import type { TaskOutcome } from "../../horsies/src/wire/task-error.ts";
import { workflowHandleByIdOf } from "../../horsies/src/workflow/handle.ts";
import { workflowRegistry } from "../../horsies/src/workflow/registry.ts";
import { fakeCore, WORKFLOW_ID } from "../unit/workflow/core-support.ts";
import { addonAccess, fakeProducer, storedRead, TASK_ID } from "../unit/read/support.ts";
import { show } from "../support/values.ts";

/** The status of the stored row that the fake core answers with an envelope text. */
export type StoredStatus = "COMPLETED" | "FAILED";

/** The outcome that `TaskHandle.get` gives for the envelope `text` of a task of output `wire`. */
export async function taskHandleOutcome(wire: RequiredWire, text: string, status: StoredStatus): Promise<TaskOutcome<unknown>> {
  const contract = defineTask({ name: "stored_task", input: {}, output: wire });
  const fake = fakeProducer({ results: [storedRead(status, text)] });
  const handle = taskHandleOf<unknown, string>(addonAccess(fake), readFormOf(contract), TASK_ID);
  const read = await handle.get();

  assert(read.ok, show(read));

  return read.value;
}

/** The outcome that `resultFor` of a workflow handle by id gives for the envelope `text` of a node of output `wire`. */
export async function workflowResultForOutcome(wire: RequiredWire, text: string): Promise<TaskOutcome<unknown>> {
  const contract = defineTask({ name: "stored_task", input: {}, output: wire });
  const core = fakeCore({ nodeResult: { kind: "stored", result: { kind: "checked", text } } });
  const workflowId = WorkflowId.parse(WORKFLOW_ID);

  assert(workflowId.ok);

  const registry = workflowRegistry([], contractLookup(checkContractList([contract], { kind: "default" }).entries), {
    kind: "default",
  });

  const handle = workflowHandleByIdOf<never>({
    core,
    registry,
    workflow: {},
    workflowId: workflowId.value,
    targets: new Map(),
  });

  // SAFETY: the handle by id types `resultFor` by the contract's own types; this test reads them as `unknown`.
  const read = await handle.resultFor("node", contract as never);

  assert(read.ok, show(read));

  // SAFETY: `read.ok` holds a `TaskOutcome` of the contract's types; `unknown` is their widest form.
  return read.value as TaskOutcome<unknown>;
}
