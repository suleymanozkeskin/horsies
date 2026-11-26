// Compile-pass cases of the workflow context in the runtime: only a task with `workflowContext: true` has
// `runtime.workflowContext`, a closed union of `context` and `noContext`; `TaskRuntime.forTest` gives a runtime that
// fits the body of a task with or without the context.

import {
  type AttemptNumber,
  TaskRuntime,
  type ThreadTaskRuntime,
  type ThreadWorkflowTaskRuntime,
  type WorkflowTaskRuntime,
} from "../../../horsies/src/child/public.ts";
import type { TaskId } from "../../../horsies/src/contract/ids.ts";
import type { RuntimeOf } from "../../../horsies/src/contract/public.ts";
import { defineTask, h, type Horsies, type RuntimeWorkflowContext } from "../../../horsies/src/index.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

declare const app: Horsies;
declare const taskId: TaskId;
declare const attempt: AttemptNumber;

const produce = defineTask({ name: "produce", input: {}, output: h.int() });

const audit = defineTask({ name: "audit", input: {}, output: h.int(), workflowContext: true });

const plain = defineTask({ name: "plain", input: {}, output: h.int() });

export const isolated = expectType<Equal<RuntimeOf<"ISOLATED", "takesContext">, WorkflowTaskRuntime>>();

export const sharedAsync = expectType<Equal<RuntimeOf<"SHARED_ASYNC", "takesContext">, WorkflowTaskRuntime>>();

export const sharedThread = expectType<Equal<RuntimeOf<"SHARED_THREAD", "takesContext">, ThreadWorkflowTaskRuntime>>();

export const noContext = expectType<Equal<RuntimeOf<"ISOLATED", "noContext">, TaskRuntime>>();

export const threadNoContext = expectType<Equal<RuntimeOf<"SHARED_THREAD", "noContext">, ThreadTaskRuntime>>();

export const member = expectType<Equal<WorkflowTaskRuntime["workflowContext"], RuntimeWorkflowContext>>();

/** The body reads the context with a switch over its kinds. */
export const audited = app.implement(audit, (_input, runtime) => {
  const context = runtime.workflowContext;

  switch (context.kind) {
    case "context": {
      const produced = context.context.resultFor("produce", produce);

      return audit.ok(produced.ok && produced.value.kind === "succeeded" ? produced.value.value : 0);
    }

    case "noContext":
      return audit.ok(0);
  }
});

export const plainImplementation = app.implement(plain, (_input, runtime: TaskRuntime) => plain.ok(runtime.fencingToken.value));

const forTest: WorkflowTaskRuntime = TaskRuntime.forTest({
  taskId,
  attempt,
  workflowContext: {
    workflow_id: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5d",
    task_index: 1,
    task_name: "audit",
    results_by_id: { produce: { __h_task_result__: true, ok: 10, err: null } },
  },
});

export const runs = [audited.run({}, forTest), plainImplementation.run({}, forTest), audited.run({}, TaskRuntime.forTest({ taskId, attempt }))];
