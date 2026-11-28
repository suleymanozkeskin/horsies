// Compile-pass cases of a workflow with no outputType: the build returns no output node; get reads
// NoWorkflowOutput; a sub-workflow node of it gives NoWorkflowOutput, which an h.taskOutcome parameter of the
// workflow takes; resultFor of that node by a handle of the id gives NoWorkflowOutput.

import {
  defineTask,
  defineWorkflow,
  h,
  type Horsies,
  type NodeRef,
  type NoWorkflowOutput,
  type ParameterSchemas,
  type SubWorkflowOutput,
  type WorkflowContract,
  WorkflowId,
  type WorkflowOutput,
} from "../../../horsies/src/index.ts";

declare const app: Horsies;

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const ping = defineTask({ name: "ping", input: {}, output: h.int() });

export const fanout = defineWorkflow({ name: "fanout", definitionKey: "shop.fanout.v1", parameters: {} }, (w) => {
  const root = w.task(ping, {}, { nodeId: "root" });
  const left = w.task(ping, {}, { nodeId: "left", waitsFor: [root] });

  w.task(ping, {}, { nodeId: "right", waitsFor: [root] });

  return { successPolicy: { cases: [{ required: [left] }] } };
});

/** A build with no output node may return an empty object. */
export const bare = defineWorkflow({ name: "bare", definitionKey: "shop.bare.v1", parameters: {} }, (w) => {
  w.task(ping, {}, { nodeId: "only" });

  return {};
});

type BuildOf<W> = W extends WorkflowContract<ParameterSchemas, infer R> ? R : never;

expectType<Equal<WorkflowOutput<BuildOf<typeof fanout>>, NoWorkflowOutput>>();

expectType<Equal<SubWorkflowOutput<BuildOf<typeof fanout>>, NoWorkflowOutput>>();

expectType<Equal<WorkflowOutput<BuildOf<typeof bare>>, NoWorkflowOutput>>();

const afterFanout = defineTask({ name: "after_fanout", input: { done: h.taskOutcome(fanout) }, output: h.int() });

export const parent = defineWorkflow(
  { name: "parent", definitionKey: "shop.parent.v1", outputType: h.int(), parameters: {} },
  (w) => {
    const child = w.subWorkflow(fanout, {}, { nodeId: "child" });

    expectType<Equal<typeof child, NodeRef<NoWorkflowOutput, string, "child">>>();

    return { output: w.task(afterFanout, { done: child }, { nodeId: "after" }) };
  },
);

export async function readResults(workflowId: WorkflowId): Promise<void> {
  const started = await app.start(fanout, {});

  if (started.ok) {
    const read = await started.value.get({ waitMs: 1_000 });

    expectType<Equal<typeof read, Awaited<ReturnType<typeof started.value.get>>>>();

    if (read.ok && read.value.kind === "succeeded") {
      expectType<Equal<typeof read.value.value, NoWorkflowOutput>>();
    }
  }

  const byId = app.workflowHandle(parent, workflowId);
  const child = await byId.resultFor("child", fanout);

  if (child.ok && child.value.kind === "succeeded") {
    expectType<Equal<typeof child.value.value, NoWorkflowOutput>>();
  }
}
