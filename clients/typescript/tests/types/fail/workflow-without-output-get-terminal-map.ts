// expect: TS2339 at 21
// The result of `get` of a workflow with no output node is NoWorkflowOutput: it holds no map of terminal nodes.

import { defineTask, defineWorkflow, h, type Horsies } from "../../../horsies/src/index.ts";

declare const app: Horsies;

const ping = defineTask({ name: "ping", input: {}, output: h.int() });

const fanout = defineWorkflow({ name: "fanout", definitionKey: "tests.fanout.v1", parameters: {} }, (w) => {
  w.task(ping, {}, { nodeId: "only" });

  return {};
});

export async function terminalMap(): Promise<void> {
  const started = await app.start(fanout, {});
  const read = started.ok ? await started.value.get() : undefined;

  if (read?.ok && read.value.kind === "succeeded") {
    void read.value.value.byNodeId;
  }
}
