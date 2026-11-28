// expect: TS2345 at 22
// resendStart takes only ENQUEUE_FAILED.

import { defineTask, defineWorkflow, h, type Horsies } from "../../../horsies/src/index.ts";

declare const app: Horsies;

const count = defineTask({ name: "count", input: {}, output: h.int() });

const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", outputType: h.int(), parameters: { n: h.int() }, checkCases: [{ n: 1 }] }, (w) => {
  const counted = w.task(count, {}, { nodeId: "count" });

  w.task(count, {}, { nodeId: "hidden", waitsFor: [counted] });

  return { output: counted };
});

export async function retry(): Promise<void> {
  const started = await app.start(flow, { n: 1 });

  if (!started.ok && started.error.code === "WORKFLOW_COMMAND_CONFLICT") {
    void (await app.resendStart(started.error));
  }
}
