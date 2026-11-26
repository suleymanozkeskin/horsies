// expect: TS2345 at 32
// A start-error switch that leaves out a code fails at assertNever.

import { defineTask, defineWorkflow, h, type Horsies } from "../../../horsies/src/index.ts";

declare const app: Horsies;

const count = defineTask({ name: "count", input: {}, output: h.int() });

const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.int(), params: { n: h.int() }, checkCases: [{ n: 1 }] }, (w) => {
  const counted = w.task(count, {}, { nodeId: "count" });

  w.task(count, {}, { nodeId: "hidden", waitsFor: [counted] });

  return { output: counted };
});

function assertNever(value: never): never {
  throw new Error(String(value));
}

export async function start(): Promise<void> {
  const started = await app.start(flow, { n: 1 });

  if (!started.ok) {
    switch (started.error.code) {
      case "WORKFLOW_COMMAND_CONFLICT":
      case "WORKFLOW_ID_CONFLICT":
      case "VALIDATION_FAILED":
        break;
      default:
        assertNever(started.error);
    }
  }
}
