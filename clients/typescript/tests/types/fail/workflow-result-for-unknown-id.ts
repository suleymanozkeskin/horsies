// expect: TS2345 at 22
// resultFor takes only the id of a node of the returned object.

import { defineTask, defineWorkflow, h, type Horsies } from "../../../horsies/src/index.ts";

declare const app: Horsies;

const count = defineTask({ name: "count", input: {}, output: h.int() });

const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.int(), params: { n: h.int() }, checkCases: [{ n: 1 }] }, (w) => {
  const counted = w.task(count, {}, { id: "count" });

  w.task(count, {}, { id: "hidden", waitsFor: [counted] });

  return { output: counted };
});

export async function read(): Promise<void> {
  const started = await app.start(flow, { n: 1 });

  if (started.ok) {
    void (await started.value.resultFor("hidden"));
  }
}
