// expect: TS2322 at 18
// A start param of another type than the params declare.

import { defineTask, defineWorkflow, h, type Horsies } from "../../../horsies/src/index.ts";

declare const app: Horsies;

const count = defineTask({ name: "count", input: {}, output: h.int() });

const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", params: { n: h.int() }, checkCases: [{ n: 1 }] }, (w) => {
  const counted = w.task(count, {}, { id: "count" });

  w.task(count, {}, { id: "hidden", waitsFor: [counted] });

  return { output: counted };
});

export const started = app.start(flow, { n: "one" });
