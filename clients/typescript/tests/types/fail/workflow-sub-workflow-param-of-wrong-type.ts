// expect: TS2322 at 18
// A child param of another type than the child's params declare.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: { region: h.string() }, output: h.null() });

const child = defineWorkflow(
  { name: "child", definitionKey: "tests.child.v1", params: { region: h.string() }, checkCases: [{ region: "eu" }] },
  (w, { region }) => ({ output: w.task(ping, { region }, { id: "ping" }) }),
);

export const parent = defineWorkflow(
  { name: "parent", definitionKey: "tests.parent.v1", params: {} },
  (w) => ({
    output: w.subWorkflow(child,
      { region: 7 },
      { id: "child" }),
  }),
);
