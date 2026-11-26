// expect: TS2353 at 14
// A child workflow with params: {} takes {} only.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });

const child = defineWorkflow({ name: "child", definitionKey: "tests.child.v1", params: {} }, (w) => ({
  output: w.task(ping, {}, { id: "ping" }),
}));

export const parent = defineWorkflow({ name: "parent", definitionKey: "tests.parent.v1", params: {} }, (w) => ({
  output: w.subWorkflow(child, { region: "eu" }, { id: "child" }),
}));
