// expect: TS2322 at 13
// ctxFrom on a task that does not take the workflow context.

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });
const audit = defineTask({ name: "audit", input: {}, output: h.null() });

export const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.null(), params: {} }, (w) => {
  const pinged = w.task(ping, {}, { id: "ping" });

  return { output: w.task(audit, {}, { id: "audit", waitsFor: [pinged], ctxFrom: [pinged] }) };
});
