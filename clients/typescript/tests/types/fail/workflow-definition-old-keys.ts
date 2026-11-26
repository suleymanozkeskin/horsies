// expect: TS2353 at 9, TS2353 at 17
// `defineWorkflow` declares its input as `parameters` and its result as `outputType`; the build returns the output
// node as `output`. The keys `params` and `output` of the definition are gone.

import { defineWorkflow, h } from "../../../horsies/src/index.ts";

export const withParams = defineWorkflow(
  {
    params: { region: h.string() },
    name: "with_params",
    definitionKey: "tests.with_params.v1",
    outputType: h.null(),
  },
  (w) => ({ output: w.task(undefined as never, {}, { nodeId: "a" }) }),
);

export const withOutput = defineWorkflow({ output: h.null(), name: "o", definitionKey: "tests.o.v1", parameters: {} }, (w) => ({
  output: w.task(undefined as never, {}, { nodeId: "a" }),
}));
