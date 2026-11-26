// expect: TS2339 at 12
// A node reference holds its node id as `nodeId`; it has no `id` member.

import { defineTask, defineWorkflow, h } from "../../../horsies/src/index.ts";

const count = defineTask({ name: "count", input: {}, output: h.int() });

export const flow = defineWorkflow(
  { name: "flow", definitionKey: "tests.flow.v1", output: h.int(), params: {} },
  (w) => {
    const counted = w.task(count, {}, { nodeId: "count" });
    const id: string = counted.id;

    return { output: id === "count" ? counted : counted };
  },
);
