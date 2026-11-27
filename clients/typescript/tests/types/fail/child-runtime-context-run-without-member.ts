// expect: TS2345 at 13
// `implementation.run` of a task that takes the workflow context needs a runtime with `workflowContext`.

import type { TaskRuntime } from "../../../horsies/src/child/public.ts";
import { defineTask, h, type Horsies } from "../../../horsies/src/index.ts";

declare const app: Horsies;
declare const runtime: TaskRuntime;

const audit = defineTask({ name: "audit", input: {}, output: h.null(), workflowContext: true });
const implementation = app.implement(audit, (_input, { ok }) => ok(null));

export const outcome = implementation.run({}, runtime);
