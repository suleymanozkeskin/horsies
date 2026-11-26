// expect: TS2345 at 12
// A task with a required `h.taskOutcome` parameter cannot be sent: only a workflow node runs it.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const count = defineTask({ name: "count", input: {}, output: h.int() });
const charge = defineTask({ name: "charge", input: { order: h.taskOutcome(count) }, output: h.null() });

export const sent = app.send(charge, {});
