// expect: TS2345 at 11
// A task with a required `h.resultOf` parameter cannot be sent: only a workflow node runs it.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const charge = defineTask({ name: "charge", input: { order: h.resultOf(h.int()) }, output: h.null() });

export const sent = app.send(charge, {});
