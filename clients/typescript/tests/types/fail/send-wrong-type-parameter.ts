// expect: TS2322 at 11
// A send with a parameter of the wrong type.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const report = defineTask({ name: "report", input: { n: h.int() }, output: h.null() });

export const sent = app.send(report, { n: "one" });
