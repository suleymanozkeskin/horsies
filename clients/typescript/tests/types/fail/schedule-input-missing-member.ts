// expect: TS2741 at 11
// A schedule input that leaves out a required parameter of its task.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const buildReport = defineTask({ name: "build_report", input: { format: h.string() }, output: h.null() });

app.schedules([{ name: "a", task: buildReport, input: {}, pattern: { kind: "hourly", minute: 0 } }]);
