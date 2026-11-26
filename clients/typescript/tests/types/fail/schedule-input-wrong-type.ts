// expect: TS2322 at 12
// A schedule input with a value of the wrong type for its own task.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const buildReport = defineTask({ name: "build_report", input: { format: h.string() }, output: h.null() });
const refreshStock = defineTask({ name: "refresh_stock", input: {}, output: h.null() });

app.schedules([{ name: "a", task: refreshStock, input: {}, pattern: { kind: "hourly", minute: 0 } }, { name: "b", task: buildReport, input: { format: 1 }, pattern: { kind: "hourly", minute: 0 } }]);
