// expect: TS2741 at 9
// A schedule input that leaves out a required parameter of its task.

import { defineTask, taskSchedule } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

const buildReport = defineTask({ name: "build_report", input: { format: h.string() }, output: h.null() });

export const entry = taskSchedule({ name: "a", task: buildReport, input: {}, pattern: { kind: "hourly", minute: 0 } });
