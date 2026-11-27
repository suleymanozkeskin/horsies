// expect: TS2322 at 16
// A schedule input with a value of the wrong type for its own task, in a list with an entry of another task.

import { defineTask, type HorsiesConfig, taskSchedule } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

const buildReport = defineTask({ name: "build_report", input: { format: h.string() }, output: h.null() });
const refreshStock = defineTask({ name: "refresh_stock", input: {}, output: h.null() });

export const config: HorsiesConfig = {
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [buildReport, refreshStock],
  schedule: {
    schedules: [
      taskSchedule({ name: "a", task: refreshStock, input: {}, pattern: { kind: "hourly", minute: 0 } }),
      taskSchedule({ name: "b", task: buildReport, input: { format: 1 }, pattern: { kind: "hourly", minute: 0 } }),
    ],
  },
};
