// expect: TS2322 at 14
// A schedule entry is made by `taskSchedule`, which checks its input against its task: a plain object is no entry.

import { defineTask, type HorsiesConfig } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

const buildReport = defineTask({ name: "build_report", input: { format: h.string() }, output: h.null() });

export const config: HorsiesConfig = {
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [buildReport],
  schedule: {
    schedules: [
      { name: "a", task: buildReport, input: { format: 1 }, pattern: { kind: "hourly", minute: 0 } },
    ],
  },
};
