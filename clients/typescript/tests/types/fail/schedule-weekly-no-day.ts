// expect: TS2322 at 7
// A weekly pattern with no day: `days` holds at least one day.

import type { SchedulePattern } from "../../../horsies/src/index.ts";

export const pattern: SchedulePattern = {
  kind: "weekly", weekdays: [], time: Temporal.PlainTime.from("08:30"),
};
