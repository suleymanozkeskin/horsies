// expect: TS2820 at 10
// A day of week that is not a `Weekday` spelling.

import type { SchedulePattern } from "../../../horsies/src/index.ts";

export const pattern: SchedulePattern = {
  kind: "cron",
  minute: [{ kind: "values", values: [0] }],
  hour: [{ kind: "every" }],
  month: [{ kind: "every" }], day: { kind: "byWeekday", dayOfWeek: [{ kind: "enumValues", values: ["Monday"] }] },
};
