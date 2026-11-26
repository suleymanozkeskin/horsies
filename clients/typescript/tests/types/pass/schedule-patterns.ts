// Compile-pass cases: a pattern of each kind, with the defaulted fields left out.

import type { SchedulePattern } from "../../../horsies/src/index.ts";

export const patterns: readonly SchedulePattern[] = [
  { kind: "interval", every: Temporal.Duration.from({ minutes: 5 }) },
  { kind: "hourly", minute: 30 },
  { kind: "daily", time: Temporal.PlainTime.from("09:00") },
  { kind: "weekly", weekdays: ["monday", "thursday"], time: Temporal.PlainTime.from("08:30") },
  { kind: "monthly", dayOfMonth: 1, time: Temporal.PlainTime.from("06:00") },
  {
    kind: "cron",
    minute: [{ kind: "values", values: [0, 30] }],
    hour: [{ kind: "range", start: 8, end: 18 }],
    month: [{ kind: "every" }],
    day: { kind: "byWeekday", dayOfWeek: [{ kind: "enumValues", values: ["monday", "friday"] }] },
  },
  {
    kind: "cron",
    minute: [{ kind: "every" }],
    hour: [{ kind: "step", step: 6 }],
    month: [{ kind: "enumRange", start: "march", end: "june", step: 2 }],
    day: { kind: "eitherDay", dayOfMonth: [{ kind: "values", values: [13] }], dayOfWeek: [{ kind: "enumStep", step: 2 }] },
  },
];
