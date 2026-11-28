// expect: TS2305 at 7, TS2353 at 9, TS2353 at 11, TS2353 at 13
// The patterns name their fields as the Python and Rust clients do: `time` (not `at`), `weekdays` (not `days`) and
// `dayOfMonth` (not `day`). One schedule is a `TaskSchedule`; `ScheduleEntry` is no public name.

import type { SchedulePattern } from "../../../horsies/src/index.ts";

import type { ScheduleEntry } from "../../../horsies/src/index.ts";

export const daily: SchedulePattern = { kind: "daily", at: Temporal.PlainTime.from("09:00") };

export const weekly: SchedulePattern = { kind: "weekly", days: ["monday"], time: Temporal.PlainTime.from("09:00") };

export const monthly: SchedulePattern = { kind: "monthly", day: 1, time: Temporal.PlainTime.from("09:00") };

export type Old = ScheduleEntry<never>;
