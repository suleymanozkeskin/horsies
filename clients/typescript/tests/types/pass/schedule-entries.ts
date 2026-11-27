// Compile-pass cases: the `schedule` section of the app config, each entry made by `taskSchedule` with its input typed
// by its own task, a list declared apart, and an empty list.

import { defineTask, type HorsiesConfig, type ScheduleSection, type TaskSchedule, taskSchedule } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

const buildReport = defineTask({ name: "build_report", input: { format: h.string() }, output: h.null() });
const refreshStock = defineTask({ name: "refresh_stock", input: {}, output: h.null() });

const SCHEDULES: readonly TaskSchedule[] = [
  taskSchedule({
    name: "daily-report",
    task: buildReport,
    input: { format: "pdf" },
    pattern: { kind: "daily", time: Temporal.PlainTime.from("09:00") },
    timezone: "Europe/Berlin",
    catchUpMissed: false,
  }),
  taskSchedule({
    name: "stock-refresh",
    task: refreshStock,
    input: {},
    pattern: { kind: "interval", every: Temporal.Duration.from({ minutes: 5 }) },
    queue: "standard",
    enabled: false,
    maxCatchUpRuns: 2,
  }),
];

export const config: HorsiesConfig = {
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [buildReport, refreshStock],
  schedule: { enabled: true, schedules: SCHEDULES, checkIntervalSeconds: 1 },
};

export const empty: ScheduleSection = { schedules: [] };

export const settings: Pick<HorsiesConfig, "schemaMode" | "logFormat"> = {
  schemaMode: "ASSUME_INITIALIZED",
  logFormat: { kind: "jsonLogs" },
};
