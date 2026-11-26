// Compile-pass cases: the entries of `app.schedules`, each input typed by its own task, and the section.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const buildReport = defineTask({ name: "build_report", input: { format: h.string() }, output: h.null() });
const refreshStock = defineTask({ name: "refresh_stock", input: {}, output: h.null() });

app.schedules(
  [
    {
      name: "daily-report",
      task: buildReport,
      input: { format: "pdf" },
      pattern: { kind: "daily", time: Temporal.PlainTime.from("09:00") },
      timezone: "Europe/Berlin",
      catchUpMissed: false,
    },
    {
      name: "stock-refresh",
      task: refreshStock,
      input: {},
      pattern: { kind: "interval", every: Temporal.Duration.from({ minutes: 5 }) },
      queue: "standard",
      enabled: false,
      maxCatchUpRuns: 2,
    },
  ],
  { enabled: true, checkIntervalSeconds: 1 },
);

app.schedules([]);
