// The schedule table of the app, the one of the Rust showcase: 32 schedules, 29 enabled. Each slot of a pattern sends
// its task with its input; the `scheduler` command runs the table, and `check` checks it (the input against the task's
// input types, each pattern, each zone). The patterns are typed: interval, hourly, daily, weekly, monthly and cron.

import type { AnyTaskContract, CronNumericTerm, Horsies, SchedulePattern, TaskSchedule } from "horsies";

import { abandonedCartSweep, flakyExport, regionalRollup, retentionAudit, salesRollup } from "./contracts/analytics.ts";
import { replenishCatalog, syncSupplierFeed } from "./contracts/inventory.ts";
import { marketingBlast } from "./contracts/notify.ts";
import { reconcilePayments } from "./contracts/payments.ts";
import { prewarmSearch, warmCacheEdge } from "./contracts/promotions.ts";
import {
  ABANDONED_CART_AGE_MINUTES,
  ABANDONED_CART_MINUTE,
  CACHE_WARM_INTERVAL_MINUTES,
  CATALOG_STOCK_PER_SKU,
  PRICE_SYNC_MINUTE_STEP,
  RECONCILE_HOUR_STEP,
  RECONCILE_MINUTE,
  REGIONS,
  RETENTION_AUDIT_DAYS,
  ROLLUP_FIRST_MINUTE,
  ROLLUP_MINUTE_STEP,
  SALES_ROLLUP_HOUR,
  SEARCH_PREWARM_INTERVAL_MINUTES,
  SUPPLIER_FEED_INTERVAL_SECONDS,
  SUPPLIER_FEED_STEP_SECONDS,
  SUPPLIERS,
} from "./tuning.ts";

/** How an app gets its schedules: one call of `app.schedules`. The showcase passes `declareAcmeSchedules`. */
export type DeclareSchedules = (app: Horsies) => void;

/** A schedule of the table: an entry whose input typechecks against its task. Pure. */
function schedule<C extends AnyTaskContract>(entry: TaskSchedule<C>): TaskSchedule<C> {
  return entry;
}

/** The time of day `hour:minute`. Pure. */
function at(hour: number, minute: number): Temporal.PlainTime {
  return Temporal.PlainTime.from({ hour, minute });
}

function everySeconds(seconds: number): SchedulePattern {
  return { kind: "interval", every: Temporal.Duration.from({ seconds }) };
}

function everyMinutes(minutes: number): SchedulePattern {
  return { kind: "interval", every: Temporal.Duration.from({ minutes }) };
}

function values(...numbers: [number, ...number[]]): CronNumericTerm {
  return { kind: "values", values: numbers };
}

/** The schedules of each supplier: its feed, at its own interval. */
function supplierSchedules(): readonly TaskSchedule<typeof syncSupplierFeed>[] {
  return SUPPLIERS.map((supplier, offset) =>
    schedule({
      name: `supplier-feed-${supplier}`,
      task: syncSupplierFeed,
      input: { supplier },
      pattern: everySeconds(SUPPLIER_FEED_INTERVAL_SECONDS + offset * SUPPLIER_FEED_STEP_SECONDS),
    }),
  );
}

/** The schedules of each region: the hourly rollup and the cache warm-up. */
function regionSchedules(): readonly (TaskSchedule<typeof regionalRollup> | TaskSchedule<typeof warmCacheEdge>)[] {
  return REGIONS.flatMap((region, offset) => [
    schedule({
      name: `rollup-${region}`,
      task: regionalRollup,
      input: { region },
      pattern: { kind: "hourly", minute: ROLLUP_FIRST_MINUTE + offset * ROLLUP_MINUTE_STEP },
    }),
    schedule({
      name: `cache-warm-${region}`,
      task: warmCacheEdge,
      input: { campaignId: `steady-${region}` },
      pattern: everyMinutes(CACHE_WARM_INTERVAL_MINUTES + offset),
    }),
  ]);
}

/** The schedules of the daily, weekly and monthly work. */
function calendarSchedules(): readonly TaskSchedule<AnyTaskContract>[] {
  return [
    schedule({ name: "search-prewarm", task: prewarmSearch, input: { campaignId: "steady-state" }, pattern: everyMinutes(SEARCH_PREWARM_INTERVAL_MINUTES) }),
    schedule({
      name: "abandoned-cart-sweep",
      task: abandonedCartSweep,
      input: { olderThanMinutes: ABANDONED_CART_AGE_MINUTES },
      pattern: { kind: "hourly", minute: ABANDONED_CART_MINUTE },
    }),
    schedule({ name: "retention-audit-hourly", task: retentionAudit, input: { olderThanDays: RETENTION_AUDIT_DAYS }, pattern: { kind: "hourly", minute: 50 } }),
    schedule({ name: "sales-rollup-daily", task: salesRollup, input: { window: "daily" }, pattern: { kind: "daily", time: at(SALES_ROLLUP_HOUR, 0) } }),
    schedule({ name: "retention-audit-daily", task: retentionAudit, input: { olderThanDays: 90 }, pattern: { kind: "daily", time: at(3, 30) } }),
    schedule({ name: "nightly-stocktake", task: replenishCatalog, input: { targetUnits: CATALOG_STOCK_PER_SKU }, pattern: { kind: "daily", time: at(4, 15) } }),
    schedule({ name: "reconcile-daily", task: reconcilePayments, input: { window: "daily" }, pattern: { kind: "daily", time: at(4, 0) } }),
    schedule({ name: "winback-blast", task: marketingBlast, input: { segment: "winback" }, pattern: { kind: "daily", time: at(9, 0) } }),
    schedule({ name: "newsletter-blast", task: marketingBlast, input: { segment: "newsletter" }, pattern: { kind: "daily", time: at(10, 0) }, enabled: false }),
    schedule({ name: "weekly-sales-review", task: salesRollup, input: { window: "weekly" }, pattern: { kind: "weekly", weekdays: ["monday"], time: at(6, 0) } }),
    schedule({
      name: "weekly-supplier-audit",
      task: syncSupplierFeed,
      input: { supplier: SUPPLIERS[0] },
      pattern: { kind: "weekly", weekdays: ["wednesday"], time: at(7, 0) },
    }),
    schedule({
      name: "weekend-flash-prep",
      task: prewarmSearch,
      input: { campaignId: "weekend" },
      pattern: { kind: "weekly", weekdays: ["friday", "saturday"], time: at(12, 0) },
    }),
    schedule({
      name: "weekly-retention-audit",
      task: retentionAudit,
      input: { olderThanDays: 180 },
      pattern: { kind: "weekly", weekdays: ["sunday"], time: at(2, 0) },
      enabled: false,
    }),
    schedule({ name: "monthly-close", task: reconcilePayments, input: { window: "monthly" }, pattern: { kind: "monthly", dayOfMonth: 1, time: at(5, 0) } }),
    schedule({ name: "monthly-catalog-audit", task: retentionAudit, input: { olderThanDays: 365 }, pattern: { kind: "monthly", dayOfMonth: 15, time: at(8, 0) } }),
    schedule({ name: "monthly-markdown-review", task: salesRollup, input: { window: "monthly" }, pattern: { kind: "monthly", dayOfMonth: 28, time: at(11, 0) } }),
  ];
}

/** The schedules with a typed cron pattern. */
function cronSchedules(): readonly TaskSchedule<AnyTaskContract>[] {
  const every = { kind: "every" } as const;

  return [
    schedule({
      name: "payment-reconciliation",
      task: reconcilePayments,
      input: { window: "4h" },
      pattern: { kind: "cron", minute: [values(RECONCILE_MINUTE)], hour: [{ kind: "step", step: RECONCILE_HOUR_STEP }], month: [every], day: { kind: "everyDay" } },
    }),
    schedule({
      name: "price-sync-quarter-hour",
      task: warmCacheEdge,
      input: { campaignId: "price-sync" },
      pattern: { kind: "cron", minute: [{ kind: "step", step: PRICE_SYNC_MINUTE_STEP }], hour: [every], month: [every], day: { kind: "everyDay" } },
    }),
    schedule({
      name: "fraud-review-friday-13th",
      task: reconcilePayments,
      input: { window: "fraud-review" },
      pattern: {
        kind: "cron",
        minute: [values(0)],
        hour: [values(9)],
        month: [every],
        day: { kind: "bothDays", dayOfMonth: [values(13)], dayOfWeek: [{ kind: "enumValues", values: ["friday"] }] },
      },
    }),
    schedule({
      name: "nightly-export",
      task: flakyExport,
      input: { exportId: "nightly" },
      pattern: { kind: "cron", minute: [values(40)], hour: [{ kind: "range", start: 1, end: 5, step: 2 }], month: [every], day: { kind: "everyDay" } },
      enabled: false,
    }),
    schedule({
      name: "quarterly-supplier-review",
      task: syncSupplierFeed,
      input: { supplier: SUPPLIERS[2] },
      pattern: {
        kind: "cron",
        minute: [values(30)],
        hour: [values(6)],
        month: [{ kind: "enumValues", values: ["january", "april", "july", "october"] }],
        day: { kind: "byMonthDay", dayOfMonth: [values(1)] },
      },
    }),
  ];
}

/** The schedule table of the app, in the order of the Rust showcase. */
export const ACME_SCHEDULES: readonly TaskSchedule<AnyTaskContract>[] = [
  ...supplierSchedules(),
  ...regionSchedules(),
  ...calendarSchedules(),
  ...cronSchedules(),
];

/** Declare the schedule table on `app`. Calls no I/O. */
export const declareAcmeSchedules: DeclareSchedules = (app) => {
  app.schedules(ACME_SCHEDULES);
};
