// The contracts of the analytics and maintenance tasks. Each one runs on a seat of the shared async child, and its
// figures are draws over its input.

import { defineTask, h } from "horsies";

import { AbandonedCartSweep, CatalogChunk, ExportManifest, RegionalRollup, RetentionAudit, SalesRollup } from "../domain.ts";
import { SHARED_TASK_TIMEOUT_MS } from "../tuning.ts";
import { CRASH_RETRY, EXPORT_RETRY, QUEUE_ANALYTICS } from "./policies.ts";

/** The sales figures of one window (`daily`, `weekly`, ...). */
export const salesRollup = defineTask({
  name: "sales_rollup",
  queue: QUEUE_ANALYTICS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { window: h.string() },
  output: SalesRollup,
  errorCodes: [],
  retryPolicy: CRASH_RETRY,
});

/** Find the carts that no order completed in the window. */
export const abandonedCartSweep = defineTask({
  name: "abandoned_cart_sweep",
  queue: QUEUE_ANALYTICS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { olderThanMinutes: h.int() },
  output: AbandonedCartSweep,
  errorCodes: [],
  retryPolicy: CRASH_RETRY,
});

/** The sales figures of one region. */
export const regionalRollup = defineTask({
  name: "regional_rollup",
  queue: QUEUE_ANALYTICS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { region: h.string() },
  output: RegionalRollup,
  errorCodes: [],
  retryPolicy: CRASH_RETRY,
});

/** Count the rows that are older than the retention window. */
export const retentionAudit = defineTask({
  name: "retention_audit",
  queue: QUEUE_ANALYTICS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { olderThanDays: h.int() },
  output: RetentionAudit,
  errorCodes: [],
  retryPolicy: CRASH_RETRY,
});

/** Import one chunk of a supplier catalog. */
export const catalogImportChunk = defineTask({
  name: "catalog_import_chunk",
  queue: QUEUE_ANALYTICS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { importId: h.string(), chunkIndex: h.int() },
  output: CatalogChunk,
  errorCodes: [],
  retryPolicy: CRASH_RETRY,
});

/**
 * Write one export. The first attempt of a drawn export ends its child process, as a crash of a native export library
 * does: the worker records `WORKER_CRASHED`, starts a new child, and the retry policy runs the export again. The task
 * runs in an isolated child, so the crash ends no other task.
 */
export const flakyExport = defineTask({
  name: "flaky_export",
  queue: QUEUE_ANALYTICS,
  input: { exportId: h.string() },
  output: ExportManifest,
  errorCodes: [],
  retryPolicy: EXPORT_RETRY,
});

export const ANALYTICS_TASKS = [
  salesRollup,
  abandonedCartSweep,
  regionalRollup,
  retentionAudit,
  catalogImportChunk,
  flakyExport,
] as const;
