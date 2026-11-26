// The implementations of the analytics and maintenance tasks. Each figure is a draw over the task's input, so the same
// input gives the same figures in every run, as in the Rust showcase.
//
// `flaky_export` is the crash story: the first attempt of a drawn export ends its child process with
// `EXPORT_CRASH_EXIT_CODE`, as a crash of a native export library does. The worker records `WORKER_CRASHED`, starts a
// new child, and the retry policy of the contract runs the export again.

import type { Horsies } from "horsies";

import {
  abandonedCartSweep,
  catalogImportChunk,
  flakyExport,
  regionalRollup,
  retentionAudit,
  salesRollup,
} from "../contracts/analytics.ts";
import type {
  AbandonedCartSweep,
  CatalogChunk,
  ExportManifest,
  RegionalRollup,
  RetentionAudit,
  SalesRollup,
} from "../domain.ts";
import { draw, integer } from "../simulate.ts";
import { CATALOG_IMPORT_ROWS_PER_CHUNK, CHAOS_EXPORT_CRASH_RATE } from "../tuning.ts";

/**
 * The exit code of a child that a drawn export ends. It is not 0 or 1, and not a code of the horsies child host
 * (71 to 80, 173): 134 is the code of a process that a native library aborts.
 */
export const EXPORT_CRASH_EXIT_CODE = 134;

/** The largest checksum of a catalog chunk: the largest unsigned 32-bit integer. */
const CHECKSUM_MAX = 4_294_967_295;

/** The hex digits of a checksum. */
const CHECKSUM_HEX_DIGITS = 8;

/** The digits of the number of an order id. */
const ORDER_NUMBER_DIGITS = 5;

/** The share of the gross that a sales rollup counts as captured: 9 of 10. */
const CAPTURED_SHARE_NUMERATOR = 9;
const CAPTURED_SHARE_DENOMINATOR = 10;

/** The share of the examined orders that a retention audit counts as prunable: 1 of 4. */
const PRUNABLE_SHARE_DENOMINATOR = 4;

/** What one attempt of an export does: it ends its child process, or it writes the export. */
export type ExportRun = "crash" | "write";

/** The sales figures of `window`. Pure. */
export function salesFigures(window: string): SalesRollup {
  const ordersCounted = integer(20, 400, [window, "orders"]);
  const grossCents = ordersCounted * integer(2_000, 9_000, [window, "basket"]);
  const capturedCents = Math.floor(grossCents / CAPTURED_SHARE_DENOMINATOR) * CAPTURED_SHARE_NUMERATOR;

  return { ordersCounted, grossCents, capturedCents };
}

/** The abandoned carts of the window `olderThanMinutes`: their count, and the oldest one when there is one. Pure. */
export function abandonedCarts(olderThanMinutes: number): AbandonedCartSweep {
  const window = String(olderThanMinutes);
  const swept = integer(0, 12, [window, "swept"]);

  if (swept === 0) {
    return { swept, oldest: { kind: "none" } };
  }

  const number = String(integer(1, 99_999, [window, "oldest"])).padStart(ORDER_NUMBER_DIGITS, "0");

  return { swept, oldest: { kind: "found", orderId: `ACME-${number}` } };
}

/** The sales figures of `region`. Pure. */
export function regionalFigures(region: string): RegionalRollup {
  const ordersCounted = integer(5, 120, [region, "orders"]);

  return { region, ordersCounted, grossCents: ordersCounted * integer(2_000, 9_000, [region, "basket"]) };
}

/** The rows of the retention window `olderThanDays`. Pure. */
export function retentionFigures(olderThanDays: number): RetentionAudit {
  const ordersExamined = integer(100, 5_000, [String(olderThanDays), "examined"]);

  return { olderThanDays, ordersExamined, rowsPrunable: Math.floor(ordersExamined / PRUNABLE_SHARE_DENOMINATOR) };
}

/** Chunk `chunkIndex` of the import `importId`: its rows and its checksum as 8 hex digits. Pure. */
export function importedChunk(importId: string, chunkIndex: number): CatalogChunk {
  const checksum = integer(0, CHECKSUM_MAX, [importId, String(chunkIndex), "checksum"]);

  return {
    chunkIndex,
    rows: CATALOG_IMPORT_ROWS_PER_CHUNK,
    checksum: checksum.toString(16).padStart(CHECKSUM_HEX_DIGITS, "0"),
  };
}

/** The manifest of the export `exportId`. Pure. */
export function exportManifest(exportId: string): ExportManifest {
  return { exportId, rows: integer(1_000, 50_000, [exportId, "rows"]) };
}

/** What attempt `attempt` of the export `exportId` does: only the first attempt of a drawn export crashes. Pure. */
export function exportRunOf(exportId: string, attempt: number): ExportRun {
  return attempt === 1 && draw(CHAOS_EXPORT_CRASH_RATE, [exportId, "export"]) ? "crash" : "write";
}

/** Implement the analytics tasks on `app`. */
export function implementAnalyticsTasks(app: Horsies): void {
  app.implement(salesRollup, async ({ window }, { ok }) => ok(salesFigures(window)));
  app.implement(abandonedCartSweep, async ({ olderThanMinutes }, { ok }) => ok(abandonedCarts(olderThanMinutes)));
  app.implement(regionalRollup, async ({ region }, { ok }) => ok(regionalFigures(region)));
  app.implement(retentionAudit, async ({ olderThanDays }, { ok }) => ok(retentionFigures(olderThanDays)));
  app.implement(catalogImportChunk, async ({ importId, chunkIndex }, { ok }) => ok(importedChunk(importId, chunkIndex)));
  app.implement(flakyExport, ({ exportId }, { ok, fencingToken }) => {
    const run = exportRunOf(exportId, fencingToken.value);

    switch (run) {
      case "crash":
        // The child ends here, as a native crash ends it: the worker records WORKER_CRASHED for this attempt.
        return process.exit(EXPORT_CRASH_EXIT_CODE);
      case "write":
        return ok(exportManifest(exportId));
      default: {
        const unexpected: never = run;

        throw new TypeError(`flaky_export: a run outside its type: ${String(unexpected)}`);
      }
    }
  });
}
