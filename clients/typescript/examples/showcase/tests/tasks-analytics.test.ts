// The analytics task logic: one test per outcome of each function. No database: each figure is a draw over the input.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { draw } from "../src/simulate.ts";
import {
  abandonedCarts,
  EXPORT_CRASH_EXIT_CODE,
  exportManifest,
  exportRunOf,
  importedChunk,
  regionalFigures,
  retentionFigures,
  salesFigures,
} from "../src/tasks/analytics.ts";
import { CATALOG_IMPORT_ROWS_PER_CHUNK, CHAOS_EXPORT_CRASH_RATE } from "../src/tuning.ts";

/** The most ids that a search for a draw tries. */
const ID_SEARCH_BOUND = 100_000;

/** The first id `<prefix>-<n>` that `wanted` accepts. A search past the bound throws: the draw rate is wrong. */
function firstId(prefix: string, wanted: (id: string) => boolean): string {
  for (let index = 0; index < ID_SEARCH_BOUND; index += 1) {
    const id = `${prefix}-${String(index)}`;

    if (wanted(id)) {
      return id;
    }
  }

  throw new Error(`no id of ${prefix} in ${String(ID_SEARCH_BOUND)} tries`);
}

/** The first window of minutes, from 1, whose sweep has `swept === 0` (`empty`) or more (`some`). */
function sweepWindow(kind: "empty" | "some"): number {
  for (let minutes = 1; minutes < ID_SEARCH_BOUND; minutes += 1) {
    const swept = abandonedCarts(minutes).swept;

    if ((kind === "empty") === (swept === 0)) {
      return minutes;
    }
  }

  throw new Error(`no sweep window of kind ${kind}`);
}

describe("sales and regional figures", () => {
  test("a window gives the same figures each time, with 9 of 10 of the gross captured", () => {
    const figures = salesFigures("daily");

    assert.deepEqual(salesFigures("daily"), figures);
    assert.ok(figures.ordersCounted >= 20 && figures.ordersCounted <= 400);
    assert.equal(figures.capturedCents, Math.floor(figures.grossCents / 10) * 9);
  });

  test("a region gives its own name and a gross of its orders", () => {
    const figures = regionalFigures("uk");

    assert.equal(figures.region, "uk");
    assert.ok(figures.ordersCounted >= 5 && figures.ordersCounted <= 120);
    assert.equal(figures.grossCents % figures.ordersCounted, 0);
  });
});

describe("abandonedCarts", () => {
  test("a window with no stranded cart has no oldest order", () => {
    const sweep = abandonedCarts(sweepWindow("empty"));

    assert.equal(sweep.swept, 0);
    assert.deepEqual(sweep.oldest, { kind: "none" });
  });

  test("a window with stranded carts names the oldest order as ACME-nnnnn", () => {
    const sweep = abandonedCarts(sweepWindow("some"));

    assert.ok(sweep.swept > 0);
    assert.ok(sweep.oldest.kind === "found");
    assert.match(sweep.oldest.orderId, /^ACME-\d{5}$/u);
  });
});

describe("retentionFigures", () => {
  test("a quarter of the examined orders is prunable", () => {
    const audit = retentionFigures(30);

    assert.equal(audit.olderThanDays, 30);
    assert.equal(audit.rowsPrunable, Math.floor(audit.ordersExamined / 4));
  });
});

describe("importedChunk", () => {
  test("a chunk has the tuned rows and a checksum of 8 hex digits", () => {
    const chunk = importedChunk("import-1", 7);

    assert.equal(chunk.chunkIndex, 7);
    assert.equal(chunk.rows, CATALOG_IMPORT_ROWS_PER_CHUNK);
    assert.match(chunk.checksum, /^[0-9a-f]{8}$/u);
    assert.deepEqual(importedChunk("import-1", 7), chunk);
    assert.notEqual(importedChunk("import-1", 8).checksum, chunk.checksum);
  });
});

describe("exportRunOf and exportManifest", () => {
  const drawn = firstId("chaos-export", (id) => draw(CHAOS_EXPORT_CRASH_RATE, [id, "export"]));
  const kept = firstId("chaos-export", (id) => !draw(CHAOS_EXPORT_CRASH_RATE, [id, "export"]));

  test("the first attempt of a drawn export crashes", () => {
    assert.equal(exportRunOf(drawn, 1), "crash");
  });

  test("a later attempt of a drawn export writes", () => {
    assert.equal(exportRunOf(drawn, 2), "write");
  });

  test("the first attempt of an export that is not drawn writes", () => {
    assert.equal(exportRunOf(kept, 1), "write");
  });

  test("the manifest names the export and its rows", () => {
    const manifest = exportManifest(kept);

    assert.equal(manifest.exportId, kept);
    assert.ok(manifest.rows >= 1_000 && manifest.rows <= 50_000);
  });

  test("the crash exit code is not a code of the horsies child host", () => {
    const childHostCodes = [0, 1, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 173];

    assert.ok(!childHostCodes.includes(EXPORT_CRASH_EXIT_CODE));
  });
});
