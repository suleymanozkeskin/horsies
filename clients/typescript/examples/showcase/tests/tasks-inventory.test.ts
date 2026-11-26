// The logic of the stock tasks: one test per outcome. The database tests run only with `ACME_DATABASE_URL`.
//
// The stocktake test clears every reservation of the database, as the nightly stocktake does: run this file alone,
// or with a test concurrency of 1, when other files of the run hold reservations.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { TaskOutcome } from "horsies";

import { releaseStock, replenishCatalog, reserveStock, syncSupplierFeed, updateStockLevels } from "../src/contracts/inventory.ts";
import type { SupplierFeed } from "../src/domain.ts";
import { draw } from "../src/simulate.ts";
import type { Store } from "../src/store/connection.ts";
import { getOrder } from "../src/store/orders.ts";
import { release, replenish, reserve, stockUpdate, supplierFeed } from "../src/tasks/inventory.ts";
import { RESTOCK_SKUS_PER_SUPPLIER, STOCKTAKE_CEILING_UNITS, SUPPLIER_TIMEOUT_RATE } from "../src/tuning.ts";
import { uniqueId } from "./support.ts";
import {
  failed,
  idWhere,
  resultsOf,
  seedOrder,
  seedProduct,
  stockOf,
  succeeded,
  upstreamError,
  withStore,
  withUnreachableStore,
} from "./fixtures.ts";

const releaseStockResults = resultsOf(releaseStock);
const replenishCatalogResults = resultsOf(replenishCatalog);
const reserveStockResults = resultsOf(reserveStock);
const syncSupplierFeedResults = resultsOf(syncSupplierFeed);
const updateStockLevelsResults = resultsOf(updateStockLevels);

/** The status of `orderId` in the store. */
async function statusOf(store: Store, orderId: string): Promise<string> {
  const read = await getOrder(store, orderId);

  assert.ok(read.ok && read.value.kind === "found", `the order ${orderId}`);

  return read.value.value.status;
}

describe("reserve", () => {
  test("a line with stock: reserved, with the units available after", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "reserve", 8);
      const order = await seedOrder(store, uniqueId("t1-reserve"), [{ sku, quantity: 3 }]);
      const reservation = succeeded(await reserve(store, { orderId: order.orderId, lineNo: 1, sku, quantity: 3 }, reserveStockResults));

      assert.deepEqual(reservation, { orderId: order.orderId, sku, quantity: 3, availableAfter: 5, replayed: false });
      assert.deepEqual(await stockOf(store, sku), { onHand: 8, reserved: 3 });
    });
  });

  test("a line that an earlier run reserved: a replay that changes nothing", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "replay", 8);
      const order = await seedOrder(store, uniqueId("t1-replay"), [{ sku, quantity: 3 }]);
      const request = { orderId: order.orderId, lineNo: 1, sku, quantity: 3 };

      succeeded(await reserve(store, request, reserveStockResults));

      const replay = succeeded(await reserve(store, request, reserveStockResults));

      assert.equal(replay.replayed, true);
      assert.equal(replay.availableAfter, 5);
      assert.deepEqual(await stockOf(store, sku), { onHand: 8, reserved: 3 });
    });
  });

  test("an unknown order: ORDER_NOT_FOUND", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "no-order", 8);
      const error = failed(await reserve(store, { orderId: uniqueId("t1-missing"), lineNo: 1, sku, quantity: 1 }, reserveStockResults));

      assert.equal(error.code, "ORDER_NOT_FOUND");
    });
  });

  test("an order past the reservable statuses: ORDER_CLOSED, and the order stays as it is", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "closed", 8);
      const order = await seedOrder(store, uniqueId("t1-closed"), [{ sku, quantity: 1 }], "captured");

      assert.equal(
        failed(await reserve(store, { orderId: order.orderId, lineNo: 1, sku, quantity: 1 }, reserveStockResults)).code,
        "ORDER_CLOSED",
      );
      assert.equal(await statusOf(store, order.orderId), "captured");
    });
  });

  test("a line number that the order does not have: UNKNOWN_SKU, and the order fails", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "no-line", 8);
      const order = await seedOrder(store, uniqueId("t1-no-line"), [{ sku, quantity: 1 }]);

      assert.equal(
        failed(await reserve(store, { orderId: order.orderId, lineNo: 9, sku, quantity: 1 }, reserveStockResults)).code,
        "UNKNOWN_SKU",
      );
      assert.equal(await statusOf(store, order.orderId), "failed");
    });
  });

  test("a SKU that the catalog does not have: UNKNOWN_SKU, and the order fails", async () => {
    await withStore(async (store) => {
      const sku = uniqueId("t1-not-in-catalog");
      const order = await seedOrder(store, uniqueId("t1-unknown-sku"), [{ sku, quantity: 1 }]);

      assert.equal(
        failed(await reserve(store, { orderId: order.orderId, lineNo: 1, sku, quantity: 1 }, reserveStockResults)).code,
        "UNKNOWN_SKU",
      );
      assert.equal(await statusOf(store, order.orderId), "failed");
    });
  });

  test("a short line: INSUFFICIENT_STOCK; the order fails and its other reservations are released", async () => {
    await withStore(async (store) => {
      const stocked = await seedProduct(store, "stocked", 8);
      const short = await seedProduct(store, "short", 1);

      const order = await seedOrder(store, uniqueId("t1-short"), [
        { sku: stocked, quantity: 2 },
        { sku: short, quantity: 4 },
      ]);

      succeeded(await reserve(store, { orderId: order.orderId, lineNo: 1, sku: stocked, quantity: 2 }, reserveStockResults));

      const error = failed(await reserve(store, { orderId: order.orderId, lineNo: 2, sku: short, quantity: 4 }, reserveStockResults));

      assert.equal(error.code, "INSUFFICIENT_STOCK");
      assert.deepEqual(error.data, { sku: short, available: 1 });
      assert.equal(await statusOf(store, order.orderId), "failed");
      assert.deepEqual(await stockOf(store, stocked), { onHand: 8, reserved: 0 });
    });
  });
});

describe("release", () => {
  test("reserved units of a SKU: back on the shelf", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "release", 8);
      const order = await seedOrder(store, uniqueId("t1-release"), [{ sku, quantity: 3 }]);

      succeeded(await reserve(store, { orderId: order.orderId, lineNo: 1, sku, quantity: 3 }, reserveStockResults));

      assert.deepEqual(succeeded(await release(store, sku, 3, releaseStockResults)), { sku, quantity: 3, availableAfter: 8 });
    });
  });

  test("a SKU that the catalog does not have: UNKNOWN_SKU", async () => {
    await withStore(async (store) => {
      assert.equal(failed(await release(store, uniqueId("t1-not-in-catalog"), 1, releaseStockResults)).code, "UNKNOWN_SKU");
    });
  });
});

describe("replenish", () => {
  test("the stocktake brings the shelf into the bounds and clears the reservations", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "stocktake", STOCKTAKE_CEILING_UNITS + 1_000);
      const order = await seedOrder(store, uniqueId("t1-stocktake"), [{ sku, quantity: 3 }]);

      succeeded(await reserve(store, { orderId: order.orderId, lineNo: 1, sku, quantity: 3 }, reserveStockResults));

      // Target 0: no SKU below the target is topped up, so the other rows of the database keep their shelf.
      const summary = succeeded(await replenish(store, 0, replenishCatalogResults));

      assert.equal(summary.targetUnits, 0);
      assert.ok(summary.skusToppedUp >= 1 && summary.reservationsCleared >= 1);
      assert.deepEqual(await stockOf(store, sku), { onHand: STOCKTAKE_CEILING_UNITS, reserved: 0 });
    });
  });
});

describe("supplierFeed", () => {
  const drawn = idWhere("supplier-timeout", (supplier) => draw(SUPPLIER_TIMEOUT_RATE, [supplier, "timeout"]));
  const steady = idWhere("supplier-steady", (supplier) => !draw(SUPPLIER_TIMEOUT_RATE, [supplier, "timeout"]));

  test("a drawn supplier on its first attempt: SUPPLIER_TIMEOUT", () => {
    assert.equal(failed(supplierFeed(drawn, 1, syncSupplierFeedResults)).code, "SUPPLIER_TIMEOUT");
  });

  test("a drawn supplier on a later attempt, and a supplier that is not drawn: the feed", () => {
    const feed = succeeded(supplierFeed(drawn, 2, syncSupplierFeedResults));

    assert.equal(feed.supplier, drawn);
    assert.equal(feed.skuCount, RESTOCK_SKUS_PER_SUPPLIER);
    assert.ok(feed.changedCount >= 0 && feed.changedCount <= 5);
    assert.deepEqual(
      succeeded(supplierFeed(steady, 1, syncSupplierFeedResults)),
      succeeded(supplierFeed(steady, 1, syncSupplierFeedResults)),
    );
  });
});

describe("stockUpdate", () => {
  /** A succeeded feed of `supplier` that changed `changedCount` SKUs. */
  function feed(supplier: string, changedCount: number): TaskOutcome<SupplierFeed> {
    return { kind: "succeeded", value: { supplier, skuCount: RESTOCK_SKUS_PER_SUPPLIER, changedCount } };
  }

  test("enough feeds: their changes, and the count of the skipped ones", () => {
    assert.deepEqual(succeeded(stockUpdate([feed("atlas", 2), feed("brera", 3), undefined], updateStockLevelsResults)), {
      suppliersApplied: ["atlas", "brera"],
      applied: 5,
      skipped: 1,
    });
  });

  test("fewer succeeded feeds than the quorum: QUORUM_NOT_MET", () => {
    const failedFeed: TaskOutcome<SupplierFeed> = { kind: "failed", error: upstreamError("SUPPLIER_TIMEOUT") };
    const error = failed(stockUpdate([feed("atlas", 2), failedFeed, undefined], updateStockLevelsResults));

    assert.equal(error.code, "QUORUM_NOT_MET");
    assert.match(error.message ?? "", /1 supplier feeds reported, 2 needed/u);
  });
});

describe("a store that does not connect", () => {
  test("each store-backed stock function: STORE_UNAVAILABLE", async () => {
    await withUnreachableStore(async (store) => {
      assert.equal(
        failed(await reserve(store, { orderId: "t1-any", lineNo: 1, sku: "t1-any", quantity: 1 }, reserveStockResults)).code,
        "STORE_UNAVAILABLE",
      );
      assert.equal(failed(await release(store, "t1-any", 1, releaseStockResults)).code, "STORE_UNAVAILABLE");
      assert.equal(failed(await replenish(store, 0, replenishCatalogResults)).code, "STORE_UNAVAILABLE");
    });
  });
});
