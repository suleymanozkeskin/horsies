// The logic of the order tasks: one test per outcome. The database tests run only with `ACME_DATABASE_URL`.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { reserveStock } from "../src/contracts/inventory.ts";
import { allocateWarehouse, generateInvoice, pickPack, validateOrder } from "../src/contracts/orders.ts";
import { getOrder } from "../src/store/orders.ts";
import { reserve } from "../src/tasks/inventory.ts";
import { allocate, invoice, pickAndPack, validate } from "../src/tasks/orders.ts";
import { PICK_STATIONS, WAREHOUSES } from "../src/tuning.ts";
import { uniqueId } from "./support.ts";
import {
  failed,
  resultsOf,
  seedOrder,
  seedProduct,
  stockOf,
  succeeded,
  withStore,
  withUnreachableStore,
} from "./fixtures.ts";

const allocateWarehouseResults = resultsOf(allocateWarehouse);
const generateInvoiceResults = resultsOf(generateInvoice);
const pickPackResults = resultsOf(pickPack);
const reserveStockResults = resultsOf(reserveStock);
const validateOrderResults = resultsOf(validateOrder);

describe("validate", () => {
  test("an order with lines: validated, with its line count and total", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "validate", 8);
      const order = await seedOrder(store, uniqueId("t1-validate"), [{ sku, quantity: 2 }]);
      const validation = succeeded(await validate(store, order.orderId, validateOrderResults));

      assert.deepEqual(validation, { orderId: order.orderId, lineCount: 1, totalCents: order.totalCents });

      const stored = await getOrder(store, order.orderId);

      assert.ok(stored.ok && stored.value.kind === "found");
      assert.equal(stored.value.value.status, "validated");
    });
  });

  test("an unknown order: ORDER_NOT_FOUND", async () => {
    await withStore(async (store) => {
      assert.equal(failed(await validate(store, uniqueId("t1-missing"), validateOrderResults)).code, "ORDER_NOT_FOUND");
    });
  });

  test("an order with no lines: ORDER_NOT_FOUND", async () => {
    await withStore(async (store) => {
      const order = await seedOrder(store, uniqueId("t1-empty"), []);
      const error = failed(await validate(store, order.orderId, validateOrderResults));

      assert.equal(error.code, "ORDER_NOT_FOUND");
      assert.match(error.message ?? "", /has no lines/u);
    });
  });
});

describe("pickAndPack", () => {
  test("a reserved order: the units leave the shelf, the order is packed; a second run consumes nothing", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "pick", 8);
      const order = await seedOrder(store, uniqueId("t1-pick"), [{ sku, quantity: 3 }]);

      succeeded(await reserve(store, { orderId: order.orderId, lineNo: 1, sku, quantity: 3 }, reserveStockResults));

      const picked = succeeded(await pickAndPack(store, order.orderId, pickPackResults));

      assert.equal(picked.unitsPicked, 3);
      assert.ok(PICK_STATIONS.includes(picked.station));
      assert.deepEqual(await stockOf(store, sku), { onHand: 5, reserved: 0 });

      succeeded(await pickAndPack(store, order.orderId, pickPackResults));
      assert.deepEqual(await stockOf(store, sku), { onHand: 5, reserved: 0 });

      const stored = await getOrder(store, order.orderId);

      assert.ok(stored.ok && stored.value.kind === "found");
      assert.equal(stored.value.value.status, "packed");
    });
  });

  test("an unknown order: ORDER_NOT_FOUND", async () => {
    await withStore(async (store) => {
      assert.equal(failed(await pickAndPack(store, uniqueId("t1-missing"), pickPackResults)).code, "ORDER_NOT_FOUND");
    });
  });
});

describe("invoice", () => {
  test("an order: the invoice number and the total", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "invoice", 8);
      const order = await seedOrder(store, uniqueId("t1-invoice"), [{ sku, quantity: 2 }]);

      assert.deepEqual(succeeded(await invoice(store, order.orderId, generateInvoiceResults)), {
        orderId: order.orderId,
        invoiceNumber: `INV-${order.orderId}`,
        totalCents: order.totalCents,
      });
    });
  });

  test("an unknown order: ORDER_NOT_FOUND", async () => {
    await withStore(async (store) => {
      assert.equal(failed(await invoice(store, uniqueId("t1-missing"), generateInvoiceResults)).code, "ORDER_NOT_FOUND");
    });
  });
});

describe("allocate", () => {
  test("the same SKU gives the same warehouse and distance, inside the bounds", () => {
    const first = succeeded(allocate("ACME-SKU-0001", 4, allocateWarehouseResults));

    assert.deepEqual(succeeded(allocate("ACME-SKU-0001", 4, allocateWarehouseResults)), first);
    assert.equal(first.orderId, "transfer:ACME-SKU-0001:4");
    assert.ok(WAREHOUSES.includes(first.warehouseCode));
    assert.ok(first.distanceKm >= 20 && first.distanceKm <= 1_400);
  });
});

describe("a store that does not connect", () => {
  test("each store-backed order function: STORE_UNAVAILABLE", async () => {
    await withUnreachableStore(async (store) => {
      assert.equal(failed(await validate(store, "t1-any", validateOrderResults)).code, "STORE_UNAVAILABLE");
      assert.equal(failed(await pickAndPack(store, "t1-any", pickPackResults)).code, "STORE_UNAVAILABLE");
      assert.equal(failed(await invoice(store, "t1-any", generateInvoiceResults)).code, "STORE_UNAVAILABLE");
    });
  });
});
