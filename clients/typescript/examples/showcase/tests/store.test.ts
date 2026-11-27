// The Acme store against the database: the database and the schema are idempotent, and the catalog, an order and its
// reservations read back as written. Opt-in: `ACME_DATABASE_URL` (tests/support.ts).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { ensureDatabase, ensureSchema, tableExists } from "../src/store/schema.ts";
import { countProducts, listCatalog, reservedUnits } from "../src/store/catalog.ts";
import { closeStore, type Store } from "../src/store/connection.ts";
import { consumeLine, failOrderAndReleaseReservations, getOrder, nextOrderNumber, reserveLine } from "../src/store/orders.ts";
import { seedOneLineOrder, withStore } from "./fixtures.ts";
import { testDatabase, uniqueId } from "./support.ts";

async function reservedOf(store: Store, sku: string): Promise<number> {
  const reserved = await reservedUnits(store, sku);

  assert.ok(reserved.ok && reserved.value.kind === "found", JSON.stringify(reserved));

  return reserved.value.value;
}

describe("the Acme store", () => {
  test("the database exists, and a second schema install changes nothing", async () => {
    const database = await testDatabase();

    if (database.kind === "skipped") {
      return;
    }

    try {
      assert.deepEqual(await ensureDatabase(database.settings), { ok: true, value: "existed" });
      assert.equal((await ensureSchema(database.store)).ok, true);
      assert.deepEqual(await tableExists(database.store, "acme_reruns"), { ok: true, value: true });
      assert.deepEqual(await tableExists(database.store, "acme_no_such_table"), { ok: true, value: false });
    } finally {
      await closeStore(database.store);
    }
  });

  test("an order and its catalog row read back as written", async () => {
    await withStore(async (store) => {
      const order = await seedOneLineOrder(store, uniqueId("roundtrip"), 6, 2);
      const read = await getOrder(store, order.orderId);
      const sku = order.lines[0]?.sku ?? assert.fail("the seeded order has a line");

      assert.ok(read.ok && read.value.kind === "found");
      assert.equal(read.value.value.totalCents, order.totalCents);
      assert.deepEqual(read.value.value.lines, order.lines);
      assert.equal(read.value.value.status, "placed");

      const catalog = await listCatalog(store);

      assert.ok(catalog.ok);
      assert.ok(catalog.value.some((entry) => entry.product.sku === sku && entry.stock.onHand === 6));

      const products = await countProducts(store);

      assert.ok(products.ok && products.value >= 1);
      assert.deepEqual(await getOrder(store, uniqueId("absent")), { ok: true, value: { kind: "notFound" } });
    });
  });

  test("a reservation holds units once; a compensation releases them; a consumed line stays consumed", async () => {
    await withStore(async (store) => {
      const order = await seedOneLineOrder(store, uniqueId("reserve"), 6, 2);
      const sku = order.lines[0]?.sku ?? assert.fail("the seeded order has a line");

      assert.deepEqual(await reserveLine(store, order.orderId, 1, sku, 2), { ok: true, value: { kind: "reserved", availableAfter: 4 } });
      assert.deepEqual(await reserveLine(store, order.orderId, 1, sku, 2), { ok: true, value: { kind: "alreadyReserved", available: 4 } });
      assert.equal(await reservedOf(store, sku), 2);
      assert.deepEqual(await failOrderAndReleaseReservations(store, order.orderId), { ok: true, value: 2 });
      assert.equal(await reservedOf(store, sku), 0);
      assert.deepEqual(await failOrderAndReleaseReservations(store, order.orderId), { ok: true, value: 0 });
      assert.deepEqual(await reserveLine(store, order.orderId, 1, sku, 2), {
        ok: true,
        value: { kind: "orderClosed", status: "failed" },
      });

      const second = await seedOneLineOrder(store, uniqueId("consume"), 6, 1);
      const secondSku = second.lines[0]?.sku ?? assert.fail("the seeded order has a line");

      assert.equal((await reserveLine(store, second.orderId, 1, secondSku, 1)).ok, true);
      assert.deepEqual(await consumeLine(store, second.orderId, 1, secondSku, 1), { ok: true, value: "consumed" });
      assert.deepEqual(await consumeLine(store, second.orderId, 1, secondSku, 1), { ok: true, value: "alreadyConsumed" });
      assert.deepEqual(await consumeLine(store, second.orderId, 9, secondSku, 1), { ok: true, value: "lineNotFound" });
      assert.equal(await reservedOf(store, secondSku), 0);
    });
  });

  test("each refusal of a reservation", async () => {
    await withStore(async (store) => {
      const order = await seedOneLineOrder(store, uniqueId("refuse"), 1, 3);
      const sku = order.lines[0]?.sku ?? assert.fail("the seeded order has a line");

      assert.deepEqual(await reserveLine(store, uniqueId("absent"), 1, sku, 1), { ok: true, value: { kind: "orderNotFound" } });
      assert.deepEqual(await reserveLine(store, order.orderId, 9, sku, 1), { ok: true, value: { kind: "lineNotFound" } });
      assert.deepEqual(await reserveLine(store, order.orderId, 1, uniqueId("no-sku"), 1), { ok: true, value: { kind: "unknownSku" } });
      assert.deepEqual(await reserveLine(store, order.orderId, 1, sku, 3), {
        ok: true,
        value: { kind: "insufficientStock", available: 1 },
      });
    });
  });

  test("the order sequence grows", async () => {
    await withStore(async (store) => {
      const first = await nextOrderNumber(store);
      const second = await nextOrderNumber(store);

      assert.ok(first.ok && second.ok);
      assert.ok(second.value > first.value);
    });
  });
});
