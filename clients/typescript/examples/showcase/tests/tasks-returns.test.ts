// The logic of the return tasks: one test per outcome. The database tests run only with `ACME_DATABASE_URL`.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { ReturnCase } from "../src/domain.ts";
import { draw } from "../src/simulate.ts";
import type { Store } from "../src/store/connection.ts";
import { getReturn, recordInspection } from "../src/store/returns.ts";
import { inspect, receive, type ReturnRequest, settle } from "../src/tasks/returns.ts";
import { RETURN_DAMAGE_RATE } from "../src/tuning.ts";
import { uniqueId } from "./support.ts";
import { failed, idWhere, seedOrder, seedProduct, stockOf, succeeded, upstreamError, withStore, withUnreachableStore } from "./fixtures.ts";

/** The units of every test return. */
const RETURNED_UNITS = 2;

/** The units on the shelf of every seeded product. */
const SHELF_UNITS = 8;

/** A received return of a seeded order, under `returnId`, of a product that the catalog has (or not). */
async function seedReturn(store: Store, returnId: string, sku: string): Promise<ReturnRequest> {
  const order = await seedOrder(store, uniqueId("t1-return-order"), [{ sku, quantity: RETURNED_UNITS }], "captured");
  const request = { returnId, orderId: order.orderId, sku, quantity: RETURNED_UNITS };

  succeeded(await receive(store, request));

  return request;
}

async function caseOf(store: Store, returnId: string): Promise<ReturnCase> {
  const read = await getReturn(store, returnId);

  assert.ok(read.ok && read.value.kind === "found", `the return ${returnId}`);

  return read.value.value;
}

/** A return id that the damage draw does not mark. */
function intactId(): string {
  return idWhere(uniqueId("t1-intact"), (id) => !draw(RETURN_DAMAGE_RATE, [id, "damage"]));
}

describe("receive", () => {
  test("a returned item: the receipt, and a received case with no condition", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "receive", SHELF_UNITS);
      const request = await seedReturn(store, uniqueId("t1-receive"), sku);
      const stored = await caseOf(store, request.returnId);

      assert.equal(stored.status, "received");
      assert.equal(stored.condition, null);
      assert.equal(stored.quantity, RETURNED_UNITS);
    });
  });

  test("a case of the id that exists: the receipt, and the case stays as it is", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "receive-again", SHELF_UNITS);
      const request = await seedReturn(store, uniqueId("t1-receive-again"), sku);

      const inspected = await recordInspection(store, request.returnId, "resellable");

      assert.ok(inspected.ok && inspected.value);
      assert.deepEqual(succeeded(await receive(store, request)), request);
      assert.equal((await caseOf(store, request.returnId)).status, "inspected");
    });
  });
});

describe("inspect", () => {
  test("an item that the draw does not damage: resellable, and the case is inspected", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "intact", SHELF_UNITS);
      const request = await seedReturn(store, intactId(), sku);
      const inspection = succeeded(await inspect(store, request.returnId));

      const stored = await caseOf(store, request.returnId);

      assert.equal(inspection.condition, "resellable");
      assert.equal(stored.status, "inspected");
      assert.equal(stored.condition, "resellable");
    });
  });

  test("a drawn damage: DAMAGED_ITEM with the item, and the condition is recorded", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "damaged", SHELF_UNITS);
      const returnId = idWhere(uniqueId("t1-damaged"), (id) => draw(RETURN_DAMAGE_RATE, [id, "damage"]));
      const request = await seedReturn(store, returnId, sku);
      const error = failed(await inspect(store, request.returnId));

      assert.equal(error.code, "DAMAGED_ITEM");
      assert.deepEqual(error.data, { returnId, sku, quantity: RETURNED_UNITS });
      assert.equal((await caseOf(store, returnId)).condition, "damaged");
    });
  });

  test("an unknown case: RETURN_NOT_FOUND", async () => {
    await withStore(async (store) => {
      assert.equal(failed(await inspect(store, uniqueId("t1-missing"))).code, "RETURN_NOT_FOUND");
    });
  });
});

describe("settle", () => {
  test("a passed inspection: the units go back on the shelf, and the case is restocked", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "restock", SHELF_UNITS);
      const request = await seedReturn(store, intactId(), sku);
      const inspection = succeeded(await inspect(store, request.returnId));
      const decision = succeeded(await settle(store, request.returnId, { kind: "succeeded", value: inspection }));

      assert.deepEqual(decision.outcome, { kind: "restocked", availableAfter: SHELF_UNITS + RETURNED_UNITS });
      assert.deepEqual(await stockOf(store, sku), { onHand: SHELF_UNITS + RETURNED_UNITS, reserved: 0 });
      assert.equal((await caseOf(store, request.returnId)).status, "restocked");
    });
  });

  test("a failed inspection: the item is written off, and the shelf stays as it is", async () => {
    await withStore(async (store) => {
      const sku = await seedProduct(store, "write-off", SHELF_UNITS);
      const request = await seedReturn(store, uniqueId("t1-write-off"), sku);
      const decision = succeeded(await settle(store, request.returnId, { kind: "failed", error: upstreamError("DAMAGED_ITEM") }));

      assert.deepEqual(decision.outcome, { kind: "writtenOff" });
      assert.deepEqual(await stockOf(store, sku), { onHand: SHELF_UNITS, reserved: 0 });
      assert.equal((await caseOf(store, request.returnId)).status, "written_off");
    });
  });

  test("an unknown case: RETURN_NOT_FOUND", async () => {
    await withStore(async (store) => {
      const error = failed(await settle(store, uniqueId("t1-missing"), { kind: "failed", error: upstreamError("DAMAGED_ITEM") }));

      assert.equal(error.code, "RETURN_NOT_FOUND");
    });
  });

  test("a passed inspection of a SKU that the catalog does not have: UNKNOWN_SKU", async () => {
    await withStore(async (store) => {
      const sku = uniqueId("t1-not-in-catalog");
      const request = await seedReturn(store, intactId(), sku);
      const inspection = succeeded(await inspect(store, request.returnId));

      assert.equal(failed(await settle(store, request.returnId, { kind: "succeeded", value: inspection })).code, "UNKNOWN_SKU");
    });
  });
});

describe("a store that does not connect", () => {
  test("each store-backed return function: STORE_UNAVAILABLE", async () => {
    await withUnreachableStore(async (store) => {
      const request = { returnId: "t1-any", orderId: "t1-any", sku: "t1-any", quantity: 1 };

      assert.equal(failed(await receive(store, request)).code, "STORE_UNAVAILABLE");
      assert.equal(failed(await inspect(store, "t1-any")).code, "STORE_UNAVAILABLE");
      assert.equal(failed(await settle(store, "t1-any", { kind: "failed", error: upstreamError("DAMAGED_ITEM") })).code, "STORE_UNAVAILABLE");
    });
  });
});
