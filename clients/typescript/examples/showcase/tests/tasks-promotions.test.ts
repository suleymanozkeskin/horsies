// The promotion, loyalty and price task logic: one test per outcome of each function. The pricing and the draws run
// with no database; `applyPromotionsWith` runs against the database of `ACME_DATABASE_URL` (opt-in: with no variable,
// those tests pass with no check).

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe, test } from "node:test";

import { applyPromotions, computeLoyaltyPoints } from "../src/contracts/promotions.ts";
import type { OrderLine } from "../src/domain.ts";
import { draw } from "../src/simulate.ts";
import { loadCatalog, reservedUnits } from "../src/store/catalog.ts";
import { closeStore, openStore, type Store } from "../src/store/connection.ts";
import { getOrder, insertOrder, reserveLine } from "../src/store/orders.ts";
import {
  applyPromotionsWith,
  bundleDiscountCents,
  campaignDiscountPercent,
  cacheWarm,
  listPriceCents,
  LOYALTY_FAULT_MESSAGE,
  LOYALTY_TIER,
  loyaltyLifetimeEuros,
  loyaltyPoints,
  priceDrawnClearanceBundle,
  pricePush,
  priceUpdate,
  PROMOTION_SIZE_CODE,
  promotionSizeCode,
  searchPrewarm,
  SizeCodeError,
  sizeMultiplierPercent,
} from "../src/tasks/promotions.ts";
import {
  BUNDLE_PRICE_FLOOR_CENTS,
  CLEARANCE_PRICE_CENTS,
  CORRUPT_SIZE_CODE,
  FLASH_SALE_DISCOUNT_PERCENT,
  LOYALTY_ENGINE_BUG_RATE,
  LOYALTY_POINTS_PER_EURO,
  PROMOTION_BUNDLE_BUG_RATE,
  PROMOTION_CODES,
  PROMOTION_SIZE_CODE_BUG_RATE,
} from "../src/tuning.ts";
import { idWhere, resultsOf, UNREACHABLE_URL } from "./fixtures.ts";
import { testDatabase } from "./support.ts";

const applyPromotionsResults = resultsOf(applyPromotions);
const computeLoyaltyPointsResults = resultsOf(computeLoyaltyPoints);

function bundleDrawn(id: string): boolean {
  return draw(PROMOTION_BUNDLE_BUG_RATE, [id, "bundle-bug"]);
}

function sizeDrawn(id: string): boolean {
  return draw(PROMOTION_SIZE_CODE_BUG_RATE, [id, "size-code"]);
}

/** An id that draws neither fault of the promotion story. */
function cleanId(prefix: string): string {
  return idWhere(prefix, (id) => !bundleDrawn(id) && !sizeDrawn(id));
}

function line(lineNo: number, quantity: number, unitPriceCents: number, sku = "sku"): OrderLine {
  return { lineNo, sku, sizeCode: PROMOTION_SIZE_CODE, quantity, unitPriceCents };
}

/** Write the order `orderId` with `lines`, each line on a SKU of its own with 8 units on hand. */
async function seedPricedOrder(store: Store, orderId: string, lines: readonly OrderLine[]): Promise<void> {
  const products = lines.map((each) => ({ sku: each.sku, name: "T2 test item", category: "tests", priceCents: each.unitPriceCents }));
  const loaded = await loadCatalog(store, products, lines.map((each) => ({ sku: each.sku, onHand: 8, reserved: 0 })));

  assert.ok(loaded.ok, "the catalog rows");

  const inserted = await insertOrder(store, {
    orderId,
    customerId: `customer-${orderId}`,
    status: "placed",
    totalCents: lines.reduce((sum, each) => sum + each.quantity * each.unitPriceCents, 0),
    lines: [...lines],
    createdAt: Temporal.Now.instant(),
  });

  assert.ok(inserted.ok, "the order row");
}

function sku(prefix: string): string {
  return `t2-${prefix}-${randomUUID().slice(0, 8)}`;
}

describe("bundleDiscountCents", () => {
  test("an order with no bundled line has no discount", () => {
    assert.equal(bundleDiscountCents([line(1, 1, 5_000)]), 0);
  });

  test("a tenth of the bundled total is shared by the units at the price floor or above", () => {
    assert.equal(bundleDiscountCents([line(1, 2, BUNDLE_PRICE_FLOOR_CENTS)]), (2 * BUNDLE_PRICE_FLOOR_CENTS) / 10 / 2);
  });

  test("bundled lines that are all below the price floor divide by zero: RangeError", () => {
    assert.throws(() => bundleDiscountCents([line(1, 2, CLEARANCE_PRICE_CENTS)]), RangeError);
  });
});

describe("sizeMultiplierPercent and promotionSizeCode", () => {
  test("a size of the table gives its multiplier", () => {
    assert.equal(sizeMultiplierPercent("L"), 105);
  });

  test("a size outside the table throws SizeCodeError with the code", () => {
    assert.throws(() => sizeMultiplierPercent(CORRUPT_SIZE_CODE), (cause: unknown) => cause instanceof SizeCodeError && cause.sizeCode === CORRUPT_SIZE_CODE);
  });

  test("an order that the size fault draws prices the corrupt code; another order prices the base size", () => {
    assert.equal(promotionSizeCode(idWhere("t2-size", sizeDrawn)), CORRUPT_SIZE_CODE);
    assert.equal(promotionSizeCode(idWhere("t2-size", (id) => !sizeDrawn(id))), PROMOTION_SIZE_CODE);
  });
});

describe("priceDrawnClearanceBundle", () => {
  test("an order that the bundle fault draws throws RangeError", () => {
    assert.throws(() => {
      priceDrawnClearanceBundle(idWhere("t2-bundle", bundleDrawn));
    }, RangeError);
  });

  test("another order prices nothing", () => {
    assert.doesNotThrow(() => {
      priceDrawnClearanceBundle(idWhere("t2-bundle", (id) => !bundleDrawn(id)));
    });
  });
});

describe("loyaltyPoints", () => {
  test("a customer earns the tuned points per euro of the lifetime spend", () => {
    const customerId = idWhere("t2-loyal", (id) => !draw(LOYALTY_ENGINE_BUG_RATE, [id, "lifetime-bug"]));
    const result = loyaltyPoints(customerId, "ACME-00001", computeLoyaltyPointsResults);

    assert.ok(result.ok);
    assert.deepEqual(result.value, {
      customerId,
      orderId: "ACME-00001",
      points: loyaltyLifetimeEuros(customerId) * LOYALTY_POINTS_PER_EURO,
      tier: LOYALTY_TIER,
    });
  });

  test("an engine fault of a drawn customer is LOYALTY_ENGINE_BUG with the fault's message", () => {
    const customerId = idWhere("t2-loyal", (id) => draw(LOYALTY_ENGINE_BUG_RATE, [id, "lifetime-bug"]));
    const result = loyaltyPoints(customerId, "ACME-00001", computeLoyaltyPointsResults);

    assert.ok(!result.ok);
    assert.equal(result.error.code, "LOYALTY_ENGINE_BUG");
    assert.equal(result.error.message, LOYALTY_FAULT_MESSAGE);
  });
});

describe("price, search and cache figures", () => {
  test("a price push carries the list price of the SKU to the target", () => {
    assert.deepEqual(pricePush("ACME-SKU-0001", "cdn:spring"), {
      sku: "ACME-SKU-0001",
      priceCents: listPriceCents("ACME-SKU-0001"),
      target: "cdn:spring",
    });
  });

  test("a campaign takes the flash sale discount off the list price", () => {
    const update = priceUpdate("ACME-SKU-0002", "spring");

    assert.equal(campaignDiscountPercent("spring"), FLASH_SALE_DISCOUNT_PERCENT);
    assert.equal(update.nowCents, Math.floor((update.wasCents * (100 - FLASH_SALE_DISCOUNT_PERCENT)) / 100));
  });

  test("no campaign (an empty id) keeps the list price", () => {
    const update = priceUpdate("ACME-SKU-0002", "");

    assert.equal(update.nowCents, update.wasCents);
  });

  test("the search index and the edge cache are named by the campaign", () => {
    assert.equal(searchPrewarm("spring").indexName, "products-spring");
    assert.equal(cacheWarm("spring").target, "spring");
  });
});

describe("applyPromotionsWith with no database", () => {
  test("an order that the bundle fault draws throws RangeError before any store call", async () => {
    const store = openStore(UNREACHABLE_URL);

    try {
      await assert.rejects(applyPromotionsWith(store, idWhere("t2-apply-bundle", bundleDrawn), applyPromotionsResults), RangeError);
    } finally {
      await closeStore(store);
    }
  });

  test("a store that does not connect is STORE_UNAVAILABLE", async () => {
    const store = openStore(UNREACHABLE_URL);

    try {
      const result = await applyPromotionsWith(store, cleanId("t2-apply-store"), applyPromotionsResults);

      assert.ok(!result.ok);
      assert.equal(result.error.code, "STORE_UNAVAILABLE");
      assert.deepEqual(result.error.data, { operation: "get_order" });
    } finally {
      await closeStore(store);
    }
  });
});

describe("applyPromotionsWith on the database", () => {
  test("an order that the size fault draws fails, releases its reservations and throws SizeCodeError", async () => {
    const database = await testDatabase();

    if (database.kind === "skipped") {
      return;
    }

    const { store } = database;
    const orderId = idWhere(`t2-size-${randomUUID().slice(0, 8)}`, (id) => sizeDrawn(id) && !bundleDrawn(id));
    const lineSku = sku("size");

    try {
      await seedPricedOrder(store, orderId, [line(1, 2, 2_000, lineSku)]);
      const reserved = await reserveLine(store, orderId, 1, lineSku, 2);

      assert.ok(reserved.ok && reserved.value.kind === "reserved", "the line is reserved before the promotion");

      await assert.rejects(applyPromotionsWith(store, orderId, applyPromotionsResults), SizeCodeError);

      const order = await getOrder(store, orderId);
      const units = await reservedUnits(store, lineSku);

      assert.ok(order.ok && order.value.kind === "found");
      assert.equal(order.value.value.status, "failed");
      assert.deepEqual(units.ok ? units.value : null, { kind: "found", value: 0 });
    } finally {
      await closeStore(store);
    }
  });

  test("an order of the store gets the bundle discount of its lines and the spring code", async () => {
    const database = await testDatabase();

    if (database.kind === "skipped") {
      return;
    }

    const { store } = database;
    const orderId = cleanId(`t2-ok-${randomUUID().slice(0, 8)}`);
    const lines = [line(1, 2, 3_000, sku("ok-a")), line(2, 3, 1_500, sku("ok-b"))];

    try {
      await seedPricedOrder(store, orderId, lines);

      const result = await applyPromotionsWith(store, orderId, applyPromotionsResults);

      assert.ok(result.ok);
      assert.deepEqual(result.value, { orderId, discountCents: bundleDiscountCents(lines), appliedCodes: [PROMOTION_CODES[0]] });
    } finally {
      await closeStore(store);
    }
  });

  test("an id with no order is ORDER_NOT_FOUND", async () => {
    const database = await testDatabase();

    if (database.kind === "skipped") {
      return;
    }

    const { store } = database;

    try {
      const result = await applyPromotionsWith(store, cleanId(`t2-none-${randomUUID().slice(0, 8)}`), applyPromotionsResults);

      assert.ok(!result.ok);
      assert.equal(result.error.code, "ORDER_NOT_FOUND");
    } finally {
      await closeStore(store);
    }
  });
});
