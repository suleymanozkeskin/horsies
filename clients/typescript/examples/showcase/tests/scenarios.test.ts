// The scenarios of the showcase, with no database: the seed catalog, the deterministic orders and their drawn faults,
// the parse of the scenario arguments, and the two runs that touch no database (`simulate`, and a usage error).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { availableUnits, type CatalogEntry } from "../src/store/catalog.ts";
import { buildOrder, catalogSeed, orderIdOf, product, skuOf, stockFallsShort } from "../src/scenarios/catalog.ts";
import { parseSteadyArgs, runScenario, SCENARIO_NAMES, SCENARIO_USAGE, scenarioNameOf } from "../src/scenarios/index.ts";
import { draw } from "../src/simulate.ts";
import {
  BUNDLE_MIN_QUANTITY,
  CATALOG_SIZE,
  CATALOG_STOCK_PER_SKU,
  CLEARANCE_PRICE_CENTS,
  CORRUPT_SIZE_CODE,
  DISCONTINUED_SKU_COUNT,
  MAX_LINES_PER_ORDER,
  MIN_LINES_PER_ORDER,
  PROMOTION_BUNDLE_BUG_RATE,
  PROMOTION_SIZE_CODE_BUG_RATE,
  STEADY_MAX_ORDERS,
} from "../src/tuning.ts";

/** The most order ids that a search for a drawn fault tries. */
const SEARCH_BOUND = 10_000;

/** The seed catalog as the store lists it. */
function seedEntries(): readonly CatalogEntry[] {
  const seed = catalogSeed();

  return seed.products.map((entry, index) => {
    const stock = seed.stock[index];

    assert.ok(stock !== undefined);

    return { product: entry, stock };
  });
}

/** The first order id `ACME-<n>` that `wanted` takes. */
function orderIdWhere(wanted: (orderId: string) => boolean): string {
  for (let index = 1; index <= SEARCH_BOUND; index += 1) {
    const orderId = orderIdOf(index);

    if (wanted(orderId)) {
      return orderId;
    }
  }

  return assert.fail(`no order id within ${String(SEARCH_BOUND)}`);
}

describe("the seed catalog", () => {
  test("it has CATALOG_SIZE products; the last DISCONTINUED_SKU_COUNT have no stock", () => {
    const seed = catalogSeed();

    assert.equal(seed.products.length, CATALOG_SIZE);
    assert.equal(new Set(seed.products.map((entry) => entry.sku)).size, CATALOG_SIZE);
    assert.equal(seed.stock.filter((level) => level.onHand === 0).length, DISCONTINUED_SKU_COUNT);
    assert.ok(seed.stock.slice(0, CATALOG_SIZE - DISCONTINUED_SKU_COUNT).every((level) => level.onHand === CATALOG_STOCK_PER_SKU));
  });

  test("a product is the same for the same position", () => {
    assert.deepEqual(product(7), product(7));
    assert.equal(product(1).sku, skuOf(1));
    assert.equal(skuOf(1), "ACME-SKU-0001");
  });
});

describe("the order of an order id", () => {
  test("the same id gives the same lines, customer and total", () => {
    const first = buildOrder("ACME-00042", seedEntries());
    const second = buildOrder("ACME-00042", seedEntries());

    assert.ok(first.ok && second.ok);
    assert.deepEqual(first.value.lines, second.value.lines);
    assert.equal(first.value.customerId, second.value.customerId);
    assert.equal(first.value.totalCents, second.value.totalCents);
  });

  test("its lines are in bounds, and the total is the sum of the lines", () => {
    for (let index = 1; index <= 50; index += 1) {
      const order = buildOrder(orderIdOf(index), seedEntries());

      assert.ok(order.ok);
      assert.ok(order.value.lines.length >= MIN_LINES_PER_ORDER && order.value.lines.length <= MAX_LINES_PER_ORDER);
      assert.equal(
        order.value.totalCents,
        order.value.lines.reduce((total, line) => total + line.quantity * line.unitPriceCents, 0),
      );
      assert.equal(order.value.status, "placed");
    }
  });

  test("a drawn shortfall puts a discontinued SKU on the first line", () => {
    const orderId = orderIdWhere(stockFallsShort);
    const order = buildOrder(orderId, seedEntries());
    const discontinued = new Set(seedEntries().filter((entry) => entry.stock.onHand === 0).map((entry) => entry.product.sku));

    assert.ok(order.ok);
    assert.ok(discontinued.has(order.value.lines[0]?.sku ?? ""));
  });

  test("a drawn bundle fault gives clearance lines of the bundle quantity", () => {
    const orderId = orderIdWhere((id) => draw(PROMOTION_BUNDLE_BUG_RATE, [id, "bundle-bug"]));
    const order = buildOrder(orderId, seedEntries());

    assert.ok(order.ok);
    assert.ok(order.value.lines.every((line) => line.quantity === BUNDLE_MIN_QUANTITY && line.unitPriceCents === CLEARANCE_PRICE_CENTS));
  });

  test("a drawn size fault writes the corrupt size code on the first line", () => {
    const orderId = orderIdWhere((id) => draw(PROMOTION_SIZE_CODE_BUG_RATE, [id, "size-bug"]));
    const order = buildOrder(orderId, seedEntries());

    assert.ok(order.ok);
    assert.equal(order.value.lines[0]?.sizeCode, CORRUPT_SIZE_CODE);
  });

  test("a catalog with no sellable stock gives no order", () => {
    const empty = seedEntries().map((entry) => ({ ...entry, stock: { ...entry.stock, onHand: 0 } }));

    assert.ok(empty.every((entry) => availableUnits(entry.stock) === 0));
    assert.deepEqual(buildOrder("ACME-00001", empty), { ok: false, error: "the catalog has no sellable stock" });
  });
});

describe("scenario names", () => {
  test("each scenario name reads as itself; a command of cli.run is no scenario", () => {
    for (const name of SCENARIO_NAMES) {
      assert.deepEqual(scenarioNameOf(name), { kind: "scenario", name });
    }

    assert.deepEqual(scenarioNameOf("worker"), { kind: "notScenario" });
  });
});

describe("the arguments of steady", () => {
  test("no argument: orders until stopped, no error cover, pace 1", () => {
    assert.deepEqual(parseSteadyArgs([]), { ok: true, value: { orders: { kind: "untilStopped" }, errors: "noCover", pace: 1 } });
  });

  test("--orders, --cover-errors and --pace", () => {
    assert.deepEqual(parseSteadyArgs(["--orders", "3", "--cover-errors", "--pace=2.5"]), {
      ok: true,
      value: { orders: { kind: "bounded", orders: 3 }, errors: "coverErrors", pace: 2.5 },
    });
  });

  test("a count or a pace out of its bounds, an unknown flag and a positional argument are usage errors", () => {
    for (const args of [
      ["--orders", "0"],
      ["--orders", String(STEADY_MAX_ORDERS + 1)],
      ["--orders", "2.5"],
      ["--pace", "0.5"],
      ["--pace", "fast"],
      ["--loud"],
      ["ten"],
    ]) {
      assert.equal(parseSteadyArgs(args).ok, false, args.join(" "));
    }
  });
});

describe("runs with no database", () => {
  test("simulate prints its samples and exits 0", async () => {
    assert.equal(await runScenario("simulate", []), 0);
  });

  test("a scenario that takes no arguments exits with the usage code for one", async () => {
    assert.equal(await runScenario("seed", ["--now"]), SCENARIO_USAGE);
  });
});
