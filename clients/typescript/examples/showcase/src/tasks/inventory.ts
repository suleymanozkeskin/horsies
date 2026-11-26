// The implementations of the stock tasks: reservations, releases, the nightly stocktake, and the supplier feeds of a
// restock.

import type { BodyResult, Horsies, TaskOutcome } from "horsies";

import {
  releaseStock,
  replenishCatalog,
  reserveStock,
  syncSupplierFeed,
  updateStockLevels,
} from "../contracts/inventory.ts";
import type { StockRelease, StockReservation, StocktakeSummary, StockUpdate, SupplierFeed } from "../domain.ts";
import { draw, integer } from "../simulate.ts";
import { nightlyStocktake, releaseUnits } from "../store/catalog.ts";
import { type Store, storeFailureText } from "../store/connection.ts";
import { failOrderAndReleaseReservations, reserveLine } from "../store/orders.ts";
import { RESTOCK_MIN_SUCCESSFUL_FEEDS, RESTOCK_SKUS_PER_SUPPLIER, STOCKTAKE_CEILING_UNITS, SUPPLIER_TIMEOUT_RATE } from "../tuning.ts";
import { type ResultsOf, storeFailure, type StoreKey } from "./support.ts";

/** The attempts of a drawn supplier that time out before the supplier answers. */
const SUPPLIER_FAILING_ATTEMPTS = 1;

/** The fewest and the most SKUs that one supplier feed changes. */
const MIN_FEED_CHANGES = 0;
const MAX_FEED_CHANGES = 5;

type ReserveCode = "ORDER_NOT_FOUND" | "ORDER_CLOSED" | "UNKNOWN_SKU" | "INSUFFICIENT_STOCK" | "STORE_UNAVAILABLE";

type ReserveResult = BodyResult<typeof StockReservation, ReserveCode>;

type ReleaseResult = BodyResult<typeof StockRelease, "UNKNOWN_SKU" | "STORE_UNAVAILABLE">;

type ReplenishResult = BodyResult<typeof StocktakeSummary, "STORE_UNAVAILABLE">;

type FeedResult = BodyResult<typeof SupplierFeed, "SUPPLIER_TIMEOUT">;

type StockUpdateResult = BodyResult<typeof StockUpdate, "QUORUM_NOT_MET">;

/** The outcome of one supplier feed node, or `undefined` when the node had not ended under the quorum join. */
export type FeedOutcome = TaskOutcome<SupplierFeed> | undefined;

/** One reservation request: line `lineNo` of `orderId` takes `quantity` units of `sku`. */
export type ReservationRequest = {
  readonly orderId: string;
  readonly lineNo: number;
  readonly sku: string;
  readonly quantity: number;
};

/**
 * Fail `orderId` and release its reservations: the compensation of a refused reservation or a declined card. A store
 * failure goes to stderr with the order id; the nightly stocktake clears what stays reserved.
 */
export async function compensateFailedOrder(store: Store, orderId: string): Promise<void> {
  const released = await failOrderAndReleaseReservations(store, orderId);

  if (!released.ok) {
    process.stderr.write(`reservation compensation failed for ${orderId}: ${storeFailureText(released.error)}\n`);
  }
}

/**
 * Reserve the units of one order line. A refused reservation of an open order fails the order and releases its other
 * reservations. A line that an earlier run reserved is a replay.
 */
export async function reserve(
  store: Store,
  request: ReservationRequest,
  { ok, err }: ResultsOf<typeof reserveStock>,
): Promise<ReserveResult> {
  const { orderId, lineNo, sku, quantity } = request;
  const reserved = await reserveLine(store, orderId, lineNo, sku, quantity);

  if (!reserved.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(reserved.error) });
  }

  const outcome = reserved.value;

  switch (outcome.kind) {
    case "orderNotFound":
      return err({ code: "ORDER_NOT_FOUND", message: `no order ${orderId}` });
    case "orderClosed":
      return err({ code: "ORDER_CLOSED", message: `order ${orderId} no longer accepts reservations (${outcome.status})` });
    case "lineNotFound":
      await compensateFailedOrder(store, orderId);

      return err({ code: "UNKNOWN_SKU", message: `order ${orderId} has no line ${String(lineNo)}` });
    case "unknownSku":
      await compensateFailedOrder(store, orderId);

      return err({ code: "UNKNOWN_SKU", message: `${sku} is not in the catalog` });
    case "insufficientStock":
      await compensateFailedOrder(store, orderId);

      return err({
        code: "INSUFFICIENT_STOCK",
        message: `${sku} has ${String(outcome.available)} available, needs ${String(quantity)}`,
        data: { sku, available: outcome.available },
      });
    case "reserved":
      return ok({ orderId, sku, quantity, availableAfter: outcome.availableAfter, replayed: false });
    case "alreadyReserved":
      return ok({ orderId, sku, quantity, availableAfter: outcome.available, replayed: true });
    default: {
      const unexpected: never = outcome;

      throw new TypeError(`reserve: a reservation outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** Put `quantity` reserved units of `sku` back on the shelf. `UNKNOWN_SKU`; `STORE_UNAVAILABLE`. */
export async function release(
  store: Store,
  sku: string,
  quantity: number,
  { ok, err }: ResultsOf<typeof releaseStock>,
): Promise<ReleaseResult> {
  const released = await releaseUnits(store, sku, quantity);

  if (!released.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(released.error) });
  }

  if (released.value.kind === "notFound") {
    return err({ code: "UNKNOWN_SKU", message: `${sku} is not in the catalog` });
  }

  return ok({ sku, quantity, availableAfter: released.value.value });
}

/** Top each SKU up to `targetUnits` and clear every reservation. `STORE_UNAVAILABLE`. */
export async function replenish(
  store: Store,
  targetUnits: number,
  { ok, err }: ResultsOf<typeof replenishCatalog>,
): Promise<ReplenishResult> {
  const changed = await nightlyStocktake(store, targetUnits, STOCKTAKE_CEILING_UNITS);

  if (!changed.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(changed.error) });
  }

  return ok({
    skusToppedUp: changed.value.skusToppedUp,
    reservationsCleared: changed.value.reservationsCleared,
    targetUnits,
  });
}

/** The feed of `supplier` on attempt `attempt`. A drawn supplier times out on its first attempt. Pure. */
export function supplierFeed(supplier: string, attempt: number, { ok, err }: ResultsOf<typeof syncSupplierFeed>): FeedResult {
  if (draw(SUPPLIER_TIMEOUT_RATE, [supplier, "timeout"]) && attempt <= SUPPLIER_FAILING_ATTEMPTS) {
    return err({
      code: "SUPPLIER_TIMEOUT",
      message: `${supplier} did not answer in time on attempt ${String(attempt)}`,
    });
  }

  return ok({
    supplier,
    skuCount: RESTOCK_SKUS_PER_SUPPLIER,
    changedCount: integer(MIN_FEED_CHANGES, MAX_FEED_CHANGES, [supplier, "changed"]),
  });
}

/**
 * Apply the supplier feeds that succeeded. A feed that failed, or whose node had not ended, is skipped. Fewer
 * succeeded feeds than `RESTOCK_MIN_SUCCESSFUL_FEEDS` is `QUORUM_NOT_MET`. Pure.
 */
export function stockUpdate(feeds: readonly FeedOutcome[], { ok, err }: ResultsOf<typeof updateStockLevels>): StockUpdateResult {
  const applied: SupplierFeed[] = [];

  for (const feed of feeds) {
    if (feed?.kind === "succeeded") {
      applied.push(feed.value);
    }
  }

  const required = Math.min(RESTOCK_MIN_SUCCESSFUL_FEEDS, feeds.length);

  if (applied.length < required) {
    return err({
      code: "QUORUM_NOT_MET",
      message: `${String(applied.length)} supplier feeds reported, ${String(required)} needed`,
    });
  }

  return ok({
    suppliersApplied: applied.map((feed) => feed.supplier),
    applied: applied.reduce((units, feed) => units + feed.changedCount, 0),
    skipped: feeds.length - applied.length,
  });
}

/** Implement the stock tasks on `app`; the store-backed ones use the store of `store`. */
export function implementInventoryTasks(app: Horsies, store: StoreKey): void {
  app.implement(reserveStock, async (input, runtime) => reserve(runtime.state(store), input, runtime));
  app.implement(releaseStock, async ({ sku, quantity }, runtime) => release(runtime.state(store), sku, quantity, runtime));
  app.implement(replenishCatalog, async ({ targetUnits }, runtime) => replenish(runtime.state(store), targetUnits, runtime));
  app.implement(syncSupplierFeed, async ({ supplier }, runtime) => supplierFeed(supplier, runtime.fencingToken.value, runtime));
  app.implement(updateStockLevels, async ({ atlasFeed, breraFeed, coastlineFeed }, results) =>
    stockUpdate([atlasFeed, breraFeed, coastlineFeed], results),
  );
}
