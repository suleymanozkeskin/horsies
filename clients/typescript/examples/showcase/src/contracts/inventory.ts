// The contracts of the stock tasks.

import { defineTask, h } from "horsies";

import { StockRelease, StockReservation, StocktakeSummary, StockUpdate, SupplierFeed } from "../domain.ts";
import { SHARED_TASK_TIMEOUT_MS } from "../tuning.ts";
import { CRASH_RETRY, QUEUE_ANALYTICS, QUEUE_FULFILLMENT, SUPPLIER_RETRY } from "./policies.ts";

/**
 * Reserve the units of one order line. A refused reservation fails the order and releases its other reservations, so
 * the order workflow stops before the payment.
 */
export const reserveStock = defineTask({
  name: "reserve_stock",
  queue: QUEUE_FULFILLMENT,
  input: { orderId: h.string(), lineNo: h.int(), sku: h.string(), quantity: h.int() },
  output: StockReservation,
  errorCodes: ["ORDER_NOT_FOUND", "ORDER_CLOSED", "UNKNOWN_SKU", "INSUFFICIENT_STOCK", "STORE_UNAVAILABLE"],
  retryPolicy: CRASH_RETRY,
});

/** Put reserved units of a SKU back on the shelf. */
export const releaseStock = defineTask({
  name: "release_stock",
  queue: QUEUE_FULFILLMENT,
  input: { sku: h.string(), quantity: h.int() },
  output: StockRelease,
  errorCodes: ["UNKNOWN_SKU", "STORE_UNAVAILABLE"],
  retryPolicy: CRASH_RETRY,
});

/** The nightly stocktake: clear every reservation and top each SKU up to the target. */
export const replenishCatalog = defineTask({
  name: "replenish_catalog",
  queue: QUEUE_ANALYTICS,
  input: { targetUnits: h.int() },
  output: StocktakeSummary,
  errorCodes: ["STORE_UNAVAILABLE"],
  retryPolicy: CRASH_RETRY,
});

/** Read the feed of one supplier. The first attempt of a drawn supplier times out; the retry policy retries it. */
export const syncSupplierFeed = defineTask({
  name: "sync_supplier_feed",
  queue: QUEUE_ANALYTICS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { supplier: h.string() },
  output: SupplierFeed,
  errorCodes: ["SUPPLIER_TIMEOUT"],
  retryPolicy: SUPPLIER_RETRY,
});

/**
 * Apply the supplier feeds of a restock. Each feed parameter takes the outcome of one feed node; under the quorum join
 * of the restock workflow a feed node may not have ended, so each one is optional. Fewer reporting feeds than the
 * quorum is `QUORUM_NOT_MET`.
 */
export const updateStockLevels = defineTask({
  name: "update_stock_levels",
  queue: QUEUE_ANALYTICS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: {
    atlasFeed: h.taskOutcome(syncSupplierFeed).optional(),
    breraFeed: h.taskOutcome(syncSupplierFeed).optional(),
    coastlineFeed: h.taskOutcome(syncSupplierFeed).optional(),
  },
  output: StockUpdate,
  errorCodes: ["QUORUM_NOT_MET"],
  retryPolicy: CRASH_RETRY,
});

export const INVENTORY_TASKS = [reserveStock, releaseStock, replenishCatalog, syncSupplierFeed, updateStockLevels] as const;
