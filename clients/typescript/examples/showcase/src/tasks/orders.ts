// The implementations of the order tasks. The store logic of each store-backed task is a function of its own, so a
// test runs it with a store of the test.

import type { BodyResult, Horsies } from "horsies";

import { allocateWarehouse, generateInvoice, pickPack, validateOrder } from "../contracts/orders.ts";
import type { Invoice, OrderValidation, PickPack, WarehouseAllocation } from "../domain.ts";
import { choice, integer } from "../simulate.ts";
import type { Store } from "../store/connection.ts";
import { consumeLine, getOrder, setOrderStatus } from "../store/orders.ts";
import { PICK_STATIONS, WAREHOUSES } from "../tuning.ts";
import { type ResultsOf, storeFailure, type StoreKey } from "./support.ts";

/** The shortest and the longest distance of a drawn warehouse, in km. */
const MIN_WAREHOUSE_DISTANCE_KM = 20;
const MAX_WAREHOUSE_DISTANCE_KM = 1_400;

type ValidateResult = BodyResult<typeof OrderValidation, "ORDER_NOT_FOUND" | "STORE_UNAVAILABLE">;

type PickPackResult = BodyResult<typeof PickPack, "ORDER_NOT_FOUND" | "STORE_UNAVAILABLE">;

type InvoiceResult = BodyResult<typeof Invoice, "ORDER_NOT_FOUND" | "STORE_UNAVAILABLE">;

type AllocationResult = BodyResult<typeof WarehouseAllocation, never>;

/**
 * Mark the order `orderId` validated. `ORDER_NOT_FOUND` for an unknown order or an order with no lines;
 * `STORE_UNAVAILABLE`.
 */
export async function validate(store: Store, orderId: string, { ok, err }: ResultsOf<typeof validateOrder>): Promise<ValidateResult> {
  const read = await getOrder(store, orderId);

  if (!read.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(read.error) });
  }

  if (read.value.kind === "notFound") {
    return err({ code: "ORDER_NOT_FOUND", message: `no order ${orderId}` });
  }

  const order = read.value.value;

  if (order.lines.length === 0) {
    return err({ code: "ORDER_NOT_FOUND", message: `order ${orderId} has no lines` });
  }

  const marked = await setOrderStatus(store, orderId, "validated");

  if (!marked.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(marked.error) });
  }

  return ok({ orderId, lineCount: order.lines.length, totalCents: order.totalCents });
}

/**
 * Take the reserved units of each line of `orderId` off the shelf and mark the order packed. A line that an earlier
 * run consumed changes nothing. `ORDER_NOT_FOUND`; `STORE_UNAVAILABLE`.
 */
export async function pickAndPack(store: Store, orderId: string, { ok, err }: ResultsOf<typeof pickPack>): Promise<PickPackResult> {
  const read = await getOrder(store, orderId);

  if (!read.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(read.error) });
  }

  if (read.value.kind === "notFound") {
    return err({ code: "ORDER_NOT_FOUND", message: `no order ${orderId}` });
  }

  const order = read.value.value;

  for (const line of order.lines) {
    const consumed = await consumeLine(store, orderId, line.lineNo, line.sku, line.quantity);

    if (!consumed.ok) {
      return err({ code: "STORE_UNAVAILABLE", ...storeFailure(consumed.error) });
    }
  }

  const marked = await setOrderStatus(store, orderId, "packed");

  if (!marked.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(marked.error) });
  }

  return ok({
    orderId,
    station: choice(PICK_STATIONS, [orderId, "station"]),
    unitsPicked: order.lines.reduce((units, line) => units + line.quantity, 0),
  });
}

/** The invoice of `orderId`. `ORDER_NOT_FOUND`; `STORE_UNAVAILABLE`. */
export async function invoice(store: Store, orderId: string, { ok, err }: ResultsOf<typeof generateInvoice>): Promise<InvoiceResult> {
  const read = await getOrder(store, orderId);

  if (!read.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(read.error) });
  }

  if (read.value.kind === "notFound") {
    return err({ code: "ORDER_NOT_FOUND", message: `no order ${orderId}` });
  }

  return ok({ orderId, invoiceNumber: `INV-${orderId}`, totalCents: read.value.value.totalCents });
}

/** The warehouse that a transfer of `quantity` units of `sku` takes its units from: a draw over the SKU. Pure. */
export function allocate(sku: string, quantity: number, { ok }: ResultsOf<typeof allocateWarehouse>): AllocationResult {
  return ok({
    orderId: `transfer:${sku}:${String(quantity)}`,
    warehouseCode: choice(WAREHOUSES, [sku, "warehouse"]),
    distanceKm: integer(MIN_WAREHOUSE_DISTANCE_KM, MAX_WAREHOUSE_DISTANCE_KM, [sku, "distance"]),
  });
}

/** Implement the order tasks on `app`; the store-backed ones use the store of `store`. */
export function implementOrderTasks(app: Horsies, store: StoreKey): void {
  app.implement(validateOrder, async ({ orderId }, runtime) => validate(runtime.state(store), orderId, runtime));
  app.implement(pickPack, async ({ orderId }, runtime) => pickAndPack(runtime.state(store), orderId, runtime));
  app.implement(generateInvoice, async ({ orderId }, runtime) => invoice(runtime.state(store), orderId, runtime));
  app.implement(allocateWarehouse, async ({ sku, quantity }, results) => allocate(sku, quantity, results));
}
