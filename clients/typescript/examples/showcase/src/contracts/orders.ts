// The contracts of the order tasks. A contract holds what a sender needs; it imports no implementation and does no
// I/O.

import { defineTask, h } from "horsies";

import { Invoice, OrderValidation, PickPack, WarehouseAllocation } from "../domain.ts";
import { INVOICE_TIMEOUT_MS, SHARED_TASK_TIMEOUT_MS } from "../tuning.ts";
import { CRASH_RETRY, QUEUE_FULFILLMENT } from "./policies.ts";

/** Check that the order exists and has lines, and mark it validated. */
export const validateOrder = defineTask({
  name: "validate_order",
  queue: QUEUE_FULFILLMENT,
  input: { orderId: h.string() },
  output: OrderValidation,
  errorCodes: ["ORDER_NOT_FOUND", "STORE_UNAVAILABLE"],
  retryPolicy: CRASH_RETRY,
});

/** Take the reserved units of each line of the order off the shelf, and mark it packed. */
export const pickPack = defineTask({
  name: "pick_pack",
  queue: QUEUE_FULFILLMENT,
  input: { orderId: h.string() },
  output: PickPack,
  errorCodes: ["ORDER_NOT_FOUND", "STORE_UNAVAILABLE"],
  retryPolicy: CRASH_RETRY,
});

/** Choose the warehouse that a stock transfer takes its units from. */
export const allocateWarehouse = defineTask({
  name: "allocate_warehouse",
  queue: QUEUE_FULFILLMENT,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { sku: h.string(), quantity: h.int() },
  output: WarehouseAllocation,
  retryPolicy: CRASH_RETRY,
});

/** Render the invoice of the order. A run past the deadline ends as `TASK_TIMEOUT`. */
export const generateInvoice = defineTask({
  name: "generate_invoice",
  queue: QUEUE_FULFILLMENT,
  timeoutMs: INVOICE_TIMEOUT_MS,
  input: { orderId: h.string() },
  output: Invoice,
  errorCodes: ["ORDER_NOT_FOUND", "STORE_UNAVAILABLE"],
  retryPolicy: CRASH_RETRY,
});

export const ORDER_TASKS = [validateOrder, pickPack, allocateWarehouse, generateInvoice] as const;
