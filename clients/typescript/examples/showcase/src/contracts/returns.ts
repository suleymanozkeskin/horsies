// The contracts of the return tasks: the steps of the returns review workflow.
//
// A damaged item is an error of the inspection, which pauses the returns review workflow. The settle node takes
// failed dependencies, so a resume of the workflow writes the item off.

import { defineTask, h } from "horsies";

import { Inspection, RestockDecision, ReturnReceipt } from "../domain.ts";
import { CRASH_RETRY, QUEUE_FULFILLMENT } from "./policies.ts";

/** Book the returned item in. A case that is booked already stays as it is. */
export const receiveReturn = defineTask({
  name: "receive_return",
  queue: QUEUE_FULFILLMENT,
  input: { returnId: h.string(), orderId: h.string(), sku: h.string(), quantity: h.int() },
  output: ReturnReceipt,
  errors: ["STORE_UNAVAILABLE"],
  retry: CRASH_RETRY,
});

/** Inspect the returned item. A drawn damage is `DAMAGED_ITEM`. */
export const inspectItem = defineTask({
  name: "inspect_item",
  queue: QUEUE_FULFILLMENT,
  input: { returnId: h.string() },
  output: Inspection,
  errors: ["RETURN_NOT_FOUND", "DAMAGED_ITEM", "STORE_UNAVAILABLE"],
  retry: CRASH_RETRY,
});

/** Put a resellable item back on the shelf, or write a damaged one off. */
export const restockOrWriteoff = defineTask({
  name: "restock_or_writeoff",
  queue: QUEUE_FULFILLMENT,
  input: { returnId: h.string(), inspection: h.taskOutcome(inspectItem) },
  output: RestockDecision,
  errors: ["RETURN_NOT_FOUND", "UNKNOWN_SKU", "STORE_UNAVAILABLE"],
  retry: CRASH_RETRY,
});

export const RETURN_TASKS = [receiveReturn, inspectItem, restockOrWriteoff] as const;
