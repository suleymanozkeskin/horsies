// The returns workflow: receive the item, inspect it, then restock it or write it off. Its result is the restock
// decision.
//
// A damaged item fails the inspection, which pauses the workflow (`onError: "pause"`). The settle node takes failed
// dependencies, so a resume of the paused workflow writes the item off.

import { defineWorkflow, h } from "horsies";

import { inspectItem, receiveReturn, restockOrWriteoff } from "../contracts/returns.ts";
import { RestockDecision } from "../domain.ts";

/** The check case of the returns workflow. */
export const RETURNS_CHECK_PARAMS = {
  returnId: "RET-CHECK",
  orderId: "ACME-CHECK-0001",
  sku: "ACME-SKU-0001",
  quantity: 1,
} as const;

export const returnsReview = defineWorkflow(
  {
    name: "returns_review",
    definitionKey: "acme.returns_review.v1",
    params: { returnId: h.string(), orderId: h.string(), sku: h.string(), quantity: h.int() },
    output: RestockDecision,
    onError: "pause",
    checkCases: [RETURNS_CHECK_PARAMS],
  },
  (w, { returnId, orderId, sku, quantity }) => {
    const receive = w.task(receiveReturn, { returnId, orderId, sku, quantity }, { nodeId: "receive_return" });
    const inspection = w.task(inspectItem, { returnId }, { nodeId: "inspect_item", waitsFor: [receive] });
    const settle = w.task(restockOrWriteoff, { returnId, inspection }, { nodeId: "restock_or_writeoff", allowFailedDependencies: true });

    return { output: settle };
  },
);
