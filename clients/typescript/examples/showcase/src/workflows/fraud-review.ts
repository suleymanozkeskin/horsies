// The fraud review: reconcile the payments of the disputed order, then refund it. A failed node pauses the workflow
// (`onError: "pause"`) for an operator. Its result is the refund.

import { defineWorkflow } from "horsies";

import { reconcilePayments, refundPayment } from "../contracts/payments.ts";
import { PaymentRefund } from "../domain.ts";

/** The disputed order of the workflow. */
export const DISPUTED_ORDER_ID = "ACME-CHECK-0001";

/** The refunded amount, in cents. */
export const REFUND_AMOUNT_CENTS = 4_900;

/** When the customer opened the dispute. A build reads no clock, so the instant is a constant of the definition. */
export const DISPUTE_OPENED_AT: Temporal.Instant = Temporal.Instant.from("2026-01-15T09:00:00Z");

export const fraudReview = defineWorkflow(
  { name: "fraud_review", definitionKey: "acme.fraud_review.v1", params: {}, output: PaymentRefund, onError: "pause" },
  (w) => {
    const reconcile = w.task(reconcilePayments, { window: `dispute-${DISPUTED_ORDER_ID}` }, { id: "reconcile_payments" });

    const refund = w.task(
      refundPayment,
      { orderId: DISPUTED_ORDER_ID, amountCents: REFUND_AMOUNT_CENTS, disputedAt: DISPUTE_OPENED_AT },
      { id: "refund_payment", waitsFor: [reconcile] },
    );

    return { output: refund };
  },
);
