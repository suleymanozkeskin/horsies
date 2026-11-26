// The contracts of the payment tasks.
//
// `PSP_UNAVAILABLE` is operational: the provider does not answer, and the retry policy backs off. `CARD_DECLINED` is a
// fact of the domain: no retry, and the draw is a hash of the order id, so a rerun of a declined payment declines
// again.

import { defineTask, h } from "horsies";
import { hz } from "horsies/zod";
import { z } from "zod";

import { PaymentAuthorization, PaymentCapture, PaymentRefund, ReconciliationReport } from "../domain.ts";
import { SHARED_TASK_TIMEOUT_MS } from "../tuning.ts";
import { CRASH_RETRY, PSP_RETRY, QUEUE_PAYMENTS } from "./policies.ts";

/** Hold the order total on the customer's card. An existing hold is returned as a replay: no second charge. */
export const authorizePayment = defineTask({
  name: "authorize_payment",
  queue: QUEUE_PAYMENTS,
  input: { orderId: h.string(), amountCents: h.int() },
  output: PaymentAuthorization,
  errors: ["ORDER_NOT_FOUND", "CARD_DECLINED", "PSP_UNAVAILABLE", "STORE_UNAVAILABLE"],
  retry: PSP_RETRY,
});

/** Settle the hold that the authorization node gave. */
export const capturePayment = defineTask({
  name: "capture_payment",
  queue: QUEUE_PAYMENTS,
  input: { orderId: h.string(), authorization: h.resultOf(PaymentAuthorization) },
  output: PaymentCapture,
  errors: ["UPSTREAM_FAILED", "PAYMENT_ALREADY_CAPTURED", "STORE_UNAVAILABLE"],
  retry: CRASH_RETRY,
});

/**
 * Refund a disputed order. The parameters are zod schemas, as in an app whose models are in zod: a send checks the
 * refinements (an empty order id or an amount below 1 cent is `VALIDATION_FAILED`, and nothing is written), and the
 * instant of the dispute is stored with the `hz.instant()` codec of `horsies/zod`.
 */
export const refundPayment = defineTask({
  name: "refund_payment",
  queue: QUEUE_PAYMENTS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { orderId: z.string().min(1), amountCents: z.int().positive(), disputedAt: hz.instant() },
  output: PaymentRefund,
  errors: [],
  retry: CRASH_RETRY,
});

/** Compare the authorizations and the captures of one reconciliation window. */
export const reconcilePayments = defineTask({
  name: "reconcile_payments",
  queue: QUEUE_PAYMENTS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { window: h.string() },
  output: ReconciliationReport,
  errors: [],
  retry: CRASH_RETRY,
});

export const PAYMENT_TASKS = [authorizePayment, capturePayment, refundPayment, reconcilePayments] as const;
