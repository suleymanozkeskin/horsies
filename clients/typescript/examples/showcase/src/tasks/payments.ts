// The implementations of the payment tasks. A hold and a capture are each at most one row per order, so a second run
// of a task never charges twice.

import { randomUUID } from "node:crypto";

import type { BodyResult, Horsies, TaskOutcome } from "horsies";

import { authorizePayment, capturePayment, reconcilePayments, refundPayment } from "../contracts/payments.ts";
import type { PaymentAuthorization, PaymentCapture, PaymentRefund, ReconciliationReport } from "../domain.ts";
import { upstreamFailure } from "../outcome.ts";
import { draw, integer } from "../simulate.ts";
import type { Store } from "../store/connection.ts";
import { countAuthorizationAttempt, setOrderStatus } from "../store/orders.ts";
import { findPayment, recordPayment } from "../store/payments.ts";
import { CARD_DECLINE_RATE, PSP_FAILING_ATTEMPTS, PSP_UNAVAILABLE_RATE } from "../tuning.ts";
import { compensateFailedOrder } from "./inventory.ts";
import { storeFailure, type StoreKey } from "./support.ts";

/** The fewest and the most authorizations of one reconciliation window. */
const MIN_WINDOW_AUTHORIZATIONS = 10;
const MAX_WINDOW_AUTHORIZATIONS = 400;

/** The most authorizations of one window that have no capture. */
const MAX_UNMATCHED = 3;

type AuthorizeCode = "ORDER_NOT_FOUND" | "CARD_DECLINED" | "PSP_UNAVAILABLE" | "STORE_UNAVAILABLE";

type AuthorizeResult = BodyResult<typeof PaymentAuthorization, AuthorizeCode>;

type CaptureResult = BodyResult<typeof PaymentCapture, "UPSTREAM_FAILED" | "PAYMENT_ALREADY_CAPTURED" | "STORE_UNAVAILABLE">;

type RefundResult = BodyResult<typeof PaymentRefund, never>;

type ReconcileResult = BodyResult<typeof ReconciliationReport, never>;

/** A new reference of the payment provider. */
function pspReference(): string {
  return `psp_${randomUUID().replaceAll("-", "")}`;
}

/**
 * Hold `amountCents` on the card of `orderId`. Each call counts one authorization attempt of the order. A hold that
 * exists is a replay. `CARD_DECLINED` (the order fails and releases its reservations); `PSP_UNAVAILABLE` on the first
 * attempts of a drawn outage, which the retry policy retries; `ORDER_NOT_FOUND`; `STORE_UNAVAILABLE`.
 */
export async function authorize(store: Store, orderId: string, amountCents: number): Promise<AuthorizeResult> {
  const counted = await countAuthorizationAttempt(store, orderId);

  if (!counted.ok) {
    return authorizePayment.err({ code: "STORE_UNAVAILABLE", ...storeFailure(counted.error) });
  }

  if (counted.value.kind === "notFound") {
    return authorizePayment.err({ code: "ORDER_NOT_FOUND", message: `no order ${orderId}` });
  }

  const attempt = counted.value.value;
  const existing = await findPayment(store, orderId, "authorization");

  if (!existing.ok) {
    return authorizePayment.err({ code: "STORE_UNAVAILABLE", ...storeFailure(existing.error) });
  }

  if (existing.value.kind === "found") {
    const hold = existing.value.value;

    return authorizePayment.ok({
      orderId,
      authorizationId: hold.paymentId,
      amountCents: hold.amountCents,
      pspReference: hold.pspReference,
      attempt,
      replayed: true,
    });
  }

  if (draw(CARD_DECLINE_RATE, [orderId, "card"])) {
    await compensateFailedOrder(store, orderId);

    return authorizePayment.err({ code: "CARD_DECLINED", message: "issuer declined the card" });
  }

  if (draw(PSP_UNAVAILABLE_RATE, [orderId, "psp"]) && attempt <= PSP_FAILING_ATTEMPTS) {
    return authorizePayment.err({ code: "PSP_UNAVAILABLE", message: `provider unavailable on attempt ${String(attempt)}` });
  }

  return recordHold(store, orderId, amountCents, attempt);
}

async function recordHold(store: Store, orderId: string, amountCents: number, attempt: number): Promise<AuthorizeResult> {
  const recorded = await recordPayment(store, orderId, "authorization", amountCents, pspReference());

  if (!recorded.ok) {
    return authorizePayment.err({ code: "STORE_UNAVAILABLE", ...storeFailure(recorded.error) });
  }

  switch (recorded.value.kind) {
    case "exists":
      return authorizePayment.err({ code: "PSP_UNAVAILABLE", message: "another attempt recorded the authorization first" });
    case "recorded": {
      const hold = recorded.value.payment;

      return authorizePayment.ok({
        orderId,
        authorizationId: hold.paymentId,
        amountCents: hold.amountCents,
        pspReference: hold.pspReference,
        attempt,
        replayed: false,
      });
    }

    default: {
      const unexpected: never = recorded.value;

      throw new TypeError(`recordHold: a payment record outside its type: ${String(unexpected)}`);
    }
  }
}

/** Settle `hold`, the authorization of `orderId`, and mark the order captured. `PAYMENT_ALREADY_CAPTURED`; `STORE_UNAVAILABLE`. */
export async function capture(store: Store, orderId: string, hold: PaymentAuthorization): Promise<CaptureResult> {
  const recorded = await recordPayment(store, orderId, "capture", hold.amountCents, hold.pspReference);

  if (!recorded.ok) {
    return capturePayment.err({ code: "STORE_UNAVAILABLE", ...storeFailure(recorded.error) });
  }

  if (recorded.value.kind === "exists") {
    return capturePayment.err({ code: "PAYMENT_ALREADY_CAPTURED", message: `order ${orderId} is already captured` });
  }

  const marked = await setOrderStatus(store, orderId, "captured");

  if (!marked.ok) {
    return capturePayment.err({ code: "STORE_UNAVAILABLE", ...storeFailure(marked.error) });
  }

  return capturePayment.ok({
    orderId,
    captureId: recorded.value.payment.paymentId,
    authorizationId: hold.authorizationId,
    amountCents: recorded.value.payment.amountCents,
    replayed: false,
  });
}

/** The capture of the outcome of the authorization node: `UPSTREAM_FAILED` when the authorization failed. */
export async function captureAfter(
  store: Store,
  orderId: string,
  authorization: TaskOutcome<PaymentAuthorization>,
): Promise<CaptureResult> {
  switch (authorization.kind) {
    case "succeeded":
      return capture(store, orderId, authorization.value);
    case "failed":
      return capturePayment.err({ code: "UPSTREAM_FAILED", ...upstreamFailure(authorization.error) });
    default: {
      const unexpected: never = authorization;

      throw new TypeError(`captureAfter: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** The refund of the order `orderId`, disputed at `disputedAt`. Pure. */
export function refund(orderId: string, amountCents: number, disputedAt: Temporal.Instant): RefundResult {
  return refundPayment.ok({ orderId, refundId: `rfd_${orderId}`, amountCents, disputedAt });
}

/** The reconciliation report of `window`: draws over the window. Pure. */
export function reconcile(window: string): ReconcileResult {
  const authorizations = integer(MIN_WINDOW_AUTHORIZATIONS, MAX_WINDOW_AUTHORIZATIONS, [window, "authorizations"]);
  const unmatched = integer(0, MAX_UNMATCHED, [window, "unmatched"]);

  return reconcilePayments.ok({ authorizations, captures: authorizations - unmatched, unmatched });
}

/** Implement the payment tasks on `app`; the store-backed ones use the store of `store`. */
export function implementPaymentTasks(app: Horsies, store: StoreKey): void {
  app.implement(authorizePayment, async ({ orderId, amountCents }, runtime) =>
    authorize(runtime.state(store), orderId, amountCents),
  );
  app.implement(capturePayment, async ({ orderId, authorization }, runtime) =>
    captureAfter(runtime.state(store), orderId, authorization),
  );
  app.implement(refundPayment, async ({ orderId, amountCents, disputedAt }) => refund(orderId, amountCents, disputedAt));
  app.implement(reconcilePayments, async ({ window }) => reconcile(window));
}
