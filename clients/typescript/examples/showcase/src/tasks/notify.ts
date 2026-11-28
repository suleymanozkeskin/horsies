// The implementations of the notification tasks. Each one runs on a seat of the shared async child. No message leaves
// the demo: the recipient is an address of the `.invalid` domain.

import type { BodyResult, Horsies, TaskOutcome } from "horsies";

import { marketingBlast, sendOrderEmail, sendShippingSms, winbackBlast } from "../contracts/notify.ts";
import { type AbandonedCartSweep, type EmailReceipt, MarketingBlast, type PaymentCapture, type ShippingNotice } from "../domain.ts";
import { upstreamFailure } from "../outcome.ts";
import { integer } from "../simulate.ts";
import { MARKETING_SEGMENT_SIZE } from "../tuning.ts";
import type { ResultsOf } from "./support.ts";

/** The recipient of every message of the demo. */
export const DEMO_RECIPIENT = "customer@example.invalid";

/** The email template of an order whose capture succeeded. */
export const CONFIRMATION_TEMPLATE = "order-confirmation";

/** The email template of an order whose capture failed or did not run. */
export const NOT_COMPLETED_TEMPLATE = "order-not-completed";

/** The result of the win-back body. */
export type WinbackResult = BodyResult<typeof MarketingBlast, "UPSTREAM_FAILED">;

/** The email of the end of `orderId`: the template follows the outcome of its capture. Pure. */
export function orderEmail(orderId: string, capture: TaskOutcome<PaymentCapture>): EmailReceipt {
  switch (capture.kind) {
    case "succeeded":
      return { orderId, template: CONFIRMATION_TEMPLATE, recipient: DEMO_RECIPIENT };
    case "failed":
      return { orderId, template: NOT_COMPLETED_TEMPLATE, recipient: DEMO_RECIPIENT };
    default: {
      const unexpected: never = capture;

      throw new TypeError(`orderEmail: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** The text message of the tracking code of `orderId`. Pure. */
export function shippingNotice(orderId: string, trackingCode: string): ShippingNotice {
  return { orderId, recipient: DEMO_RECIPIENT, trackingCode };
}

/** The recipients of `segment`: from 1 to `MARKETING_SEGMENT_SIZE`. Pure. */
export function segmentSize(segment: string): number {
  return integer(1, MARKETING_SEGMENT_SIZE, [segment, "recipients"]);
}

/** The marketing message of `segment`. Pure. */
export function segmentBlast(segment: string): MarketingBlast {
  return { segment, recipients: segmentSize(segment) };
}

/**
 * The win-back message of `segment` to the customers of `sweep`: at most the segment's size. A failed sweep is
 * `UPSTREAM_FAILED`. Pure.
 */
export function winbackMessage(
  segment: string,
  sweep: TaskOutcome<AbandonedCartSweep>,
  { ok, err }: ResultsOf<typeof winbackBlast>,
): WinbackResult {
  switch (sweep.kind) {
    case "succeeded":
      return ok({ segment, recipients: Math.min(sweep.value.swept, segmentSize(segment)) });
    case "failed":
      return err({ code: "UPSTREAM_FAILED", ...upstreamFailure(sweep.error) });
    default: {
      const unexpected: never = sweep;

      throw new TypeError(`winbackMessage: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** Implement the notification tasks on `app`. */
export function implementNotifyTasks(app: Horsies): void {
  app.implement(sendOrderEmail, async ({ orderId, capture }, { ok }) => ok(orderEmail(orderId, capture)));
  app.implement(sendShippingSms, async ({ orderId, trackingCode }, { ok }) => ok(shippingNotice(orderId, trackingCode)));
  app.implement(marketingBlast, async ({ segment }, { ok }) => ok(segmentBlast(segment)));
  app.implement(winbackBlast, async ({ segment, sweep }, results) => winbackMessage(segment, sweep, results));
}
