// The contracts of the notification tasks. Each one runs on a seat of the shared async child.

import { defineTask, h } from "horsies";

import { AbandonedCartSweep, EmailReceipt, MarketingBlast, PaymentCapture, ShippingNotice } from "../domain.ts";
import { SHARED_TASK_TIMEOUT_MS } from "../tuning.ts";
import { CRASH_RETRY, QUEUE_NOTIFICATIONS } from "./policies.ts";

/**
 * Email the customer the end of the order. The node takes failed dependencies: a failed capture sends the
 * "order not completed" template.
 */
export const sendOrderEmail = defineTask({
  name: "send_order_email",
  queue: QUEUE_NOTIFICATIONS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { orderId: h.string(), capture: h.resultOf(PaymentCapture) },
  output: EmailReceipt,
  errors: [],
  retry: CRASH_RETRY,
});

/** Text the customer the tracking code. */
export const sendShippingSms = defineTask({
  name: "send_shipping_sms",
  queue: QUEUE_NOTIFICATIONS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { orderId: h.string(), trackingCode: h.string() },
  output: ShippingNotice,
  errors: [],
  retry: CRASH_RETRY,
});

/** Send a marketing message to one segment. */
export const marketingBlast = defineTask({
  name: "marketing_blast",
  queue: QUEUE_NOTIFICATIONS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { segment: h.string() },
  output: MarketingBlast,
  errors: [],
  retry: CRASH_RETRY,
});

/** Send the win-back message to the customers that the abandoned cart sweep found. */
export const winbackBlast = defineTask({
  name: "winback_blast",
  queue: QUEUE_NOTIFICATIONS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { segment: h.string(), sweep: h.resultOf(AbandonedCartSweep) },
  output: MarketingBlast,
  errors: ["UPSTREAM_FAILED"],
  retry: CRASH_RETRY,
});

export const NOTIFY_TASKS = [sendOrderEmail, sendShippingSms, marketingBlast, winbackBlast] as const;
