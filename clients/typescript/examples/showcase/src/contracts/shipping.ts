// The contracts of the shipping tasks: the steps of the shipping workflow.

import { defineTask, h } from "horsies";

import { CourierBooking, ShippingLabel, TrackingSeed } from "../domain.ts";
import { COURIER_RETRY, CRASH_RETRY, QUEUE_FULFILLMENT } from "./policies.ts";

/** Book a courier for the order. The first attempt of a drawn courier flake fails; the retry policy retries it. */
export const bookCourier = defineTask({
  name: "book_courier",
  queue: QUEUE_FULFILLMENT,
  input: { orderId: h.string(), courier: h.string(), express: h.boolean() },
  output: CourierBooking,
  errorCodes: ["COURIER_UNAVAILABLE", "SHIPMENT_NOT_FOUND", "STORE_UNAVAILABLE"],
  retryPolicy: COURIER_RETRY,
});

/** Print the label of the booked shipment. */
export const printLabel = defineTask({
  name: "print_label",
  queue: QUEUE_FULFILLMENT,
  input: { orderId: h.string(), booking: h.taskOutcome(bookCourier) },
  output: ShippingLabel,
  errorCodes: ["UPSTREAM_FAILED", "SHIPMENT_NOT_FOUND", "STORE_UNAVAILABLE"],
  retryPolicy: CRASH_RETRY,
});

/** Seed the tracking code of the shipment and mark the order shipped. */
export const trackingSeed = defineTask({
  name: "tracking_seed",
  queue: QUEUE_FULFILLMENT,
  input: { orderId: h.string(), label: h.taskOutcome(printLabel) },
  output: TrackingSeed,
  errorCodes: ["UPSTREAM_FAILED", "SHIPMENT_NOT_FOUND", "STORE_UNAVAILABLE"],
  retryPolicy: CRASH_RETRY,
});

export const SHIPPING_TASKS = [bookCourier, printLabel, trackingSeed] as const;
