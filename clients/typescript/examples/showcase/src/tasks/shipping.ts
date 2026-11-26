// The implementations of the shipping tasks: the booking, the label and the tracking code of an order's shipment.

import { randomUUID } from "node:crypto";

import type { BodyResult, Horsies, TaskOutcome } from "horsies";

import { bookCourier, printLabel, trackingSeed } from "../contracts/shipping.ts";
import type { CourierBooking, ShippingLabel, TrackingSeed } from "../domain.ts";
import { upstreamFailure } from "../outcome.ts";
import { draw, integer } from "../simulate.ts";
import type { Store } from "../store/connection.ts";
import { setOrderStatus } from "../store/orders.ts";
import { countCourierAttempt, getShipment, setBookingReference, setLabelUrl, setTrackingCode } from "../store/shipments.ts";
import { COURIER_FAILING_ATTEMPTS, COURIER_FLAKE_RATE } from "../tuning.ts";
import { storeFailure, type StoreKey } from "./support.ts";

/** The letters of the courier name that start a booking reference. */
const BOOKING_PREFIX_LETTERS = 3;

/** The hex digits of the random part of a booking reference. */
const BOOKING_RANDOM_DIGITS = 10;

/** The lowest and the highest number of a tracking code. */
const MIN_TRACKING_NUMBER = 10_000_000;
const MAX_TRACKING_NUMBER = 99_999_999;

type ShippingCode = "SHIPMENT_NOT_FOUND" | "STORE_UNAVAILABLE";

type BookResult = BodyResult<typeof CourierBooking, "COURIER_UNAVAILABLE" | ShippingCode>;

type LabelResult = BodyResult<typeof ShippingLabel, "UPSTREAM_FAILED" | ShippingCode>;

type TrackResult = BodyResult<typeof TrackingSeed, "UPSTREAM_FAILED" | ShippingCode>;

/** One booking request: `courier` takes the shipment of `orderId`. */
export type BookingRequest = { readonly orderId: string; readonly courier: string; readonly express: boolean };

/** A new booking reference of `courier`, for example `FLE-3f2a9c01b7`. */
function bookingReference(courier: string): string {
  const prefix = courier.slice(0, BOOKING_PREFIX_LETTERS).toUpperCase();

  return `${prefix}-${randomUUID().replaceAll("-", "").slice(0, BOOKING_RANDOM_DIGITS)}`;
}

/**
 * Book the courier of the request. Each call counts one attempt; a booking that exists is a replay.
 * `COURIER_UNAVAILABLE` on the first attempts of a drawn flake, which the retry policy retries; `SHIPMENT_NOT_FOUND`;
 * `STORE_UNAVAILABLE`.
 */
export async function book(store: Store, request: BookingRequest): Promise<BookResult> {
  const { orderId, courier, express } = request;
  const counted = await countCourierAttempt(store, orderId, courier, express);

  if (!counted.ok) {
    return bookCourier.err({ code: "STORE_UNAVAILABLE", ...storeFailure(counted.error) });
  }

  const { attempt, bookingReference: existing } = counted.value;

  if (existing.kind === "set") {
    return bookCourier.ok({ orderId, courier, express, bookingReference: existing.value, attempt, replayed: true });
  }

  if (draw(COURIER_FLAKE_RATE, [orderId, "courier"]) && attempt <= COURIER_FAILING_ATTEMPTS) {
    return bookCourier.err({ code: "COURIER_UNAVAILABLE", message: `${courier} refused attempt ${String(attempt)}` });
  }

  const reference = bookingReference(courier);
  const stored = await setBookingReference(store, orderId, reference);

  if (!stored.ok) {
    return bookCourier.err({ code: "STORE_UNAVAILABLE", ...storeFailure(stored.error) });
  }

  if (!stored.value) {
    return bookCourier.err({ code: "SHIPMENT_NOT_FOUND", message: `order ${orderId} has no shipment row` });
  }

  return bookCourier.ok({ orderId, courier, express, bookingReference: reference, attempt, replayed: false });
}

/** Print the label of the booked shipment of `orderId`. `SHIPMENT_NOT_FOUND` for no shipment or no booking. */
export async function label(store: Store, orderId: string): Promise<LabelResult> {
  const read = await getShipment(store, orderId);

  if (!read.ok) {
    return printLabel.err({ code: "STORE_UNAVAILABLE", ...storeFailure(read.error) });
  }

  if (read.value.kind === "notFound") {
    return printLabel.err({ code: "SHIPMENT_NOT_FOUND", message: `order ${orderId} has no shipment row` });
  }

  const shipment = read.value.value;

  if (shipment.bookingReference.kind === "notSet") {
    return printLabel.err({ code: "SHIPMENT_NOT_FOUND", message: `the shipment of order ${orderId} has no booking` });
  }

  const labelUrl = `https://labels.acme.invalid/${shipment.bookingReference.value}.pdf`;
  const stored = await setLabelUrl(store, orderId, labelUrl);

  if (!stored.ok) {
    return printLabel.err({ code: "STORE_UNAVAILABLE", ...storeFailure(stored.error) });
  }

  return printLabel.ok({ orderId, labelUrl, labelFormat: shipment.express ? "A6" : "A5" });
}

/** Seed the tracking code of the shipment of `orderId` and mark the order shipped. `SHIPMENT_NOT_FOUND`. */
export async function track(store: Store, orderId: string): Promise<TrackResult> {
  const read = await getShipment(store, orderId);

  if (!read.ok) {
    return trackingSeed.err({ code: "STORE_UNAVAILABLE", ...storeFailure(read.error) });
  }

  if (read.value.kind === "notFound") {
    return trackingSeed.err({ code: "SHIPMENT_NOT_FOUND", message: `order ${orderId} has no shipment row` });
  }

  const courier = read.value.value.courier;
  const trackingCode = `ACME${String(integer(MIN_TRACKING_NUMBER, MAX_TRACKING_NUMBER, [orderId, "track"]))}`;
  const stored = await setTrackingCode(store, orderId, trackingCode);

  if (!stored.ok) {
    return trackingSeed.err({ code: "STORE_UNAVAILABLE", ...storeFailure(stored.error) });
  }

  const marked = await setOrderStatus(store, orderId, "shipped");

  if (!marked.ok) {
    return trackingSeed.err({ code: "STORE_UNAVAILABLE", ...storeFailure(marked.error) });
  }

  return trackingSeed.ok({ orderId, courier, trackingCode, trackingUrl: `https://track.${courier}.invalid/${trackingCode}` });
}

/** The label after the booking node: `UPSTREAM_FAILED` when the booking failed. */
export async function labelAfter(store: Store, orderId: string, booking: TaskOutcome<CourierBooking>): Promise<LabelResult> {
  switch (booking.kind) {
    case "succeeded":
      return label(store, orderId);
    case "failed":
      return printLabel.err({ code: "UPSTREAM_FAILED", ...upstreamFailure(booking.error) });
    default: {
      const unexpected: never = booking;

      throw new TypeError(`labelAfter: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** The tracking code after the label node: `UPSTREAM_FAILED` when the label failed. */
export async function trackAfter(store: Store, orderId: string, shippingLabel: TaskOutcome<ShippingLabel>): Promise<TrackResult> {
  switch (shippingLabel.kind) {
    case "succeeded":
      return track(store, orderId);
    case "failed":
      return trackingSeed.err({ code: "UPSTREAM_FAILED", ...upstreamFailure(shippingLabel.error) });
    default: {
      const unexpected: never = shippingLabel;

      throw new TypeError(`trackAfter: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** Implement the shipping tasks on `app`; each one uses the store of `store`. */
export function implementShippingTasks(app: Horsies, store: StoreKey): void {
  app.implement(bookCourier, async (input, runtime) => book(runtime.state(store), input));
  app.implement(printLabel, async ({ orderId, booking }, runtime) => labelAfter(runtime.state(store), orderId, booking));
  app.implement(trackingSeed, async ({ orderId, label: shippingLabel }, runtime) =>
    trackAfter(runtime.state(store), orderId, shippingLabel),
  );
}
