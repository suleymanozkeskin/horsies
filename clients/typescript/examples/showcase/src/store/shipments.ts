// The shipment of an order: the courier, the booking attempts, the label and the tracking code.

import { randomUUID } from "node:crypto";

import type { Result } from "horsies";

import { booleanColumn, intColumn, nullableTextColumn, textColumn } from "./columns.ts";
import { changedRows, firstRow, found, inTransaction, type Lookup, NOT_FOUND, type Store, type StoreFailure } from "./connection.ts";

/** A field of a shipment that a step of the shipping workflow sets, or its absence. */
export type ShipmentField = { readonly kind: "set"; readonly value: string } | { readonly kind: "notSet" };

/** A shipment row. */
export type Shipment = {
  readonly shipmentId: string;
  readonly orderId: string;
  readonly courier: string;
  readonly express: boolean;
  readonly attempts: number;
  readonly bookingReference: ShipmentField;
  readonly labelUrl: ShipmentField;
  readonly trackingCode: ShipmentField;
};

/** One counted booking attempt: its number, and the booking of an earlier attempt. */
export type CourierAttempt = { readonly attempt: number; readonly bookingReference: ShipmentField };

/** The columns of a shipment that a step sets. */
type SettableColumn = "booking_reference" | "label_url" | "tracking_code";

/** The hex digits of the random part of a shipment id. */
const SHIPMENT_ID_RANDOM_DIGITS = 16;

function fieldOf(value: string | null): ShipmentField {
  return value === null ? { kind: "notSet" } : { kind: "set", value };
}

/** Count one more courier attempt of `orderId`; the first attempt writes the shipment row. */
export async function countCourierAttempt(
  store: Store,
  orderId: string,
  courier: string,
  express: boolean,
): Promise<Result<CourierAttempt, StoreFailure>> {
  const shipmentId = `shp_${randomUUID().replaceAll("-", "").slice(0, SHIPMENT_ID_RANDOM_DIGITS)}`;

  return inTransaction(store, "count_courier_attempt", async (client) => {
    const row = await firstRow(
      client,
      `INSERT INTO acme_shipments (shipment_id, order_id, courier, express, attempts) VALUES ($1, $2, $3, $4, 1)
       ON CONFLICT (order_id) DO UPDATE SET attempts = acme_shipments.attempts + 1
       RETURNING attempts, booking_reference`,
      [shipmentId, orderId, courier, express],
    );

    if (row.kind === "notFound") {
      throw new TypeError("store: an INSERT ... RETURNING gave no row");
    }

    return {
      attempt: intColumn(row.value, "attempts"),
      bookingReference: fieldOf(nullableTextColumn(row.value, "booking_reference")),
    };
  });
}

async function setShipmentColumn(
  store: Store,
  operation: string,
  column: SettableColumn,
  orderId: string,
  value: string,
): Promise<Result<boolean, StoreFailure>> {
  return inTransaction(store, operation, async (client) => {
    // `column` is one of the three column names of `SettableColumn`, never an input.
    const changed = await changedRows(client, `UPDATE acme_shipments SET ${column} = $1 WHERE order_id = $2`, [value, orderId]);

    return changed > 0;
  });
}

/** Set the booking reference of the shipment of `orderId`. `false` for no shipment. */
export async function setBookingReference(store: Store, orderId: string, value: string): Promise<Result<boolean, StoreFailure>> {
  return setShipmentColumn(store, "set_booking_reference", "booking_reference", orderId, value);
}

/** Set the label URL of the shipment of `orderId`. `false` for no shipment. */
export async function setLabelUrl(store: Store, orderId: string, value: string): Promise<Result<boolean, StoreFailure>> {
  return setShipmentColumn(store, "set_label_url", "label_url", orderId, value);
}

/** Set the tracking code of the shipment of `orderId`. `false` for no shipment. */
export async function setTrackingCode(store: Store, orderId: string, value: string): Promise<Result<boolean, StoreFailure>> {
  return setShipmentColumn(store, "set_tracking_code", "tracking_code", orderId, value);
}

/** The shipment of `orderId`, or `notFound`. */
export async function getShipment(store: Store, orderId: string): Promise<Result<Lookup<Shipment>, StoreFailure>> {
  return inTransaction(store, "get_shipment", async (client) => {
    const row = await firstRow(
      client,
      `SELECT shipment_id, order_id, courier, express, attempts, booking_reference, label_url, tracking_code
       FROM acme_shipments WHERE order_id = $1`,
      [orderId],
    );

    if (row.kind === "notFound") {
      return NOT_FOUND;
    }

    return found({
      shipmentId: textColumn(row.value, "shipment_id"),
      orderId: textColumn(row.value, "order_id"),
      courier: textColumn(row.value, "courier"),
      express: booleanColumn(row.value, "express"),
      attempts: intColumn(row.value, "attempts"),
      bookingReference: fieldOf(nullableTextColumn(row.value, "booking_reference")),
      labelUrl: fieldOf(nullableTextColumn(row.value, "label_url")),
      trackingCode: fieldOf(nullableTextColumn(row.value, "tracking_code")),
    });
  });
}
