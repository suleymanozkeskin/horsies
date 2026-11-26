// Orders, their lines, and the stock reservations of the lines.
//
// The lock rule: a function that changes the reservations of an order locks the order row first, then its lines, so
// a compensation cannot race a later reservation of the same order.

import type { Result } from "horsies";

import { ACTIVE_STATUSES, type Order, type OrderLine, type OrderStatus, RESERVABLE_STATUSES } from "../domain.ts";
import { bigintColumn, booleanColumn, instantColumn, intColumn, textColumn } from "./columns.ts";
import { changedRows, firstRow, found, inTransaction, type Lookup, NOT_FOUND, rowsOf, type Store, type StoreClient, type StoreFailure } from "./connection.ts";

/** The statuses of an order, as the store keeps them. */
const ORDER_STATUSES: readonly OrderStatus[] = ["placed", "validated", "reserved", "authorized", "packed", "shipped", "captured", "failed"];

/** What a reservation of one order line did. */
export type ReservationOutcome =
  | { readonly kind: "orderNotFound" }
  /** The order is past the statuses that take a reservation. */
  | { readonly kind: "orderClosed"; readonly status: OrderStatus }
  | { readonly kind: "lineNotFound" }
  | { readonly kind: "unknownSku" }
  | { readonly kind: "insufficientStock"; readonly available: number }
  | { readonly kind: "reserved"; readonly availableAfter: number }
  /** An earlier run reserved the line; nothing changed. */
  | { readonly kind: "alreadyReserved"; readonly available: number };

/** What a consumption of one order line did. */
export type ConsumptionOutcome = "lineNotFound" | "consumed" | "alreadyConsumed";

/** The status of a status column. A value outside `OrderStatus` is a defect of the store. */
function orderStatusOf(text: string): OrderStatus {
  const status = ORDER_STATUSES.find((known) => known === text);

  if (status === undefined) {
    throw new TypeError(`store: ${JSON.stringify(text)} is not an order status`);
  }

  return status;
}

/** The next order number of the order sequence. */
export async function nextOrderNumber(store: Store): Promise<Result<number, StoreFailure>> {
  return inTransaction(store, "next_order_number", async (client) => {
    const row = await firstRow(client, "SELECT nextval('acme_order_seq') AS number");

    return row.kind === "found" ? bigintColumn(row.value, "number") : 0;
  });
}

/** Write `order` and its lines, in one transaction. */
export async function insertOrder(store: Store, order: Order): Promise<Result<void, StoreFailure>> {
  return inTransaction(store, "insert_order", async (client) => {
    await client.query(
      `INSERT INTO acme_orders (order_id, customer_id, status, total_cents, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $5)`,
      [order.orderId, order.customerId, order.status, order.totalCents, new Date(order.createdAt.epochMilliseconds)],
    );

    for (const line of order.lines) {
      await client.query(
        `INSERT INTO acme_order_lines (order_id, line_no, sku, size_code, quantity, unit_price_cents)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [order.orderId, line.lineNo, line.sku, line.sizeCode, line.quantity, line.unitPriceCents],
      );
    }
  });
}

/** The order `orderId` with its lines by line number. */
export async function getOrder(store: Store, orderId: string): Promise<Result<Lookup<Order>, StoreFailure>> {
  return inTransaction(store, "get_order", async (client) => {
    const header = await firstRow(
      client,
      "SELECT order_id, customer_id, status, total_cents, created_at FROM acme_orders WHERE order_id = $1",
      [orderId],
    );

    if (header.kind === "notFound") {
      return NOT_FOUND;
    }

    return found({
      orderId: textColumn(header.value, "order_id"),
      customerId: textColumn(header.value, "customer_id"),
      status: orderStatusOf(textColumn(header.value, "status")),
      totalCents: intColumn(header.value, "total_cents"),
      lines: await orderLines(client, orderId),
      createdAt: instantColumn(header.value, "created_at"),
    });
  });
}

async function orderLines(client: StoreClient, orderId: string): Promise<OrderLine[]> {
  const rows = await rowsOf(
    client,
    "SELECT line_no, sku, size_code, quantity, unit_price_cents FROM acme_order_lines WHERE order_id = $1 ORDER BY line_no",
    [orderId],
  );

  return rows.map((row) => ({
    lineNo: intColumn(row, "line_no"),
    sku: textColumn(row, "sku"),
    sizeCode: textColumn(row, "size_code"),
    quantity: intColumn(row, "quantity"),
    unitPriceCents: intColumn(row, "unit_price_cents"),
  }));
}

/** Set the status of `orderId`. `false` for no such order. */
export async function setOrderStatus(store: Store, orderId: string, status: OrderStatus): Promise<Result<boolean, StoreFailure>> {
  return inTransaction(store, "set_order_status", async (client) => {
    const changed = await changedRows(client, "UPDATE acme_orders SET status = $1, updated_at = now() WHERE order_id = $2", [
      status,
      orderId,
    ]);

    return changed > 0;
  });
}

/** Count one more authorization attempt of `orderId`: the count after, or `notFound`. */
export async function countAuthorizationAttempt(store: Store, orderId: string): Promise<Result<Lookup<number>, StoreFailure>> {
  return inTransaction(store, "count_authorization_attempt", async (client) => {
    const row = await firstRow(
      client,
      `UPDATE acme_orders SET authorization_attempts = authorization_attempts + 1, updated_at = now()
       WHERE order_id = $1 RETURNING authorization_attempts`,
      [orderId],
    );

    return row.kind === "found" ? found(intColumn(row.value, "authorization_attempts")) : NOT_FOUND;
  });
}

/** The authorization attempts of `orderId`, or `notFound`. */
export async function authorizationAttempts(store: Store, orderId: string): Promise<Result<Lookup<number>, StoreFailure>> {
  return inTransaction(store, "authorization_attempts", async (client) => {
    const row = await firstRow(client, "SELECT authorization_attempts FROM acme_orders WHERE order_id = $1", [orderId]);

    return row.kind === "found" ? found(intColumn(row.value, "authorization_attempts")) : NOT_FOUND;
  });
}

/** Reserve `quantity` units of `sku` for line `lineNo` of `orderId`. A line that is reserved already changes nothing. */
export async function reserveLine(
  store: Store,
  orderId: string,
  lineNo: number,
  sku: string,
  quantity: number,
): Promise<Result<ReservationOutcome, StoreFailure>> {
  return inTransaction(store, "reserve_line", async (client): Promise<ReservationOutcome> => {
    const order = await firstRow(client, "SELECT status FROM acme_orders WHERE order_id = $1 FOR UPDATE", [orderId]);

    if (order.kind === "notFound") {
      return { kind: "orderNotFound" };
    }

    const status = orderStatusOf(textColumn(order.value, "status"));

    if (!RESERVABLE_STATUSES.includes(status)) {
      return { kind: "orderClosed", status };
    }

    const line = await firstRow(client, "SELECT reserved FROM acme_order_lines WHERE order_id = $1 AND line_no = $2 FOR UPDATE", [
      orderId,
      lineNo,
    ]);

    if (line.kind === "notFound") {
      return { kind: "lineNotFound" };
    }

    const stock = await firstRow(client, "SELECT on_hand - reserved AS available FROM acme_stock WHERE sku = $1", [sku]);

    if (stock.kind === "notFound") {
      return { kind: "unknownSku" };
    }

    const available = intColumn(stock.value, "available");

    if (booleanColumn(line.value, "reserved")) {
      return { kind: "alreadyReserved", available };
    }

    if (available < quantity) {
      return { kind: "insufficientStock", available };
    }

    const after = await firstRow(
      client,
      `UPDATE acme_stock SET reserved = reserved + $1, updated_at = now() WHERE sku = $2
       RETURNING on_hand - reserved AS available`,
      [quantity, sku],
    );

    await client.query("UPDATE acme_order_lines SET reserved = true WHERE order_id = $1 AND line_no = $2", [orderId, lineNo]);

    return { kind: "reserved", availableAfter: after.kind === "found" ? intColumn(after.value, "available") : available - quantity };
  });
}

/**
 * Mark `orderId` failed when it is active, and release the reservations of its lines that are not consumed, in one
 * transaction: the released units. A second call releases nothing.
 */
export async function failOrderAndReleaseReservations(store: Store, orderId: string): Promise<Result<number, StoreFailure>> {
  return inTransaction(store, "fail_order_and_release_reservations", async (client) => {
    const order = await firstRow(client, "SELECT status FROM acme_orders WHERE order_id = $1 FOR UPDATE", [orderId]);

    if (order.kind === "notFound") {
      return 0;
    }

    if (ACTIVE_STATUSES.includes(orderStatusOf(textColumn(order.value, "status")))) {
      await client.query("UPDATE acme_orders SET status = 'failed', updated_at = now() WHERE order_id = $1", [orderId]);
    }

    const lines = await rowsOf(
      client,
      `SELECT sku, quantity FROM acme_order_lines WHERE order_id = $1 AND reserved = true AND consumed = false
       ORDER BY sku, line_no FOR UPDATE`,
      [orderId],
    );

    let released = 0;

    for (const line of lines) {
      const quantity = intColumn(line, "quantity");

      await client.query("UPDATE acme_stock SET reserved = greatest(0, reserved - $1), updated_at = now() WHERE sku = $2", [
        quantity,
        textColumn(line, "sku"),
      ]);
      released += quantity;
    }

    await client.query("UPDATE acme_order_lines SET reserved = false WHERE order_id = $1 AND reserved = true AND consumed = false", [
      orderId,
    ]);

    return released;
  });
}

/** Take the reserved units of line `lineNo` of `orderId` off the shelf. A consumed line changes nothing. */
export async function consumeLine(
  store: Store,
  orderId: string,
  lineNo: number,
  sku: string,
  quantity: number,
): Promise<Result<ConsumptionOutcome, StoreFailure>> {
  return inTransaction(store, "consume_line", async (client): Promise<ConsumptionOutcome> => {
    const line = await firstRow(client, "SELECT consumed FROM acme_order_lines WHERE order_id = $1 AND line_no = $2 FOR UPDATE", [
      orderId,
      lineNo,
    ]);

    if (line.kind === "notFound") {
      return "lineNotFound";
    }

    if (booleanColumn(line.value, "consumed")) {
      return "alreadyConsumed";
    }

    await client.query(
      `UPDATE acme_stock SET on_hand = greatest(0, on_hand - $1), reserved = greatest(0, reserved - $1), updated_at = now()
       WHERE sku = $2`,
      [quantity, sku],
    );
    await client.query("UPDATE acme_order_lines SET consumed = true WHERE order_id = $1 AND line_no = $2", [orderId, lineNo]);

    return "consumed";
  });
}

/** One line of a captured order: what a return can take. */
export type ReturnableLine = { readonly orderId: string; readonly sku: string; readonly quantity: number };

/** The lines of the newest captured orders, at most `limit`. */
export async function listReturnableLines(store: Store, limit: number): Promise<Result<readonly ReturnableLine[], StoreFailure>> {
  return inTransaction(store, "list_returnable_lines", async (client) => {
    const rows = await rowsOf(
      client,
      `SELECT o.order_id, ol.sku, ol.quantity FROM acme_orders o JOIN acme_order_lines ol USING (order_id)
       WHERE o.status = 'captured' ORDER BY o.created_at DESC, ol.line_no LIMIT $1`,
      [limit],
    );

    return rows.map((row) => ({
      orderId: textColumn(row, "order_id"),
      sku: textColumn(row, "sku"),
      quantity: intColumn(row, "quantity"),
    }));
  });
}
