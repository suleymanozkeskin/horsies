// The payments of an order: at most one authorization and one capture.

import { randomUUID } from "node:crypto";

import type { Result } from "horsies";

import type { PaymentKind } from "../domain.ts";
import { intColumn, textColumn } from "./columns.ts";
import { firstRow, found, inTransaction, type Lookup, NOT_FOUND, type Row, type Store, type StoreFailure } from "./connection.ts";

/** A payment row. */
export type Payment = {
  readonly paymentId: string;
  readonly orderId: string;
  readonly kind: PaymentKind;
  readonly amountCents: number;
  readonly pspReference: string;
};

/** The hex digits of the random part of a payment id. */
const PAYMENT_ID_RANDOM_DIGITS = 16;

/** What a payment record did. */
export type PaymentRecord =
  | { readonly kind: "recorded"; readonly payment: Payment }
  /** The order has a payment of the kind already; nothing changed. */
  | { readonly kind: "exists" };

function paymentKindOf(text: string): PaymentKind {
  switch (text) {
    case "authorization":
    case "capture":
      return text;
    default:
      throw new TypeError(`store: ${JSON.stringify(text)} is not a payment kind`);
  }
}

function paymentOf(row: Row): Payment {
  return {
    paymentId: textColumn(row, "payment_id"),
    orderId: textColumn(row, "order_id"),
    kind: paymentKindOf(textColumn(row, "kind")),
    amountCents: intColumn(row, "amount_cents"),
    pspReference: textColumn(row, "psp_reference"),
  };
}

/** The payment of `kind` of `orderId`, or `notFound`. */
export async function findPayment(store: Store, orderId: string, kind: PaymentKind): Promise<Result<Lookup<Payment>, StoreFailure>> {
  return inTransaction(store, "find_payment", async (client) => {
    const row = await firstRow(
      client,
      "SELECT payment_id, order_id, kind, amount_cents, psp_reference FROM acme_payments WHERE order_id = $1 AND kind = $2",
      [orderId, kind],
    );

    return row.kind === "found" ? found(paymentOf(row.value)) : NOT_FOUND;
  });
}

/** Write a payment of `kind` for `orderId`, unless the order has one of that kind. */
export async function recordPayment(
  store: Store,
  orderId: string,
  kind: PaymentKind,
  amountCents: number,
  pspReference: string,
): Promise<Result<PaymentRecord, StoreFailure>> {
  const paymentId = `pay_${randomUUID().replaceAll("-", "").slice(0, PAYMENT_ID_RANDOM_DIGITS)}`;

  return inTransaction(store, "record_payment", async (client): Promise<PaymentRecord> => {
    const row = await firstRow(
      client,
      `INSERT INTO acme_payments (payment_id, order_id, kind, amount_cents, psp_reference) VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (order_id, kind) DO NOTHING
       RETURNING payment_id, order_id, kind, amount_cents, psp_reference`,
      [paymentId, orderId, kind, amountCents, pspReference],
    );

    return row.kind === "found" ? { kind: "recorded", payment: paymentOf(row.value) } : { kind: "exists" };
  });
}
