// The order workflow: validate, reserve each line, authorize, pick and invoice, ship (a sub-workflow), capture, and
// email the customer. Its params are the order; its result is the capture.
//
// A refused reservation fails the order before the payment: the nodes after it are skipped. The email node takes
// failed dependencies, so the customer gets the "order not completed" email.

import { defineWorkflow } from "horsies";

import { reserveStock } from "../contracts/inventory.ts";
import { sendOrderEmail } from "../contracts/notify.ts";
import { generateInvoice, pickPack, validateOrder } from "../contracts/orders.ts";
import { authorizePayment, capturePayment } from "../contracts/payments.ts";
import { Order, type OrderLine, PaymentCapture } from "../domain.ts";
import { choice, draw } from "../simulate.ts";
import { COURIERS, EXPRESS_RATE, MAX_LINES_PER_ORDER, SIZE_CODES } from "../tuning.ts";
import { shipping } from "./shipping.ts";

/** The unit price of each line of the check order, in cents. */
const CHECK_UNIT_PRICE_CENTS = 3_300;

/** The check case of the order workflow: an order of the most lines. */
export function checkOrder(): Order {
  const lines: OrderLine[] = Array.from({ length: MAX_LINES_PER_ORDER }, (_unused, index) => ({
    lineNo: index + 1,
    sku: `ACME-SKU-${String(index + 1).padStart(4, "0")}`,
    sizeCode: SIZE_CODES[0],
    quantity: 1,
    unitPriceCents: CHECK_UNIT_PRICE_CENTS,
  }));

  return {
    orderId: "ACME-CHECK-0001",
    customerId: "CUS-CHECK",
    status: "placed",
    totalCents: CHECK_UNIT_PRICE_CENTS * MAX_LINES_PER_ORDER,
    lines,
    createdAt: Temporal.Instant.from("2026-01-01T00:00:00Z"),
  };
}

/** The node id of the reservation of line `lineNo`. Pure. */
export function reservationNodeId(lineNo: number): `reserve_stock_${number}` {
  return `reserve_stock_${lineNo}`;
}

export const orderFulfillment = defineWorkflow(
  {
    name: "order_fulfillment",
    definitionKey: "acme.order_fulfillment.v1",
    params: { order: Order },
    output: PaymentCapture,
    onError: "fail",
    checkCases: [{ order: checkOrder() }],
  },
  (w, { order }) => {
    const orderId = order.orderId;
    const validate = w.task(validateOrder, { orderId }, { id: "validate_order" });

    const reservations = order.lines.map((line) =>
      w.task(
        reserveStock,
        { orderId, lineNo: line.lineNo, sku: line.sku, quantity: line.quantity },
        { id: reservationNodeId(line.lineNo), waitsFor: [validate] },
      ),
    );

    const authorize = w.task(
      authorizePayment,
      { orderId, amountCents: order.totalCents },
      { id: "authorize_payment", waitsFor: reservations },
    );

    const pick = w.task(pickPack, { orderId }, { id: "pick_pack", waitsFor: [authorize] });
    const invoice = w.task(generateInvoice, { orderId }, { id: "generate_invoice", waitsFor: [authorize] });

    const shipment = w.subWorkflow(
      shipping,
      {
        orderId,
        courier: choice(COURIERS, [orderId, "courier"]),
        express: draw(EXPRESS_RATE, [orderId, "express"]),
      },
      { id: "shipping", waitsFor: [pick, invoice] },
    );

    const capture = w.task(
      capturePayment,
      { orderId, authorization: authorize },
      { id: "capture_payment", waitsFor: [shipment] },
    );

    w.task(sendOrderEmail, { orderId, capture }, { id: "send_order_email", allowFailedDeps: true });

    return { output: capture };
  },
);
