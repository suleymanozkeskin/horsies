// The shipping workflow: book the courier, print the label, seed the tracking code. The order workflow runs it as a
// sub-workflow node. Its result is the tracking seed.

import { defineWorkflow, h } from "horsies";

import { bookCourier, printLabel, trackingSeed } from "../contracts/shipping.ts";
import { TrackingSeed } from "../domain.ts";

/** The check case of the shipping workflow. */
export const SHIPPING_CHECK_PARAMS = { orderId: "CHECK-ORDER", courier: "fleetline", express: false } as const;

export const shipping = defineWorkflow(
  {
    name: "shipping",
    definitionKey: "acme.shipping.v1",
    params: { orderId: h.string(), courier: h.string(), express: h.boolean() },
    output: TrackingSeed,
    onError: "fail",
    checkCases: [SHIPPING_CHECK_PARAMS],
  },
  (w, { orderId, courier, express }) => {
    const booking = w.task(bookCourier, { orderId, courier, express }, { id: "book_courier" });
    const label = w.task(printLabel, { orderId, booking }, { id: "print_label" });
    const tracking = w.task(trackingSeed, { orderId, label }, { id: "tracking_seed" });

    return { output: tracking };
  },
);
