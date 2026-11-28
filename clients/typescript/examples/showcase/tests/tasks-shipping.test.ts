// The logic of the shipping tasks: one test per outcome. The database tests run only with `ACME_DATABASE_URL`.
//
// `book` gives `SHIPMENT_NOT_FOUND` only when its shipment row goes away between the attempt count and the booking
// write (the count writes the row); no test makes that race.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { bookCourier, printLabel, trackingSeed } from "../src/contracts/shipping.ts";
import { draw } from "../src/simulate.ts";
import type { Store } from "../src/store/connection.ts";
import { getOrder } from "../src/store/orders.ts";
import { countCourierAttempt, getShipment, type Shipment } from "../src/store/shipments.ts";
import { book, label, labelAfter, track, trackAfter } from "../src/tasks/shipping.ts";
import { COURIER_FAILING_ATTEMPTS, COURIER_FLAKE_RATE } from "../src/tuning.ts";
import { uniqueId } from "./support.ts";
import {
  failed,
  idWhere,
  resultsOf,
  seedOrder,
  seedProduct,
  succeeded,
  upstreamError,
  withStore,
  withUnreachableStore,
} from "./fixtures.ts";

const bookCourierResults = resultsOf(bookCourier);
const printLabelResults = resultsOf(printLabel);
const trackingSeedResults = resultsOf(trackingSeed);

const COURIER = "fleetline";

/** An order id that the courier draw does not flake. */
function steadyId(): string {
  return idWhere(uniqueId("t1-ship"), (id) => !draw(COURIER_FLAKE_RATE, [id, "courier"]));
}

async function seedShippableOrder(store: Store, orderId: string): Promise<void> {
  const sku = await seedProduct(store, "ship", 8);

  await seedOrder(store, orderId, [{ sku, quantity: 1 }]);
}

async function shipmentOf(store: Store, orderId: string): Promise<Shipment> {
  const read = await getShipment(store, orderId);

  assert.ok(read.ok && read.value.kind === "found", `the shipment of ${orderId}`);

  return read.value.value;
}

describe("book", () => {
  test("a courier that answers: the booking on attempt 1, stored on the shipment", async () => {
    await withStore(async (store) => {
      const orderId = steadyId();

      await seedShippableOrder(store, orderId);

      const booking = succeeded(await book(store, { orderId, courier: COURIER, express: false }, bookCourierResults));

      assert.equal(booking.attempt, 1);
      assert.equal(booking.replayed, false);
      assert.match(booking.bookingReference, /^FLE-[0-9a-f]{10}$/u);
      assert.deepEqual((await shipmentOf(store, orderId)).bookingReference, { kind: "set", value: booking.bookingReference });
    });
  });

  test("a shipment with a booking: a replay of the same booking", async () => {
    await withStore(async (store) => {
      const orderId = steadyId();

      await seedShippableOrder(store, orderId);

      const first = succeeded(await book(store, { orderId, courier: COURIER, express: false }, bookCourierResults));
      const replay = succeeded(await book(store, { orderId, courier: COURIER, express: false }, bookCourierResults));

      assert.equal(replay.replayed, true);
      assert.equal(replay.attempt, 2);
      assert.equal(replay.bookingReference, first.bookingReference);
    });
  });

  test("a drawn flake: COURIER_UNAVAILABLE on the failing attempts, then the booking; the attempts are stored", async () => {
    await withStore(async (store) => {
      const orderId = idWhere(uniqueId("t1-flake"), (id) => draw(COURIER_FLAKE_RATE, [id, "courier"]));

      await seedShippableOrder(store, orderId);

      for (let attempt = 1; attempt <= COURIER_FAILING_ATTEMPTS; attempt += 1) {
        assert.equal(
          failed(await book(store, { orderId, courier: COURIER, express: false }, bookCourierResults)).code,
          "COURIER_UNAVAILABLE",
        );
      }

      assert.equal(
        succeeded(await book(store, { orderId, courier: COURIER, express: false }, bookCourierResults)).attempt,
        COURIER_FAILING_ATTEMPTS + 1,
      );
      assert.equal((await shipmentOf(store, orderId)).attempts, COURIER_FAILING_ATTEMPTS + 1);
    });
  });
});

describe("label", () => {
  test("a booked shipment: the label, A5 for standard and A6 for express", async () => {
    await withStore(async (store) => {
      const standard = steadyId();
      const express = steadyId();

      await seedShippableOrder(store, standard);
      await seedShippableOrder(store, express);

      const booking = succeeded(await book(store, { orderId: standard, courier: COURIER, express: false }, bookCourierResults));

      succeeded(await book(store, { orderId: express, courier: COURIER, express: true }, bookCourierResults));

      const printed = succeeded(await label(store, standard, printLabelResults));

      assert.equal(printed.labelUrl, `https://labels.acme.invalid/${booking.bookingReference}.pdf`);
      assert.equal(printed.labelFormat, "A5");
      assert.equal(succeeded(await label(store, express, printLabelResults)).labelFormat, "A6");
      assert.deepEqual((await shipmentOf(store, standard)).labelUrl, { kind: "set", value: printed.labelUrl });
    });
  });

  test("an order with no shipment: SHIPMENT_NOT_FOUND", async () => {
    await withStore(async (store) => {
      const orderId = steadyId();

      await seedShippableOrder(store, orderId);
      assert.equal(failed(await label(store, orderId, printLabelResults)).code, "SHIPMENT_NOT_FOUND");
    });
  });

  test("a shipment with no booking: SHIPMENT_NOT_FOUND", async () => {
    await withStore(async (store) => {
      const orderId = steadyId();

      await seedShippableOrder(store, orderId);

      const counted = await countCourierAttempt(store, orderId, COURIER, false);

      assert.ok(counted.ok);

      const error = failed(await label(store, orderId, printLabelResults));

      assert.equal(error.code, "SHIPMENT_NOT_FOUND");
      assert.match(error.message ?? "", /has no booking/u);
    });
  });

  test("a failed booking node: UPSTREAM_FAILED", async () => {
    await withUnreachableStore(async (store) => {
      const error = failed(await labelAfter(
        store,
        "t1-any",
        { kind: "failed", error: upstreamError("COURIER_UNAVAILABLE") },
        printLabelResults,
      ));

      assert.equal(error.code, "UPSTREAM_FAILED");
      assert.deepEqual(error.data, { upstreamCode: "COURIER_UNAVAILABLE" });
    });
  });
});

describe("track", () => {
  test("a shipment: the tracking code of the order, stored; the order is shipped", async () => {
    await withStore(async (store) => {
      const orderId = steadyId();

      await seedShippableOrder(store, orderId);
      succeeded(await book(store, { orderId, courier: COURIER, express: false }, bookCourierResults));

      const seeded = succeeded(await track(store, orderId, trackingSeedResults));

      assert.match(seeded.trackingCode, /^ACME[0-9]{8}$/u);
      assert.equal(seeded.trackingUrl, `https://track.${COURIER}.invalid/${seeded.trackingCode}`);
      assert.deepEqual((await shipmentOf(store, orderId)).trackingCode, { kind: "set", value: seeded.trackingCode });

      const order = await getOrder(store, orderId);

      assert.ok(order.ok && order.value.kind === "found");
      assert.equal(order.value.value.status, "shipped");
    });
  });

  test("an order with no shipment: SHIPMENT_NOT_FOUND", async () => {
    await withStore(async (store) => {
      const orderId = steadyId();

      await seedShippableOrder(store, orderId);
      assert.equal(failed(await track(store, orderId, trackingSeedResults)).code, "SHIPMENT_NOT_FOUND");
    });
  });

  test("a failed label node: UPSTREAM_FAILED", async () => {
    await withUnreachableStore(async (store) => {
      const failedLabel = { kind: "failed", error: upstreamError("SHIPMENT_NOT_FOUND") } as const;

      assert.equal(failed(await trackAfter(store, "t1-any", failedLabel, trackingSeedResults)).code, "UPSTREAM_FAILED");
    });
  });
});

describe("a store that does not connect", () => {
  test("each store-backed shipping function: STORE_UNAVAILABLE", async () => {
    await withUnreachableStore(async (store) => {
      assert.equal(
        failed(await book(store, { orderId: "t1-any", courier: COURIER, express: false }, bookCourierResults)).code,
        "STORE_UNAVAILABLE",
      );
      assert.equal(failed(await label(store, "t1-any", printLabelResults)).code, "STORE_UNAVAILABLE");
      assert.equal(failed(await track(store, "t1-any", trackingSeedResults)).code, "STORE_UNAVAILABLE");
    });
  });
});
