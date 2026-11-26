// The logic of the payment tasks: one test per outcome. The database tests run only with `ACME_DATABASE_URL`.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { draw } from "../src/simulate.ts";
import type { Store } from "../src/store/connection.ts";
import { authorizationAttempts, getOrder } from "../src/store/orders.ts";
import { reserve } from "../src/tasks/inventory.ts";
import { authorize, capture, captureAfter, reconcile, refund } from "../src/tasks/payments.ts";
import { CARD_DECLINE_RATE, PSP_FAILING_ATTEMPTS, PSP_UNAVAILABLE_RATE } from "../src/tuning.ts";
import { uniqueId } from "./support.ts";
import {
  failed,
  idWhere,
  seedOrder,
  seedProduct,
  stockOf,
  succeeded,
  upstreamError,
  withStore,
  withUnreachableStore,
} from "./fixtures.ts";

/** The amount of every test hold, in cents. */
const AMOUNT_CENTS = 2_000;

/** An order id that the card and the provider draws do not refuse. */
function payableId(): string {
  return idWhere(uniqueId("t1-pay"), (id) => !draw(CARD_DECLINE_RATE, [id, "card"]) && !draw(PSP_UNAVAILABLE_RATE, [id, "psp"]));
}

/** A seeded order of one line, under `orderId`. */
async function seedPaymentOrder(store: Store, orderId: string): Promise<string> {
  const sku = await seedProduct(store, "pay", 8);

  await seedOrder(store, orderId, [{ sku, quantity: 2 }]);

  return sku;
}

async function statusOf(store: Store, orderId: string): Promise<string> {
  const read = await getOrder(store, orderId);

  assert.ok(read.ok && read.value.kind === "found", `the order ${orderId}`);

  return read.value.value.status;
}

describe("authorize", () => {
  test("a payable order: the hold, on attempt 1", async () => {
    await withStore(async (store) => {
      const orderId = payableId();

      await seedPaymentOrder(store, orderId);

      const hold = succeeded(await authorize(store, orderId, AMOUNT_CENTS));

      assert.equal(hold.orderId, orderId);
      assert.equal(hold.amountCents, AMOUNT_CENTS);
      assert.equal(hold.attempt, 1);
      assert.equal(hold.replayed, false);
      assert.match(hold.pspReference, /^psp_[0-9a-f]{32}$/u);
    });
  });

  test("an order with a hold: a replay of the same hold, no second hold", async () => {
    await withStore(async (store) => {
      const orderId = payableId();

      await seedPaymentOrder(store, orderId);

      const first = succeeded(await authorize(store, orderId, AMOUNT_CENTS));
      const replay = succeeded(await authorize(store, orderId, AMOUNT_CENTS));

      assert.equal(replay.replayed, true);
      assert.equal(replay.attempt, 2);
      assert.equal(replay.authorizationId, first.authorizationId);
    });
  });

  test("an unknown order: ORDER_NOT_FOUND", async () => {
    await withStore(async (store) => {
      assert.equal(failed(await authorize(store, uniqueId("t1-missing"), AMOUNT_CENTS)).code, "ORDER_NOT_FOUND");
    });
  });

  test("a drawn decline: CARD_DECLINED; the order fails and releases its reservations", async () => {
    await withStore(async (store) => {
      const orderId = idWhere(uniqueId("t1-declined"), (id) => draw(CARD_DECLINE_RATE, [id, "card"]));
      const sku = await seedPaymentOrder(store, orderId);

      succeeded(await reserve(store, { orderId, lineNo: 1, sku, quantity: 2 }));

      assert.equal(failed(await authorize(store, orderId, AMOUNT_CENTS)).code, "CARD_DECLINED");
      assert.equal(await statusOf(store, orderId), "failed");
      assert.deepEqual(await stockOf(store, sku), { onHand: 8, reserved: 0 });
    });
  });

  test("a drawn outage: PSP_UNAVAILABLE on the failing attempts, then the hold", async () => {
    await withStore(async (store) => {
      const orderId = idWhere(
        uniqueId("t1-outage"),
        (id) => !draw(CARD_DECLINE_RATE, [id, "card"]) && draw(PSP_UNAVAILABLE_RATE, [id, "psp"]),
      );

      await seedPaymentOrder(store, orderId);

      for (let attempt = 1; attempt <= PSP_FAILING_ATTEMPTS; attempt += 1) {
        assert.equal(failed(await authorize(store, orderId, AMOUNT_CENTS)).code, "PSP_UNAVAILABLE");
      }

      const hold = succeeded(await authorize(store, orderId, AMOUNT_CENTS));

      assert.equal(hold.attempt, PSP_FAILING_ATTEMPTS + 1);

      const attempts = await authorizationAttempts(store, orderId);

      assert.ok(attempts.ok);
      assert.deepEqual(attempts.value, { kind: "found", value: PSP_FAILING_ATTEMPTS + 1 });
    });
  });
});

describe("capture", () => {
  test("a hold: the capture, and the order is captured", async () => {
    await withStore(async (store) => {
      const orderId = payableId();

      await seedPaymentOrder(store, orderId);

      const hold = succeeded(await authorize(store, orderId, AMOUNT_CENTS));
      const settled = succeeded(await capture(store, orderId, hold));

      assert.equal(settled.authorizationId, hold.authorizationId);
      assert.equal(settled.amountCents, AMOUNT_CENTS);
      assert.equal(settled.replayed, false);
      assert.equal(await statusOf(store, orderId), "captured");
    });
  });

  test("an order with a capture: PAYMENT_ALREADY_CAPTURED", async () => {
    await withStore(async (store) => {
      const orderId = payableId();

      await seedPaymentOrder(store, orderId);

      const hold = succeeded(await authorize(store, orderId, AMOUNT_CENTS));

      succeeded(await capture(store, orderId, hold));
      assert.equal(failed(await capture(store, orderId, hold)).code, "PAYMENT_ALREADY_CAPTURED");
    });
  });

  test("a failed authorization node: UPSTREAM_FAILED with the upstream code, and no capture", async () => {
    await withUnreachableStore(async (store) => {
      const error = failed(await captureAfter(store, "t1-any", { kind: "failed", error: upstreamError("CARD_DECLINED") }));

      assert.equal(error.code, "UPSTREAM_FAILED");
      assert.deepEqual(error.data, { upstreamCode: "CARD_DECLINED" });
    });
  });
});

describe("refund and reconcile", () => {
  test("a refund: the refund of the order, with the instant of its dispute", () => {
    const disputedAt = Temporal.Instant.from("2026-01-15T09:00:00Z");

    assert.deepEqual(succeeded(refund("ACME-00001", 4_500, disputedAt)), {
      orderId: "ACME-00001",
      refundId: "rfd_ACME-00001",
      amountCents: 4_500,
      disputedAt,
    });
  });

  test("a reconciliation window: the same draws for the same window; captures are the matched authorizations", () => {
    const report = succeeded(reconcile("daily"));

    assert.deepEqual(succeeded(reconcile("daily")), report);
    assert.equal(report.captures, report.authorizations - report.unmatched);
    assert.ok(report.unmatched >= 0 && report.unmatched <= 3);
  });
});

describe("a store that does not connect", () => {
  test("each store-backed payment function: STORE_UNAVAILABLE", async () => {
    await withUnreachableStore(async (store) => {
      const hold = {
        orderId: "t1-any",
        authorizationId: "pay_any",
        amountCents: AMOUNT_CENTS,
        pspReference: "psp_any",
        attempt: 1,
        replayed: false,
      };

      assert.equal(failed(await authorize(store, "t1-any", AMOUNT_CENTS)).code, "STORE_UNAVAILABLE");
      assert.equal(failed(await capture(store, "t1-any", hold)).code, "STORE_UNAVAILABLE");
    });
  });
});
