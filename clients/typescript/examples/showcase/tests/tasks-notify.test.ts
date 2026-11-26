// The notification task logic: one test per outcome of each function. No database.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { TaskOutcome } from "horsies";

import { winbackBlast } from "../src/contracts/notify.ts";
import type { AbandonedCartSweep, PaymentCapture } from "../src/domain.ts";
import {
  CONFIRMATION_TEMPLATE,
  DEMO_RECIPIENT,
  NOT_COMPLETED_TEMPLATE,
  orderEmail,
  segmentBlast,
  segmentSize,
  shippingNotice,
  winbackMessage,
} from "../src/tasks/notify.ts";
import { MARKETING_SEGMENT_SIZE } from "../src/tuning.ts";
import { resultsOf } from "./fixtures.ts";

const winbackBlastResults = resultsOf(winbackBlast);

/**
 * A failed upstream outcome with the custom code `code`, as a worker gives it to a node. This is the one place in these
 * tests that writes the code shape of a task error.
 */
function failedOutcome<O>(code: string): TaskOutcome<O> {
  return {
    kind: "failed",
    error: { kind: "custom", code, message: `${code} in the test`, data: null, exception: null, subWorkflow: null },
  };
}

const CAPTURE: PaymentCapture = {
  orderId: "ACME-00001",
  captureId: "pay_capture",
  authorizationId: "pay_authorization",
  amountCents: 4_200,
  replayed: false,
};

describe("orderEmail", () => {
  test("a captured order gets the confirmation", () => {
    assert.deepEqual(orderEmail("ACME-00001", { kind: "succeeded", value: CAPTURE }), {
      orderId: "ACME-00001",
      template: CONFIRMATION_TEMPLATE,
      recipient: DEMO_RECIPIENT,
    });
  });

  test("an order whose capture failed gets the not-completed template", () => {
    assert.equal(orderEmail("ACME-00001", failedOutcome("CARD_DECLINED")).template, NOT_COMPLETED_TEMPLATE);
  });
});

describe("shippingNotice and segmentBlast", () => {
  test("the notice carries the tracking code to the demo recipient", () => {
    assert.deepEqual(shippingNotice("ACME-00002", "ACME12345678"), {
      orderId: "ACME-00002",
      recipient: DEMO_RECIPIENT,
      trackingCode: "ACME12345678",
    });
  });

  test("a segment has the same size each time, from 1 to the tuned size", () => {
    const blast = segmentBlast("newsletter");

    assert.deepEqual(segmentBlast("newsletter"), blast);
    assert.ok(blast.recipients >= 1 && blast.recipients <= MARKETING_SEGMENT_SIZE);
  });
});

describe("winbackMessage", () => {
  test("a sweep sends to its stranded customers, at most the segment's size", () => {
    const sweep: AbandonedCartSweep = { swept: 7, oldest: { kind: "found", orderId: "ACME-00003" } };
    const result = winbackMessage("winback", { kind: "succeeded", value: sweep }, winbackBlastResults);

    assert.ok(result.ok);
    assert.equal(result.value.recipients, Math.min(7, segmentSize("winback")));
  });

  test("a failed sweep is UPSTREAM_FAILED with the code of the sweep", () => {
    const result = winbackMessage("winback", failedOutcome("BOOM"), winbackBlastResults);

    assert.ok(!result.ok);
    assert.equal(result.error.code, "UPSTREAM_FAILED");
    assert.deepEqual(result.error.data, { upstreamCode: "BOOM" });
  });
});
