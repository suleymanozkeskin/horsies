// The showcase through one worker of the entry (`worker --profile acme`): the order workflow (the happy path, a
// courier retry, a short stock), the three error codes of the promotion story, a crash and its retry, the operator
// cancel, the rerun with its audit row, and the report. Opt-in: `ACME_DATABASE_URL` (tests/support.ts).

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";

import type { TaskOutcome } from "horsies";

import { type ShowcaseApp, WORKER_PROFILE } from "../src/app.ts";
import { flakyExport } from "../src/contracts/analytics.ts";
import { computeLoyaltyPoints, applyPromotions, warmCacheEdge } from "../src/contracts/promotions.ts";
import { bookCourier } from "../src/contracts/shipping.ts";
import { cancelTask } from "../src/operations/cancel.ts";
import { reportLines } from "../src/operations/report.ts";
import { rerunWithAudit } from "../src/operations/rerun.ts";
import { failureCodeText } from "../src/outcome.ts";
import { draw } from "../src/simulate.ts";
import { closeStore, type Store } from "../src/store/connection.ts";
import { authorizationAttempts, getOrder } from "../src/store/orders.ts";
import { getShipment } from "../src/store/shipments.ts";
import {
  CARD_DECLINE_RATE,
  CHAOS_EXPORT_CRASH_RATE,
  COURIER_FLAKE_RATE,
  INVOICE_HANG_RATE,
  LOYALTY_ENGINE_BUG_RATE,
  PROMOTION_BUNDLE_BUG_RATE,
  PROMOTION_SIZE_CODE_BUG_RATE,
  PSP_UNAVAILABLE_RATE,
} from "../src/tuning.ts";
import { orderFulfillment } from "../src/workflows/order-fulfillment.ts";
import { type EntryProcess, logTail, startEntry, stopEntry } from "../scripts/entry-process.ts";
import { idWhere, seedOneLineOrder } from "./fixtures.ts";
import { testShowcase } from "./showcase.ts";
import { testDatabase, uniqueId } from "./support.ts";

/** The longest wait for one result: the worker starts its children first, and a retry waits for its delay. */
const RESULT_WAIT_MS = 90_000;

/** The timeout of one test: one result wait, and the setup around it. */
const TEST_TIMEOUT_MS = 150_000;

/** The delay of a task that the cancel test cancels before any worker claims it. */
const CANCEL_DELAY_SECONDS = 3_600;

/** The log lines of the worker that a failed assertion shows. */
const LOG_TAIL_LINES = 40;

type Context = { readonly showcase: ShowcaseApp; readonly store: Store; readonly worker: EntryProcess };

let context: Context | "skipped" = "skipped";

/** The code text of a failed outcome; a success fails the test. */
function failedCode<O>(outcome: TaskOutcome<O, string>): string {
  switch (outcome.kind) {
    case "failed":
      return failureCodeText(outcome.error);
    case "succeeded":
      return assert.fail(`expected a failed task, got ${JSON.stringify(outcome.value)}`);
    default: {
      const unexpected: never = outcome;

      throw new TypeError(`failedCode: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

function workerLog(ready: Context): string {
  return `worker log (${ready.worker.logPath}):\n${logTail(ready.worker, LOG_TAIL_LINES)}`;
}

/** The context of a test, or no context when the database tests are off. */
function ready(): Context | "skipped" {
  return context;
}

describe("through a worker of the entry", () => {
  before(async () => {
    const database = await testDatabase();

    if (database.kind === "skipped") {
      return;
    }

    const showcase = testShowcase(database.settings);
    const ping = await showcase.app.pingDatabase();

    assert.ok(ping.ok, "the schema installs before the worker starts");
    context = {
      showcase,
      store: database.store,
      worker: startEntry(["worker", "--profile", WORKER_PROFILE], { ACME_DATABASE_URL: database.settings.url }),
    };
  });

  after(async () => {
    const current = ready();

    if (current === "skipped") {
      return;
    }

    assert.equal(await stopEntry(current.worker), "exited", workerLog(current));
    await closeStore(current.store);
  });

  test("the order workflow captures the payment and ships the order", { timeout: TEST_TIMEOUT_MS }, async () => {
    const current = ready();

    if (current === "skipped") {
      return;
    }

    const orderId = idWhere(
      uniqueId("happy"),
      (id) =>
        !draw(CARD_DECLINE_RATE, [id, "card"]) &&
        !draw(INVOICE_HANG_RATE, [id, "invoice"]) &&
        !draw(PSP_UNAVAILABLE_RATE, [id, "psp"]) &&
        !draw(COURIER_FLAKE_RATE, [id, "courier"]),
    );

    const order = await seedOneLineOrder(current.store, orderId, 4, 1);
    const started = await current.showcase.app.start(orderFulfillment, { order });

    assert.ok(started.ok, "the order workflow starts");

    const read = await started.value.get({ timeoutMs: RESULT_WAIT_MS });

    assert.ok(read.ok && read.value.kind === "succeeded", `${JSON.stringify(read)}\n${workerLog(current)}`);

    const stored = await getOrder(current.store, orderId);

    assert.ok(stored.ok && stored.value.kind === "found");
    assert.equal(stored.value.value.status, "captured");

    const shipment = await getShipment(current.store, orderId);

    assert.ok(shipment.ok && shipment.value.kind === "found");
    assert.ok(shipment.value.value.attempts >= 1);
    assert.equal(shipment.value.value.trackingCode.kind, "set");
  });

  test("a courier flake retries, and the shipment counts both attempts", { timeout: TEST_TIMEOUT_MS }, async () => {
    const current = ready();

    if (current === "skipped") {
      return;
    }

    const orderId = idWhere(uniqueId("courier"), (id) => draw(COURIER_FLAKE_RATE, [id, "courier"]));

    await seedOneLineOrder(current.store, orderId, 4, 1);

    const sent = await current.showcase.app.send(bookCourier, { orderId, courier: "fleetline", express: false });

    assert.ok(sent.ok, "the booking sends");

    const read = await sent.value.get({ timeoutMs: RESULT_WAIT_MS });

    assert.ok(read.ok && read.value.kind === "succeeded", `${JSON.stringify(read)}\n${workerLog(current)}`);

    const shipment = await getShipment(current.store, orderId);

    assert.ok(shipment.ok && shipment.value.kind === "found");
    assert.ok(shipment.value.value.attempts >= 2, "the retry keeps its attempts");
  });

  test("a short stock fails the order before the payment", { timeout: TEST_TIMEOUT_MS }, async () => {
    const current = ready();

    if (current === "skipped") {
      return;
    }

    const order = await seedOneLineOrder(current.store, uniqueId("short"), 0, 1);
    const started = await current.showcase.app.start(orderFulfillment, { order });

    assert.ok(started.ok, "the order workflow starts");

    const read = await started.value.get({ timeoutMs: RESULT_WAIT_MS });

    assert.ok(read.ok, `${JSON.stringify(read)}\n${workerLog(current)}`);
    assert.equal(failedCode(read.value), "INSUFFICIENT_STOCK");

    const attempts = await authorizationAttempts(current.store, order.orderId);

    assert.ok(attempts.ok && attempts.value.kind === "found");
    assert.equal(attempts.value.value, 0, "no payment starts after a refused reservation");
  });

  test("the three error codes of the promotion story cross the worker", { timeout: TEST_TIMEOUT_MS }, async () => {
    const current = ready();

    if (current === "skipped") {
      return;
    }

    const bundleId = idWhere(uniqueId("bundle"), (id) => draw(PROMOTION_BUNDLE_BUG_RATE, [id, "bundle-bug"]));

    const sizeId = idWhere(
      uniqueId("size"),
      (id) => draw(PROMOTION_SIZE_CODE_BUG_RATE, [id, "size-code"]) && !draw(PROMOTION_BUNDLE_BUG_RATE, [id, "bundle-bug"]),
    );

    const loyaltyId = idWhere(uniqueId("loyalty"), (id) => draw(LOYALTY_ENGINE_BUG_RATE, [id, "lifetime-bug"]));

    const bundle = await current.showcase.app.send(applyPromotions, { orderId: bundleId });
    const size = await current.showcase.app.send(applyPromotions, { orderId: sizeId });
    const loyalty = await current.showcase.app.send(computeLoyaltyPoints, { customerId: loyaltyId, orderId: "story-order" });

    assert.ok(bundle.ok && size.ok && loyalty.ok, "the three tasks send");

    const bundleRead = await bundle.value.get({ timeoutMs: RESULT_WAIT_MS });
    const sizeRead = await size.value.get({ timeoutMs: RESULT_WAIT_MS });
    const loyaltyRead = await loyalty.value.get({ timeoutMs: RESULT_WAIT_MS });

    assert.ok(bundleRead.ok && sizeRead.ok && loyaltyRead.ok, `${JSON.stringify([bundleRead, sizeRead, loyaltyRead])}`);

    const codes = [failedCode(bundleRead.value), failedCode(sizeRead.value), failedCode(loyaltyRead.value)];

    assert.deepEqual(codes, ["UNHANDLED_EXCEPTION", "DATA_CORRUPTION", "LOYALTY_ENGINE_BUG"], workerLog(current));

    const rerun = await rerunWithAudit(current.showcase, current.store, loyalty.value.taskId, "story rerun");

    assert.ok(rerun.ok && rerun.value.kind === "enqueued", JSON.stringify(rerun));

    const audit = await current.store.pool.query("SELECT source_task_id FROM acme_reruns WHERE new_task_id = $1", [
      rerun.value.newTaskId,
    ]);

    assert.equal(audit.rows.length, 1, "the audit row commits with the rerun");

    const replay = await rerunWithAudit(current.showcase, current.store, loyalty.value.taskId, "story rerun");

    assert.ok(replay.ok && replay.value.kind === "keyReplay", JSON.stringify(replay));
  });

  test("a crash of an export child is WORKER_CRASHED, and the retry completes it", { timeout: TEST_TIMEOUT_MS }, async () => {
    const current = ready();

    if (current === "skipped") {
      return;
    }

    const exportId = idWhere(uniqueId("export"), (id) => draw(CHAOS_EXPORT_CRASH_RATE, [id, "export"]));
    const sent = await current.showcase.app.send(flakyExport, { exportId });

    assert.ok(sent.ok, "the export sends");

    const read = await sent.value.get({ timeoutMs: RESULT_WAIT_MS });

    assert.ok(read.ok && read.value.kind === "succeeded", `${JSON.stringify(read)}\n${workerLog(current)}`);

    const info = await sent.value.info();

    assert.ok(info.ok);
    assert.deepEqual(
      info.value.attempts.map((attempt) => (attempt.errorCode.kind === "recorded" ? attempt.errorCode.value : "none")).toReversed(),
      ["WORKER_CRASHED", "none"],
    );
  });

  test("an operator cancel ends a pending task as TASK_CANCELLED", { timeout: TEST_TIMEOUT_MS }, async () => {
    const current = ready();

    if (current === "skipped") {
      return;
    }

    const sent = await current.showcase.app.send(
      warmCacheEdge,
      { campaignId: uniqueId("cancel") },
      { delaySeconds: CANCEL_DELAY_SECONDS },
    );

    assert.ok(sent.ok, "the task sends");

    const cancelled = await cancelTask(current.showcase.app, { taskId: sent.value.taskId, policy: "pendingOrClaimedOnly" });

    assert.ok(cancelled.ok && cancelled.value.wasStatus === "PENDING", JSON.stringify(cancelled));

    const read = await sent.value.get({ timeoutMs: RESULT_WAIT_MS });

    assert.ok(read.ok);
    assert.equal(failedCode(read.value), "TASK_CANCELLED");
  });

  test("the report reads the database, the worker and the failed tasks", { timeout: TEST_TIMEOUT_MS }, async () => {
    const current = ready();

    if (current === "skipped") {
      return;
    }

    const lines = await reportLines(current.showcase.app, 5);
    const text = lines.join("\n");

    assert.match(text, /^database: \d+\.\d ms$/mu);
    assert.match(text, /^workers: [1-9]\d* live$/mu);
    assert.match(text, /^failed tasks: \d+ in all/mu);
    assert.match(text, /^workflows \(the newest \d+\): (?!the read failed)/mu);
  });
});
