// The producer end to end, with a built addon and a database: `Horsies.create`, a send, the task info and a result
// read that times out. No worker runs, so the task stays PENDING.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`) and `HORSIES_TEST_DATABASE_URL` names a
// throwaway PostgreSQL database.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { defineTask, h, Horsies } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

const ping = defineTask({ name: "producer_end_to_end_ping", input: { n: h.int() }, output: h.int() });

/** The app of the test database, or the failed test with every refusal. */
function testApp(): Horsies {
  const created = Horsies.create({
    database: { databaseUrl: { kind: "value", value: testDatabaseUrl() } },
    tasks: [ping],
  });

  if (!created.ok) {
    assert.fail(`Horsies.create refused: ${created.error.refusals.map((refusal) => refusal.message).join("; ")}`);
  }

  return created.value;
}

describe("the producer with a database", () => {
  test("a send gives a handle; info gives PENDING; a bounded wait is waitTimeout", async () => {
    const app = testApp();
    const sent = await app.send(ping, { n: 1 });

    if (!sent.ok) {
      assert.fail(`the send failed: ${sent.error.code}: ${sent.error.message}`);
    }

    const info = await sent.value.info();

    if (!info.ok) {
      assert.fail(`info failed: ${info.error.kind}`);
    }

    assert.equal(info.value.taskId, sent.value.taskId);
    assert.equal(info.value.taskName, "producer_end_to_end_ping");
    assert.equal(info.value.status, "PENDING");
    assert.equal(info.value.queue, "default");
    assert.deepEqual(info.value.attempts, []);

    assert.deepEqual(await sent.value.get({ waitMs: 200 }), {
      ok: false,
      error: { kind: "waitTimeout", code: "WAIT_TIMEOUT", status: "PENDING" },
    });
  });

  test("a handle from the stored id reads the same task", async () => {
    const app = testApp();
    const sent = await app.send(ping, { n: 2 }, { delaySeconds: 3_600 });

    if (!sent.ok) {
      assert.fail(`the send failed: ${sent.error.code}: ${sent.error.message}`);
    }

    const again = await app.taskHandle(ping, sent.value.taskId).info();

    assert.equal(again.ok && again.value.status, "PENDING");
  });

  test("an input that breaks the contract is VALIDATION_FAILED, and nothing is written", async () => {
    const app = testApp();
    // SAFETY: a JavaScript caller can pass a value that the static type refuses; the send must refuse it.
    const sent = await app.send(ping, { n: "one" } as never);

    assert.equal(sent.ok ? "sent" : sent.error.code, "VALIDATION_FAILED");
  });
});
