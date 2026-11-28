// The callback primitive with a built addon: a core thread calls a JavaScript function and gets its
// answer, its throw, or the refusal of an answer that does not read.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { addonSettlement, callAddon, settlingCallback, takeThrown } from "../../../horsies/src/native/call/callback.ts";
import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { probeAddon } from "./callback-probe.ts";
import type { CallbackProbe, ProbeWait } from "./callback-probe.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** Call `callback` once from a core thread through the probe of the addon. */
async function probe(
  callback: (argument: string) => unknown,
  argument: string,
  wait: ProbeWait = { kind: "noBound" },
): Promise<CallbackProbe> {
  return envelopeValue(await probeAddon().probeCallback(settlingCallback(callback, addonSettlement), argument, wait));
}

describe("the callback primitive with a built addon", () => {
  test("a sync answer reaches the core", async () => {
    assert.deepEqual(await probe((argument) => `pong:${argument}`, "ping"), {
      kind: "answered",
      answer: "pong:ping",
    });
  });

  test("an async answer reaches the core", async () => {
    assert.deepEqual(await probe(async (argument) => `later:${argument}`, "ping"), {
      kind: "answered",
      answer: "later:ping",
    });
  });

  test("a throw reaches the core by name and message, and the value stays in the package", async () => {
    const thrown = new RangeError("no seat");

    const ended = await probe(() => {
      throw thrown;
    }, "ping");

    assert.equal(ended.kind, "threw");

    if (ended.kind !== "threw") {
      return;
    }

    assert.deepEqual({ name: ended.name, message: ended.message }, { name: "RangeError", message: "no seat" });
    assert.deepEqual(takeThrown(ended.callId), { kind: "kept", value: thrown });
  });

  test("an answer that does not read as the answer type is refused", async () => {
    const ended = await probe(() => 42, "ping");

    assert.equal(ended.kind, "answerRefused");
  });

  test("a call within its bound gets its answer", async () => {
    assert.deepEqual(await probe((argument) => `pong:${argument}`, "ping", { kind: "within", millis: 5_000 }), {
      kind: "answered",
      answer: "pong:ping",
    });
  });

  test("a function that never settles ends the call with no answer at its bound", async () => {
    const ended = await probe(() => new Promise<string>(() => undefined), "ping", { kind: "within", millis: 50 });

    assert.deepEqual(ended, { kind: "noAnswer" });
  });

  test("a settle call after the bound finds no call", async () => {
    let lateCallId: bigint | undefined;

    const ended = envelopeValue(
      await probeAddon().probeCallback(
        (callId) => {
          lateCallId = callId;
        },
        "ping",
        { kind: "within", millis: 50 },
      ),
    );

    assert.deepEqual(ended, { kind: "noAnswer" });
    assert.notEqual(lateCallId, undefined);

    if (lateCallId === undefined) {
      return;
    }

    assert.equal(envelopeValue(callAddon().settleCallbackReturned(lateCallId, "late")), "unknownCall");
  });

  test("a call with no bound waits for a slow answer", async () => {
    const slow = (argument: string): Promise<string> =>
      new Promise((resolve) => {
        setTimeout(() => {
          resolve(`slow:${argument}`);
        }, 200);
      });

    assert.deepEqual(await probe(slow, "ping", { kind: "noBound" }), { kind: "answered", answer: "slow:ping" });
  });
});
