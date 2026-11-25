// The callback primitive with a built addon: a core thread calls a JavaScript function and gets its
// answer, its throw, or the refusal of an answer that does not read.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { addonSettlement, settlingCallback, takeThrown } from "../../../horsies/src/native/call/callback.ts";
import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { probeAddon } from "./callback-probe.ts";
import type { CallbackProbe } from "./callback-probe.ts";

/** Call `callback` once from a core thread through the probe of the addon. */
async function probe(callback: (argument: string) => unknown, argument: string): Promise<CallbackProbe> {
  return envelopeValue(await probeAddon().probeCallback(settlingCallback(callback, addonSettlement), argument));
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
});
