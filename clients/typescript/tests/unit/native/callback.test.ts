// The JavaScript side of the callback primitive: the wrapper settles each kind of end once, and the
// table keeps and gives back thrown values within its bound.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  MAX_KEPT_THROWN,
  keepThrown,
  settlingCallback,
  takeThrown,
  thrownFacts,
} from "../../../horsies/src/native/call/callback.ts";
import type { CallbackSettlement } from "../../../horsies/src/native/call/callback.ts";

/** A settlement that records each settle call. */
function recordingSettlement(): { readonly settlement: CallbackSettlement; readonly settled: string[] } {
  const settled: string[] = [];

  return {
    settled,
    settlement: {
      returned(callId, value) {
        settled.push(`returned ${callId} ${String(value)}`);
      },
      threw(callId, cause) {
        settled.push(`threw ${callId} ${thrownFacts(cause).message}`);
      },
    },
  };
}

/** Wait until the microtasks of a settled Promise ran. */
async function afterMicrotasks(): Promise<void> {
  await new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
}

describe("settlingCallback", () => {
  test("settles a sync return as returned", async () => {
    const { settlement, settled } = recordingSettlement();

    settlingCallback((argument: string) => `pong:${argument}`, settlement)(1n, "ping");
    await afterMicrotasks();
    assert.deepEqual(settled, ["returned 1 pong:ping"]);
  });

  test("settles a sync throw as threw, at once", () => {
    const { settlement, settled } = recordingSettlement();

    settlingCallback((): string => {
      throw new TypeError("not a spec");
    }, settlement)(2n, "ping");
    assert.deepEqual(settled, ["threw 2 not a spec"]);
  });

  test("settles a resolved Promise as returned", async () => {
    const { settlement, settled } = recordingSettlement();

    settlingCallback(async (argument: string) => `later:${argument}`, settlement)(3n, "ping");
    await afterMicrotasks();
    assert.deepEqual(settled, ["returned 3 later:ping"]);
  });

  test("settles a rejected Promise as threw", async () => {
    const { settlement, settled } = recordingSettlement();

    settlingCallback(async (): Promise<string> => {
      throw new RangeError("no seat");
    }, settlement)(4n, "ping");
    await afterMicrotasks();
    assert.deepEqual(settled, ["threw 4 no seat"]);
  });
});

describe("thrownFacts", () => {
  test("an Error gives its name and message", () => {
    assert.deepEqual(thrownFacts(new TypeError("x is not a function")), {
      name: "TypeError",
      message: "x is not a function",
    });
  });

  test("a value that is not an Error gives its text", () => {
    assert.deepEqual(thrownFacts("plain text"), { name: "NotAnError", message: "plain text" });
  });

  test("a value whose text throws gives a fixed text", () => {
    const value = {
      toString(): string {
        throw new Error("no text");
      },
    };

    assert.deepEqual(thrownFacts(value), { name: "NotAnError", message: "a thrown value with no text" });
  });
});

describe("the kept thrown values", () => {
  test("a kept value is given back once", () => {
    const value = new Error("kept");

    keepThrown(10n, value);
    assert.deepEqual(takeThrown(10n), { kind: "kept", value });
    assert.deepEqual(takeThrown(10n), { kind: "missing" });
  });

  test("at the bound the oldest kept value is dropped", () => {
    const first = 1_000_000n;

    for (let offset = 0n; offset <= BigInt(MAX_KEPT_THROWN); offset += 1n) {
      keepThrown(first + offset, offset);
    }

    assert.deepEqual(takeThrown(first), { kind: "missing" });
    assert.deepEqual(takeThrown(first + BigInt(MAX_KEPT_THROWN)), {
      kind: "kept",
      value: BigInt(MAX_KEPT_THROWN),
    });

    for (let offset = 1n; offset < BigInt(MAX_KEPT_THROWN); offset += 1n) {
      takeThrown(first + offset);
    }
  });
});
