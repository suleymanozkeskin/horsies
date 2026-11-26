// The JavaScript side of the callback primitive: the wrapper settles each kind of end once, and the
// table keeps and gives back thrown values within its bound.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  MAX_KEPT_THROWN,
  keepThrown,
  settlementVia,
  settlingCallback,
  takeThrown,
  takingCallback,
} from "../../../horsies/src/native/call/callback.ts";
import type { CallbackSettlement, SettleAddon } from "../../../horsies/src/native/call/callback.ts";
import type { NativeEnvelope } from "../../../horsies/src/native/call/envelope.ts";
import { thrownFacts } from "../../../horsies/src/native/call/thrown-facts.ts";
import type { CoreDefect } from "../../../horsies/src/thrown-errors.ts";

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

describe("takingCallback", () => {
  test("a call that the core still waits for is taken first, then run", () => {
    const order: string[] = [];

    const wrapper = takingCallback(
      (callId: bigint, argument: string) => order.push(`run ${callId} ${argument}`),
      (callId) => {
        order.push(`take ${callId}`);

        return "settled";
      },
    );

    wrapper(4n, "a");

    assert.deepEqual(order, ["take 4", "run 4 a"]);
  });

  test("a call that the core no longer waits for, or a failed take call, does not run", () => {
    for (const end of ["unknownCall", "notSettled"] as const) {
      let runs = 0;

      const wrapper = takingCallback(
        () => {
          runs += 1;
        },
        () => end,
      );

      wrapper(5n, "a");

      assert.equal(runs, 0);
    }
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

  test("an Error whose message getter throws gives a fixed text, with no throw", () => {
    const error = new Error("hidden");

    Object.defineProperty(error, "message", {
      get(): string {
        throw new Error("no message");
      },
    });

    assert.deepEqual(thrownFacts(error), { name: "Error", message: "a thrown value with no readable text" });
  });

  test("an Error whose message is a number gives the text of the number", () => {
    const error = new Error("replaced");

    Object.defineProperty(error, "message", { value: 42 });

    assert.deepEqual(thrownFacts(error), { name: "Error", message: "42" });
  });

  test("a revoked Proxy gives a fixed text, with no throw", () => {
    const { proxy, revoke } = Proxy.revocable({}, {});

    revoke();
    assert.deepEqual(thrownFacts(proxy), { name: "NotAnError", message: "a thrown value with no readable text" });
  });

  test("a value whose text throws gives a fixed text", () => {
    const value = {
      toString(): string {
        throw new Error("no text");
      },
    };

    assert.deepEqual(thrownFacts(value), { name: "NotAnError", message: "a thrown value with no readable text" });
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

/** A settle addon whose two calls answer `answer`, or throw it when it is an `Error`. */
function fakeSettleAddon(answer: NativeEnvelope<"settled" | "unknownCall"> | Error): SettleAddon {
  const settle = (): NativeEnvelope<"settled" | "unknownCall"> => {
    if (answer instanceof Error) {
      throw answer;
    }

    return answer;
  };

  return { settleCallbackReturned: settle, settleCallbackThrew: settle };
}

describe("settlementVia", () => {
  test("a settled throw keeps its value for the operation", () => {
    const defects: CoreDefect[] = [];

    const settlement = settlementVia(() => fakeSettleAddon({ kind: "value", value: "settled" }), (defect) => {
      defects.push(defect);
    });

    const thrown = new Error("kept for the operation");

    settlement.threw(21n, thrown);
    assert.deepEqual(takeThrown(21n), { kind: "kept", value: thrown });
    assert.deepEqual(defects, []);
  });

  test("a throw that no core call waits for any more is dropped", () => {
    const settlement = settlementVia(() => fakeSettleAddon({ kind: "value", value: "unknownCall" }), () => {});

    settlement.threw(22n, new Error("late"));
    assert.deepEqual(takeThrown(22n), { kind: "missing" });
  });

  test("a settle call that answers a thrown code is a defect, and the value is dropped", () => {
    const defects: CoreDefect[] = [];

    const settlement = settlementVia(
      () => fakeSettleAddon({ kind: "thrown", thrown: { code: "CLIENT_DEFECT", subject: "callId", rule: "a u64" } }),
      (defect) => {
        defects.push(defect);
      },
    );

    settlement.threw(23n, new Error("x"));
    assert.deepEqual(takeThrown(23n), { kind: "missing" });
    assert.deepEqual(
      defects.map((defect) => defect.code),
      ["CLIENT_DEFECT"],
    );
  });

  test("a settle call that throws is a ClientDefect, with no throw out of the settlement", () => {
    const defects: CoreDefect[] = [];

    const settlement = settlementVia(() => fakeSettleAddon(new TypeError("A bigint was expected")), (defect) => {
      defects.push(defect);
    });

    settlement.returned(24n, "answer");
    assert.deepEqual(
      defects.map((defect) => [defect.code, defect.message]),
      [["CLIENT_DEFECT", "a defect of the horsies package: settleCallback: the settle call threw TypeError: A bigint was expected"]],
    );
  });
});
