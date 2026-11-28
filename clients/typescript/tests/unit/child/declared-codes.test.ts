// The declared codes that the plan gives the child host for each task: the `errorCodes` of the contract, in their order.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { declaredCodesOf } from "../../../horsies/src/child/serve.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

describe("the declared codes of a task in the child plan", () => {
  test("a contract with errorCodes gives them in their order", () => {
    const charge = defineTask({ name: "charge", input: {}, output: h.null(), errorCodes: ["CARD_DECLINED", "BANK_DOWN"] });

    assert.deepEqual(declaredCodesOf(charge), ["CARD_DECLINED", "BANK_DOWN"]);
  });

  test("a contract with no errorCodes gives none", () => {
    const ping = defineTask({ name: "ping", input: {}, output: h.null() });

    assert.deepEqual(declaredCodesOf(ping), []);
  });

  test("a contract with an empty errorCodes list gives none", () => {
    const quiet = defineTask({ name: "quiet", input: {}, output: h.null(), errorCodes: [] });

    assert.deepEqual(declaredCodesOf(quiet), []);
  });
});
