// The declared custom codes that a rerun passes to the core: each listed task once, with the codes of its `errorCodes`.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { checkContractList } from "../../../horsies/src/app/contract-list.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { registeredCodesOf } from "../../../horsies/src/rerun/registered-codes.ts";
import { h } from "../../../horsies/src/wire/public.ts";

const charge = defineTask({ name: "charge", input: {}, output: h.string(), errorCodes: ["CARD_DECLINED", "PROVIDER_DOWN"] });
const noCodes = defineTask({ name: "no_codes", input: {}, output: h.string(), errorCodes: [] });
const open = defineTask({ name: "open_codes", input: {}, output: h.string() });

describe("registeredCodesOf", () => {
  test("gives each listed task in list order with its declared codes, and no code for a contract with no list", () => {
    const checked = checkContractList([charge, noCodes, open], { kind: "default" });

    assert.deepEqual(checked.refusals, []);
    assert.deepEqual(registeredCodesOf(checked.entries), {
      tasks: [
        { taskName: "charge", codes: ["CARD_DECLINED", "PROVIDER_DOWN"] },
        { taskName: "no_codes", codes: [] },
        { taskName: "open_codes", codes: [] },
      ],
    });
  });

  test("an app that lists no task gives no task", () => {
    assert.deepEqual(registeredCodesOf([]), { tasks: [] });
  });
});
