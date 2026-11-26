// The text rule of the addon boundary.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { checkText } from "../../../horsies/src/native/values/text.ts";

describe("checkText", () => {
  test("passes a text with a surrogate pair", () => {
    assert.deepEqual(checkText("idempotencyKey", "order-😀"), { kind: "wellFormed", text: "order-😀" });
  });

  test("names the argument of a text with a lone surrogate", () => {
    assert.deepEqual(checkText("idempotencyKey", "order-\uD800"), {
      kind: "loneSurrogate",
      argument: "idempotencyKey",
    });
  });
});
