// One test per outcome of the check view (the value that a user-side check sees at encode), and the order in which
// the writer runs a check.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { checkView } from "../../../horsies/src/codec/check-view.ts";
import type { CheckFailure } from "../../../horsies/src/codec/conversion.ts";
import { isNumber, isString } from "../../../horsies/src/codec/values.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import {
  checkedKind,
  conversionKind,
  INT_KIND,
  type Kind,
  objectKind,
  STRING_KIND,
} from "../../../horsies/src/wire/kinds.ts";
import { err, ok } from "../../../horsies/src/wire/result.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";

const NOT_A_NUMBER: CheckFailure = { issues: [{ path: "$", code: "invalid_type" }] };

function viewOf<V>(kind: Kind, value: V): unknown {
  const viewed = checkView(kind, value, "$", 0);

  assert.ok(viewed.ok);

  return viewed.value;
}

describe("checkView", () => {
  test("a leaf and null stay as they are", () => {
    assert.equal(viewOf(kindOf(h.string()), "a"), "a");
    assert.equal(viewOf(kindOf(h.int().nullable()), null), null);
  });

  test("a left-out defaulted member is filled with its default; a given one stays", () => {
    const kind = kindOf(h.object({ id: h.int(), retries: h.int().default(3), tags: h.array(h.string()).default([]) }));

    assert.deepEqual(viewOf(kind, { id: 1 }), { id: 1, retries: 3, tags: [] });
    assert.deepEqual(viewOf(kind, { id: 1, retries: 5, tags: ["a"] }), { id: 1, retries: 5, tags: ["a"] });
  });

  test("a left-out optional member stays absent", () => {
    assert.deepEqual(viewOf(kindOf(h.object({ note: h.string().optional() })), {}), {});
  });

  test("the defaults are filled at every depth: array items, record values, tuple items", () => {
    const line = h.object({ sku: h.string(), quantity: h.int().default(1) });
    const kind = kindOf(h.object({ lines: h.array(line), bySku: h.record(line), pair: h.tuple([line, h.int()]) }));
    const value = { lines: [{ sku: "a" }], bySku: { a: { sku: "a" } }, pair: [{ sku: "b" }, 2] };

    assert.deepEqual(viewOf(kind, value), {
      lines: [{ sku: "a", quantity: 1 }],
      bySku: { a: { sku: "a", quantity: 1 } },
      pair: [{ sku: "b", quantity: 1 }, 2],
    });
  });

  test("a tagged union fills the defaults of the variant that its tag names", () => {
    const kind = kindOf(h.union("kind", [h.object({ kind: h.literal("a"), size: h.int().default(2) })]));

    assert.deepEqual(viewOf(kind, { kind: "a" }), { kind: "a", size: 2 });
  });

  test("a conversion's value is the user's value: it stays as it is", () => {
    const kind = conversionKind(INT_KIND, (stored) => ok(stored), (value) => ok(value));
    const value = { cents: 5 };

    assert.equal(viewOf(kind, value), value);
  });

  test("CheckFailed: a default that does not read back in the worker form", () => {
    const refusesOnRead = conversionKind(
      INT_KIND,
      () => err(NOT_A_NUMBER),
      (value) => (isNumber(value) ? ok(value) : err(NOT_A_NUMBER)),
    );

    const kind = objectKind([
      {
        name: "n",
        kind: refusesOnRead,
        presence: { mode: "defaulted", fallback: { state: "stored", text: encodedInt(1) } },
      },
    ]);

    const viewed = checkView(kind, {}, "$", 0);

    assert.deepEqual(viewed.ok ? "" : [viewed.error.refusal, viewed.error.path], ["CheckFailed", "$.n"]);
  });

  test("a default that the writer refused is a defect: checkKind refuses that kind", () => {
    const kind = kindOf(h.object({ ratio: h.float().default(Number.NaN) }));

    assert.throws(() => checkView(kind, {}, "$", 0), TypeError);
  });
});

describe("the writer runs a check after its kind", () => {
  test("a check that would throw on another structure does not run: the writer refuses the value first", () => {
    const trimmed = checkedKind(STRING_KIND, (value) => {
      if (!isString(value)) {
        throw new TypeError("the check ran on a value that is not a string");
      }

      return ok(true);
    });

    assert.deepEqual(encodeValue({ node: "union", members: [trimmed, INT_KIND] }, 5), { ok: true, value: "5" });
    assert.equal(encodeValue(trimmed, 5).ok ? "" : "refused", "refused");
  });

  test("a check sees the value with its defaults filled; the text holds only the given keys", () => {
    let seen: unknown;

    const kind = checkedKind(kindOf(h.object({ id: h.int(), retries: h.int().default(3) })), (value) => {
      seen = value;

      return ok(true);
    });

    assert.deepEqual(encodeValue(kind, { id: 1 }), { ok: true, value: '{"id":1}' });
    assert.deepEqual(seen, { id: 1, retries: 3 });
  });
});

function encodedInt(value: number) {
  const written = encodeValue(INT_KIND, value);

  assert.ok(written.ok);

  return written.value;
}
