// One test per outcome of the read-back, and of the conversion and check nodes that a schema front end fills.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { CheckFailure } from "../../../horsies/src/codec/conversion.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { encodeParametersWithReadBack, encodeWithReadBack } from "../../../horsies/src/codec/read-back.ts";
import { decodeValue } from "../../../horsies/src/codec/reader.ts";
import { refusalText } from "../../../horsies/src/codec/refusal.ts";
import { isNumber } from "../../../horsies/src/codec/values.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h, parametersKind } from "../../../horsies/src/wire/builders.ts";
import { checkKind } from "../../../horsies/src/wire/check.ts";
import { checkedKind, conversionKind, INT_KIND, type Kind } from "../../../horsies/src/wire/kinds.ts";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";

const TOO_LARGE: CheckFailure = { issues: [{ path: "$", code: "too_big" }] };
const NOT_A_NUMBER: CheckFailure = { issues: [{ path: "$", code: "invalid_type" }] };

function atMost<V>(limit: number): (value: V) => Result<true, CheckFailure> {
  return (value) => (isNumber(value) && value <= limit ? ok<true>(true) : err(TOO_LARGE));
}

/** A conversion over h.int(): `decode` maps the stored number; `encode` stores the number as it is. */
function intConversion(decode: (stored: number) => number | string): Kind {
  return conversionKind(
    INT_KIND,
    (stored) => (isNumber(stored) ? ok(decode(stored)) : err(NOT_A_NUMBER)),
    (value) => (isNumber(value) ? ok(value) : err(NOT_A_NUMBER)),
  );
}

describe("read-back", () => {
  test("a value that reads back gives its text; a default left out is not a change", () => {
    const retry = kindOf(h.object({ count: h.int().default(3), name: h.string() }));
    const written = encodeWithReadBack(retry, { name: "x" });

    assert.deepEqual(written, { ok: true, value: '{"name":"x"}' });

    const parameters = parametersKind({ count: h.int().default(3), tags: h.array(h.string()).optional() });

    assert.deepEqual(encodeParametersWithReadBack(parameters, {}), { ok: true, value: "{}" });
  });

  test("an encode refusal stays the writer's refusal", () => {
    const written = encodeWithReadBack(kindOf(h.float()), Number.NaN);

    assert(!written.ok);
    assert.equal(written.error.kind, "NonFiniteFloat");
  });

  test("ReadBackRefused: the worker form refuses what the sender wrote (a check after a conversion)", () => {
    const kind = checkedKind(intConversion((stored) => stored * 10), atMost(100));
    const written = encodeWithReadBack(kind, 50);

    assert(!written.ok && written.error.kind === "ReadBackRefused");
    assert.equal(written.error.cause.kind, "CheckFailed");
  });

  test("ReadBackChanged: a conversion whose encode and decode do not invert", () => {
    const written = encodeWithReadBack(intConversion((stored) => stored + 1), 5);

    assert(!written.ok && written.error.kind === "ReadBackChanged");
    assert.equal(written.error.detail, "the read value writes another text; first difference at offset 0");
  });

  test("ReadBackChanged: the read value does not write again", () => {
    const written = encodeWithReadBack(intConversion(() => "five"), 5);

    assert(!written.ok && written.error.kind === "ReadBackChanged");
    assert.match(written.error.detail, /does not write again/);
  });
});

describe("conversions and checks", () => {
  test("the stored text is the text of the stored kind; the conversion runs in both forms", () => {
    const tenfold = intConversion((stored) => stored * 10);
    const text = jsonTextCheckedByCore("7");

    assert.deepEqual(encodeValue(tenfold, 7), { ok: true, value: "7" });
    assert.deepEqual(decodeValue(tenfold, text, "worker"), { ok: true, value: 70 });
    assert.deepEqual(decodeValue(tenfold, text, "stored"), { ok: true, value: 70 });
  });

  test("CheckFailed: a check refuses a value before an encode; the detail names the issue, not the value", () => {
    const written = encodeValue(checkedKind(INT_KIND, atMost(10)), 11);

    assert(!written.ok);
    assert.deepEqual(
      [written.error.kind, written.error.detail],
      ["CheckFailed", "the check refused the value: $ too_big"],
    );
  });

  test("CheckFailed: a conversion refuses a stored value at decode, in both forms", () => {
    const refusing = conversionKind(INT_KIND, () => err(NOT_A_NUMBER), (value) => ok(value));
    const text = jsonTextCheckedByCore("7");

    for (const form of ["worker", "stored"] as const) {
      const read = decodeValue(refusing, text, form);

      assert(!read.ok);
      assert.deepEqual(
        [read.error.kind, read.error.detail],
        ["CheckFailed", "the conversion refused the stored value: $ invalid_type"],
      );
    }
  });

  test("CheckFailed: a conversion refuses a value", () => {
    const written = encodeValue(intConversion((stored) => stored), "7");

    assert(!written.ok);
    assert.equal(written.error.kind, "CheckFailed");
  });

  test("a check runs after a decode in the worker form only", () => {
    const kind = checkedKind(INT_KIND, atMost(10));
    const text = jsonTextCheckedByCore("11");
    const worker = decodeValue(kind, text, "worker");

    assert(!worker.ok);
    assert.equal(worker.error.kind, "CheckFailed");
    assert.deepEqual(decodeValue(kind, text, "stored"), { ok: true, value: 11 });
  });

  test("checkKind walks inside a conversion and a check", () => {
    const kind = checkedKind(conversionKind(kindOf(h.union([h.int(), h.float()])), ok, ok), atMost(1));

    assert.equal(checkKind(kind)[0]?.rule, "AmbiguousUnion");
  });
});

describe("refusal text", () => {
  test("<path>: <detail> [<kind>]; a record key is <input>", () => {
    const written = encodeValue(kindOf(h.object({ a: h.record(h.int()) })), { a: { secret: "x" } });

    assert(!written.ok);
    assert.equal(refusalText(written.error), "$.a.<input>: expected a number [WrongType]");
  });
});
