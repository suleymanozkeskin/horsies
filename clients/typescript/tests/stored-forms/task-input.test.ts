// task_input.json: each value encodes to its text byte for byte, each text decodes to the same value, each refused
// value and each typed refused text is refused with its named kind. The named cases are separate tests beside the
// loops, so a change of a loop's filter cannot drop one.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { jsonTextCheckedByCore } from "../../horsies/src/codec/json-text.ts";
import { decodeParameters } from "../../horsies/src/codec/reader.ts";
import type { DecodeRefusalKind, EncodeRefusalKind } from "../../horsies/src/codec/refusal.ts";
import { encodeParameters } from "../../horsies/src/codec/writer.ts";
import { h, parametersKind } from "../../horsies/src/wire/builders.ts";
import type { AnyWire } from "../../horsies/src/wire/infer.ts";
import type { ObjectKind } from "../../horsies/src/wire/kinds.ts";
import { sameValue, show } from "../support/values.ts";
import { type ParameterCase, parameterCases, rowNamed, taskInputFile } from "./vectors.ts";

const taskInput = taskInputFile();

function kindOfCase(parameterCase: ParameterCase): ObjectKind {
  return parametersKind(Object.fromEntries(parameterCase.parameters.map(({ name, wire }) => [name, wire])));
}

/** The vector `name` under one parameter `value` of `wire`: it encodes to its text and decodes to `value`. */
function assertOneValueVector<V>(name: string, wire: AnyWire, value: V): void {
  const vector = rowNamed(taskInput.vectors, name);
  const parameters = parametersKind({ value: wire });
  const written = encodeParameters(parameters, { value });

  assert(written.ok, show(written));
  assert.equal(written.value, vector.text);

  const decoded = decodeParameters(parameters, jsonTextCheckedByCore(vector.text), "worker");

  assert(decoded.ok, show(decoded));
  assert(sameValue(decoded.value, { value }), show(decoded.value));
}

describe("task_input.json vectors", () => {
  for (const vector of taskInput.vectors) {
    const reason = vector.typescript_cannot_express;

    if (reason !== undefined) {
      test(`${vector.name}: skipped, ${reason}`, { skip: reason }, () => {});
      continue;
    }

    // A `typescript_drift` row pins the text that this client writes in place of Python's; Python's text still reads.
    const writtenText = vector.typescript_drift?.text ?? vector.text;

    for (const parameterCase of parameterCases(vector.parameters)) {
      const parameters = kindOfCase(parameterCase);

      test(`${vector.name} [${parameterCase.label}] encodes to its text`, () => {
        const written = encodeParameters(parameters, parameterCase.value);

        assert(written.ok, show(written));
        assert.equal(written.value, writtenText);
      });

      test(`${vector.name} [${parameterCase.label}] decodes to its value`, () => {
        const decoded = decodeParameters(parameters, jsonTextCheckedByCore(vector.text), "worker");

        assert(decoded.ok, show(decoded));
        assert(sameValue(decoded.value, parameterCase.value), show(decoded.value));
      });
    }
  }
});

describe("task_input.json named cases", () => {
  test("1e-05: f64_exponent_minus_5", () => {
    assertOneValueVector("f64_exponent_minus_5", h.float(), 1e-5);
    assert.equal(rowNamed(taskInput.vectors, "f64_exponent_minus_5").text, '{"value":1e-05}');
  });

  test("1e+16: f64_exponent_plus_16", () => {
    assertOneValueVector("f64_exponent_plus_16", h.float(), 1e16);
    assert.equal(rowNamed(taskInput.vectors, "f64_exponent_plus_16").text, '{"value":1e+16}');
  });

  test("123.0: f64_integral", () => {
    assertOneValueVector("f64_integral", h.float(), 123);
    assert.equal(rowNamed(taskInput.vectors, "f64_integral").text, '{"value":123.0}');
  });

  test("-0.0: f64_negative_zero", () => {
    assertOneValueVector("f64_negative_zero", h.float(), -0);
    assert.equal(rowNamed(taskInput.vectors, "f64_negative_zero").text, '{"value":-0.0}');
  });

  test("json_float_exponent: an h.json() number that is not a safe integer keeps Python's float text", () => {
    assertOneValueVector("json_float_exponent", h.json(), { f: 1e-5, g: 1e16 });
    assert.equal(rowNamed(taskInput.vectors, "json_float_exponent").text, '{"value":{"f":1e-05,"g":1e+16}}');
  });

  test("big_int_above_u128: h.bigint() with 39 digits", () => {
    const value = 2n ** 128n;

    assert.equal(String(value).length, 39);
    assertOneValueVector("big_int_above_u128", h.bigint(), value);
  });

  test("map_integer_like_keys: this client writes the JavaScript key order; Python's text reads", () => {
    const vector = rowNamed(taskInput.vectors, "map_integer_like_keys");

    assert.equal(vector.text, '{"value":{"b":1,"10":2,"2":3}}');
    assert.deepEqual(vector.typescript_drift, {
      text: '{"value":{"2":3,"10":2,"b":1}}',
      finding: "integer-like-record-keys",
    });
  });
});

const REFUSED_VALUE_KIND: Readonly<Record<string, EncodeRefusalKind>> = {
  f64_nan: "NonFiniteFloat",
  f64_infinity: "NonFiniteFloat",
  f64_negative_infinity: "NonFiniteFloat",
  json_engine_key: "EngineKey",
};

describe("task_input.json refused values", () => {
  for (const row of taskInput.refused_values) {
    for (const parameterCase of parameterCases(row.parameters)) {
      test(`${row.name} [${parameterCase.label}] is refused with its named kind`, () => {
        const expected = REFUSED_VALUE_KIND[row.name];

        assert(expected !== undefined, `the row ${row.name} has no expected refusal kind in this test`);

        const written = encodeParameters(kindOfCase(parameterCase), parameterCase.value);

        assert(!written.ok, "encoded a refused value");
        assert.equal(written.error.refusal, expected);
      });
    }
  }
});

const TYPED_REFUSAL_KIND: Readonly<Record<string, DecodeRefusalKind>> = {
  string_for_integer: "WrongType",
  float_for_integer: "FloatForInteger",
  unknown_member: "UnknownMember",
  unknown_enum_value: "NotAMember",
};

describe("task_input.json typed refused texts", () => {
  for (const row of taskInput.typed_refused_texts) {
    for (const parameterCase of parameterCases(row.parameters)) {
      test(`${row.name} [${parameterCase.label}] is refused with its named kind`, () => {
        const expected = TYPED_REFUSAL_KIND[row.name];

        assert(expected !== undefined, `the row ${row.name} has no expected refusal kind in this test`);

        const decoded = decodeParameters(kindOfCase(parameterCase), jsonTextCheckedByCore(row.text), "worker");

        assert(!decoded.ok && decoded.error.kind === "decodeRefused", "decoded a refused text");
        assert.equal(decoded.error.refusal, expected);
      });
    }
  }
});
