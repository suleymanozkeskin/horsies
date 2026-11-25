// The stored-form vectors that this client encodes and decodes itself. Each in-scope vector encodes to its text byte
// for byte, and its text decodes to the same value.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeParameters } from "../../../horsies/src/codec/reader.ts";
import type { DecodeRefusalKind, EncodeRefusalKind } from "../../../horsies/src/codec/refusal.ts";
import { encodeParameters } from "../../../horsies/src/codec/writer.ts";
import { parametersKind } from "../../../horsies/src/wire/builders.ts";
import type { ObjectKind } from "../../../horsies/src/wire/kinds.ts";
import { type ParameterCase, parameterCases, sameValue, show, taskInputFile } from "./support/vectors.ts";

const taskInput = taskInputFile();

function kindOfCase(parameterCase: ParameterCase): ObjectKind {
  return parametersKind(Object.fromEntries(parameterCase.parameters.map(({ name, wire }) => [name, wire])));
}

describe("task_input.json vectors", () => {
  for (const vector of taskInput.vectors) {
    if (vector.typescript_cannot_express !== undefined) {
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

const REFUSED_VALUE_KIND: Readonly<Record<string, EncodeRefusalKind>> = {
  f64_nan: "NonFiniteFloat",
  f64_infinity: "NonFiniteFloat",
  f64_negative_infinity: "NonFiniteFloat",
};

describe("task_input.json refused values", () => {
  for (const row of taskInput.refused_values) {
    for (const parameterCase of parameterCases(row.parameters)) {
      test(`${row.name} [${parameterCase.label}] is refused`, () => {
        const written = encodeParameters(kindOfCase(parameterCase), parameterCase.value);

        assert(!written.ok, "encoded a refused value");
        assert.equal(written.error.kind, REFUSED_VALUE_KIND[row.name]);
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
    const expected = TYPED_REFUSAL_KIND[row.name];

    if (expected === undefined) {
      continue;
    }

    for (const parameterCase of parameterCases(row.parameters)) {
      test(`${row.name} [${parameterCase.label}] is refused with ${expected}`, () => {
        const decoded = decodeParameters(kindOfCase(parameterCase), jsonTextCheckedByCore(row.text), "worker");

        assert(!decoded.ok, "decoded a refused text");
        assert.equal(decoded.error.kind, expected);
      });
    }
  }
});

describe("task_input.json rows that this client does not express", () => {
  test("each one names its reason", () => {
    const skipped = taskInput.vectors.filter((vector) => vector.typescript_cannot_express !== undefined);

    assert.deepEqual(
      skipped.map((vector) => vector.name),
      ["bytes_utf8"],
    );

    for (const vector of skipped) {
      assert.notEqual(vector.typescript_cannot_express?.length ?? 0, 0, vector.name);
    }
  });
});
