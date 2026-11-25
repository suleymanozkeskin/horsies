// The stored-form vectors that this client encodes and decodes itself. Each in-scope vector encodes to its text byte
// for byte, and its text decodes to the same value.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { isTreeObject, readTree, type TreeValue } from "../../../horsies/src/codec/number-text.ts";
import { decodeNode } from "../../../horsies/src/codec/reader.ts";
import type { DecodeRefusalKind, EncodeRefusalKind } from "../../../horsies/src/codec/refusal.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { kindOf } from "../../../horsies/src/wire/builders.ts";
import { type ParameterCase, parameterCases, sameValue, show, taskInputFile } from "./support/vectors.ts";

const taskInput = taskInputFile();

/** The stored text of a parameter list: one member per parameter, in declaration order. */
function encodeParameterCase(parameterCase: ParameterCase): string {
  const members = parameterCase.parameters.map(({ name, wire }) => {
    const written = encodeValue(kindOf(wire), parameterCase.value[name]);

    assert(written.ok, written.ok ? "" : `${name}: ${written.error.kind} at ${written.error.path}`);

    return `${JSON.stringify(name)}:${written.value}`;
  });

  return `{${members.join(",")}}`;
}

/** The value of each parameter of a stored text. */
function decodeParameterCase(
  parameterCase: ParameterCase,
  text: string,
): { readonly value: Readonly<Record<string, unknown>> } | { readonly refused: DecodeRefusalKind } {
  const tree = readTree(jsonTextCheckedByCore(text));

  assert(isTreeObject(tree), "the stored input is an object");

  const value: Record<string, unknown> = {};

  for (const { name, wire } of parameterCase.parameters) {
    const member: TreeValue | undefined = tree[name];

    assert(member !== undefined, `the stored input has the member ${name}`);

    const decoded = decodeNode(kindOf(wire), member, `$.${name}`);

    if (!decoded.ok) {
      return { refused: decoded.error.kind };
    }

    value[name] = decoded.value;
  }

  return { value };
}

describe("task_input.json vectors", () => {
  for (const vector of taskInput.vectors) {
    for (const parameterCase of parameterCases(vector.parameters)) {
      test(`${vector.name} [${parameterCase.label}] encodes to its text`, () => {
        assert.equal(encodeParameterCase(parameterCase), vector.text);
      });

      test(`${vector.name} [${parameterCase.label}] decodes to its value`, () => {
        const decoded = decodeParameterCase(parameterCase, vector.text);

        assert("value" in decoded, show(decoded));

        for (const { name } of parameterCase.parameters) {
          assert(sameValue(decoded.value[name], parameterCase.value[name]), name);
        }
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
        const [parameter] = parameterCase.parameters;

        assert(parameter !== undefined);

        const written = encodeValue(kindOf(parameter.wire), parameterCase.value[parameter.name]);

        assert(!written.ok, "encoded a refused value");
        assert.equal(written.error.kind, REFUSED_VALUE_KIND[row.name]);
      });
    }
  }
});

const TYPED_REFUSAL_KIND: Readonly<Record<string, DecodeRefusalKind>> = {
  string_for_integer: "WrongType",
  float_for_integer: "FloatForInteger",
};

describe("task_input.json typed refused texts", () => {
  for (const row of taskInput.typed_refused_texts) {
    for (const parameterCase of parameterCases(row.parameters)) {
      const expected = TYPED_REFUSAL_KIND[row.name];

      if (expected === undefined) {
        continue;
      }

      test(`${row.name} [${parameterCase.label}] is refused with ${expected}`, () => {
        assert.deepEqual(decodeParameterCase(parameterCase, row.text), { refused: expected });
      });
    }
  }
});
