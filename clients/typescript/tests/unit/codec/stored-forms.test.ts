// The stored-form vectors that this client encodes and decodes itself. Each in-scope vector encodes to its text byte
// for byte, and its text decodes to the same value.

import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import { describe, test } from "node:test";

import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeParameters } from "../../../horsies/src/codec/reader.ts";
import type { DecodeRefusalKind, EncodeRefusalKind } from "../../../horsies/src/codec/refusal.ts";
import { encodeParameters } from "../../../horsies/src/codec/writer.ts";
import { parametersKind } from "../../../horsies/src/wire/builders.ts";
import type { ObjectKind } from "../../../horsies/src/wire/kinds.ts";
import { STORED_FORMS_DIR } from "./support/files.ts";
import {
  type ParameterCase,
  parameterCases,
  resultEnvelopeFile,
  sameValue,
  show,
  specDataFile,
  taskInputFile,
} from "./support/vectors.ts";

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
  json_engine_key: "EngineKey",
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


describe("the scope of this client's vector tests", () => {
  test("the core alone writes the task options and the trace carrier: those files are not read here", () => {
    const files = readdirSync(STORED_FORMS_DIR).filter((name) => name.endsWith(".json")).toSorted();

    assert.deepEqual(files, [
      "error_data.json",
      "result_envelope.json",
      "task_input.json",
      "task_options.json",
      "trace_carrier.json",
      "workflow_spec_data.json",
    ]);
  });

  test("the strict-rule texts are the core's: it refuses them before any path reaches the reader", () => {
    const coreRefusals = taskInput.refused_texts.map((row) => row.name);
    const envelopeRefusals = resultEnvelopeFile().refused_texts.filter((row) => row.refusal === "envelope");

    assert.equal(coreRefusals.length, 12);
    assert(envelopeRefusals.some((row) => row.name === "repeated_key"));
  });

  test("the core alone reads workflow spec data: its refused texts are the core's", () => {
    assert.equal(specDataFile().refused_texts.length, 4);
  });

  test("the rows that this client cannot express name their reason", () => {
    const skipped = [
      ...taskInput.vectors.filter((row) => row.typescript_cannot_express !== undefined),
      ...resultEnvelopeFile().vectors.filter((row) => row.typescript_cannot_express !== undefined),
    ];

    assert.deepEqual(
      skipped.map((row) => row.name),
      ["bytes_utf8", "ok_bytes"],
    );
  });
});
