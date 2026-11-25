import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { pythonFloatText } from "../../../horsies/src/codec/float-text.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { IntegralFloat, readTree } from "../../../horsies/src/codec/number-text.ts";
import { corpusLines, runtimeLabel, STORED_FORMS_DIR } from "./support/files.ts";

// Python's float texts that the stored-form vectors name. Each one is a vector of task_input.json.
const NAMED_FLOATS: readonly (readonly [vector: string, value: number, text: string])[] = [
  ["f64_exponent_minus_5", 1e-5, "1e-05"],
  ["f64_exponent_plus_16", 1e16, "1e+16"],
  ["f64_integral", 123, "123.0"],
  ["f64_negative_zero", -0, "-0.0"],
];

const FLOAT_CORPUS_SIZE = 10_000;

function vectorText(name: string): string {
  const document: { vectors: readonly { name: string; text: string }[] } = JSON.parse(
    readFileSync(`${STORED_FORMS_DIR}task_input.json`, "utf8"),
  );

  const row = document.vectors.find((vector) => vector.name === name);

  assert(row !== undefined, `task_input.json has the vector ${name}`);

  return row.text;
}

describe("Python's float text", () => {
  for (const [vector, value, text] of NAMED_FLOATS) {
    test(`${vector}: ${text}`, () => {
      assert.equal(pythonFloatText(value), text);
      assert.equal(vectorText(vector), `{"value":${text}}`);
    });
  }

  test("fixed notation from 1e-04 up to below 1e+16", () => {
    assert.equal(pythonFloatText(0.0001), "0.0001");
    assert.equal(pythonFloatText(1e15), "1000000000000000.0");
    assert.equal(pythonFloatText(0.1), "0.1");
    assert.equal(pythonFloatText(-2.5), "-2.5");
  });

  test("scientific notation outside that range, two exponent digits at least", () => {
    assert.equal(pythonFloatText(1.5e-7), "1.5e-07");
    assert.equal(pythonFloatText(1.2345e22), "1.2345e+22");
    assert.equal(pythonFloatText(5e-324), "5e-324");
    assert.equal(pythonFloatText(-1.7976931348623157e308), "-1.7976931348623157e+308");
  });

  test("NaN and the infinities are a defect of the caller: TypeError", () => {
    for (const value of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
      assert.throws(() => pythonFloatText(value), TypeError);
    }
  });

  test(`corpus: ${FLOAT_CORPUS_SIZE} doubles against Python's repr, and each text reads back bit for bit`, () => {
    const bits = new DataView(new ArrayBuffer(8));
    const mismatches: string[] = [];
    const lines = corpusLines("floats.txt");

    assert.equal(lines.length, FLOAT_CORPUS_SIZE);

    for (const line of lines) {
      const space = line.indexOf(" ");

      bits.setBigUint64(0, BigInt(`0x${line.slice(0, space)}`));
      const value = bits.getFloat64(0);
      const python = line.slice(space + 1);
      const read = readTree(jsonTextCheckedByCore(python));
      const readValue = read instanceof IntegralFloat ? read.value : read;

      if (pythonFloatText(value) !== python || !Object.is(readValue, value)) {
        mismatches.push(line);
      }
    }

    assert.deepEqual(mismatches, [], `${runtimeLabel()}: ${mismatches.length} mismatches`);
  });
});
