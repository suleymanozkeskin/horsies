// Python's float text. The vectors that pin these texts and the corpus of 10,000 doubles are in tests/stored-forms/.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { pythonFloatText } from "../../../horsies/src/codec/float-text.ts";

// Python's float texts that the stored-form vectors name.
const NAMED_FLOATS: readonly (readonly [value: number, text: string])[] = [
  [1e-5, "1e-05"],
  [1e16, "1e+16"],
  [123, "123.0"],
  [-0, "-0.0"],
];

describe("Python's float text", () => {
  for (const [value, text] of NAMED_FLOATS) {
    test(text, () => {
      assert.equal(pythonFloatText(value), text);
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
});
