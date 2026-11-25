// One test per outcome of the task error data writer, and the user-error vectors of result_envelope.json.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { ERROR_DATA_DEPTH_LIMIT, MAX_JSON_DEPTH } from "../../../horsies/src/codec/bounds.ts";
import { errorDataText, type StoredErrorData } from "../../../horsies/src/codec/error-data.ts";
import { resultEnvelopeFile } from "./support/vectors.ts";

function nested(levels: number): unknown {
  let value: unknown = 1;

  for (let level = 0; level < levels; level += 1) {
    value = [value];
  }

  return value;
}

function replacedCause<V>(data: V): string {
  const stored: StoredErrorData = errorDataText(data);

  assert.equal(stored.state, "replaced");
  assert(stored.state === "replaced");
  assert.equal(stored.text, `{"data_refused":"${stored.cause}"}`);

  return stored.cause;
}

/** The core writes the error object of a user error (`error_envelope`); this test stands in for it. */
function userErrorEnvelopeText(code: string, message: string | null, dataText: string): string {
  const messageText = message === null ? "null" : JSON.stringify(message);
  const error = `{"exception":null,"error_code":${JSON.stringify(code)},"data":${dataText},"message":${messageText}}`;

  return `{"__h_task_result__":true,"ok":null,"err":${error}}`;
}

describe("task error data", () => {
  test("JSON data is stored as its text; null is null", () => {
    assert.deepEqual(errorDataText({ a: [1, 2.5, "x"] }), { state: "stored", text: '{"a":[1,2.5,"x"]}' });
    assert.deepEqual(errorDataText(null), { state: "stored", text: "null" });
  });

  test("StrictJsonError: data that is not JSON-native", () => {
    for (const data of [new Date(0), new Map(), undefined, { a: Number.NaN }, { a: "\ud800" }, () => 1]) {
      assert.equal(replacedCause(data), "StrictJsonError");
    }
  });

  test("StrictJsonError: a key that only the engine writes, at any depth", () => {
    assert.equal(replacedCause({ a: [{ __h_task_result__: true }] }), "StrictJsonError");
  });

  test(`StrictJsonError: more than ${MAX_JSON_DEPTH} levels`, () => {
    assert.equal(replacedCause(nested(MAX_JSON_DEPTH + 1)), "StrictJsonError");
  });

  test(`ValueValidationError: more than ${ERROR_DATA_DEPTH_LIMIT} levels; ${ERROR_DATA_DEPTH_LIMIT} are stored`, () => {
    assert.equal(errorDataText(nested(ERROR_DATA_DEPTH_LIMIT)).state, "stored");
    assert.equal(replacedCause(nested(ERROR_DATA_DEPTH_LIMIT + 1)), "ValueValidationError");
  });

  test("SerializationError: an integer of 4,301 digits", () => {
    assert.equal(replacedCause({ count: 10n ** 4300n }), "SerializationError");
  });
});

describe("result_envelope.json user errors", () => {
  for (const vector of resultEnvelopeFile().vectors) {
    const error = vector.err;

    if (error === undefined || !("user" in error.error_code)) {
      continue;
    }

    const code = error.error_code.user;

    test(`${vector.name}: the data text, in the error object that the core writes`, () => {
      const data = errorDataText(error.data);

      assert.equal(userErrorEnvelopeText(code, error.message, data.text), vector.text);
    });
  }
});

describe("task error data: the check in front of the reader", () => {
  test("the error data writer makes a JsonText of strict JSON only", () => {
    for (const data of [Number.NaN, { a: "\ud800" }, nested(MAX_JSON_DEPTH + 1), 10n ** 4300n, new Date(0)]) {
      const stored = errorDataText(data);

      assert.equal(stored.state, "replaced");
      assert.equal(JSON.stringify(JSON.parse(stored.text)), stored.text);
    }
  });
});
