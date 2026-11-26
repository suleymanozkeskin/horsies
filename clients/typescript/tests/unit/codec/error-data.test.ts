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

/** An integer of 4,301 digits. */
const TOO_LONG = 10n ** 4300n;

/** The levels at which Pydantic's dump refuses data: one more than it writes. */
const PYDANTIC_DUMP_LEVELS = 255;

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
  test("strict JSON data is stored as its text; null is null", () => {
    assert.deepEqual(errorDataText({ a: [1, 2.5, "x"] }), { state: "stored", text: '{"a":[1,2.5,"x"]}' });
    assert.deepEqual(errorDataText(null), { state: "stored", text: "null" });
  });

  test("an engine key and more than 199 levels are stored as text: the core replaces them", () => {
    assert.deepEqual(errorDataText({ a: { __h_task_result__: true } }), {
      state: "stored",
      text: '{"a":{"__h_task_result__":true}}',
    });
    assert.equal(errorDataText(nested(ERROR_DATA_DEPTH_LIMIT + 1)).state, "stored");
    assert.equal(errorDataText(nested(PYDANTIC_DUMP_LEVELS + 1)).state, "stored");
  });

  test("StrictJsonError: data that is not JSON-native, NaN, a lone surrogate, more than 300 levels", () => {
    for (const data of [
      new Date(0),
      new Map(),
      undefined,
      () => 1,
      { [Symbol("k")]: 1 },
      { a: Number.NaN },
      [Number.POSITIVE_INFINITY],
      { a: "\ud800" },
      { "\udc00": 1 },
      nested(MAX_JSON_DEPTH + 1),
    ]) {
      assert.equal(replacedCause(data), "StrictJsonError");
    }
  });

  test("SerializationError: an integer of 4,301 digits", () => {
    assert.equal(replacedCause({ count: TOO_LONG }), "SerializationError");
    assert.equal(replacedCause([nested(ERROR_DATA_DEPTH_LIMIT + 1), TOO_LONG]), "SerializationError");
  });

  test("two faults: the first check of the Python client names the cause", () => {
    for (const [label, other] of [
      ["an engine key", { __h_x: 1 }],
      ["NaN", Number.NaN],
      ["a Date", new Date(0)],
      ["a lone surrogate", "\ud800"],
      ["more than 300 levels", nested(MAX_JSON_DEPTH + 1)],
    ] as const) {
      assert.equal(replacedCause({ a: TOO_LONG, b: other }), "StrictJsonError", `digits, then ${label}`);
      assert.equal(replacedCause({ b: other, a: TOO_LONG }), "StrictJsonError", `${label}, then digits`);
    }
  });

  test("ValueValidationError: data that Pydantic cannot dump, with another fault", () => {
    assert.equal(replacedCause([nested(PYDANTIC_DUMP_LEVELS), TOO_LONG]), "ValueValidationError");
    assert.equal(replacedCause([nested(PYDANTIC_DUMP_LEVELS), "\ud800"]), "ValueValidationError");
    assert.equal(replacedCause([nested(PYDANTIC_DUMP_LEVELS), Number.NaN]), "StrictJsonError");
  });
});

describe("result_envelope.json user errors", () => {
  for (const vector of resultEnvelopeFile().vectors) {
    const error = vector.err;

    if (error === undefined || !("user" in error.error_code)) {
      continue;
    }

    const code = error.error_code.user;
    const replacedByCore = vector.stored_data !== undefined;

    test(`${vector.name}: the data text, in the error object that the core writes`, () => {
      const data = errorDataText(error.data);

      assert.equal(data.state, "stored");

      if (replacedByCore) {
        // The data holds an engine key: the core stores the replacement in `stored_data` around this text.
        assert.equal(data.text, JSON.stringify(error.data));
        assert.equal(JSON.stringify(vector.stored_data), '{"data_refused":"StrictJsonError"}');
      } else {
        assert.equal(userErrorEnvelopeText(code, error.message, data.text), vector.text);
      }
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
