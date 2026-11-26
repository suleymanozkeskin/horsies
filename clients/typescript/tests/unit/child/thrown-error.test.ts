// The exception record of a thrown value: an `Error`, its cause chain, the text cuts, a `ZodError` summary, and each
// type rule of a value that is not an `Error`.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import {
  CAUSE_SEPARATOR,
  cutAtCharacter,
  MAX_CAUSE_LINKS,
  MESSAGE_CUT_BYTES,
  thrownRecord,
  TRACEBACK_CUT_BYTES,
} from "../../../horsies/src/child/thrown-error.ts";

class MyFailure {
  readonly reason = "declined";
}

function utf8Length(text: string): number {
  return new TextEncoder().encode(text).length;
}

describe("the record of a thrown Error", () => {
  test("the name, the message, a repr equal to the message, and the stack", () => {
    const error = new TypeError("x is not a function");
    const record = thrownRecord(error);

    assert.equal(record.type, "TypeError");
    assert.equal(record.message, "x is not a function");
    assert.equal(record.repr, record.message);
    assert.equal(record.traceback, error.stack);
  });

  test("the stack of each cause follows the stack of the error", () => {
    const root = new RangeError("the pool is full");
    const error = new Error("the charge failed", { cause: root });
    const record = thrownRecord(error);

    assert.equal(record.traceback, `${String(error.stack)}${CAUSE_SEPARATOR}${String(root.stack)}`);
  });

  test("a cause chain is cut at 64 links, and a cycle ends it", () => {
    let error = new Error("link 0");

    for (let index = 1; index <= MAX_CAUSE_LINKS + 5; index += 1) {
      error = new Error(`link ${String(index)}`, { cause: error });
    }

    const long = thrownRecord(error).traceback;

    assert.equal(long.split(CAUSE_SEPARATOR).length, MAX_CAUSE_LINKS);
    assert.match(long, /older causes not shown/u);

    const first = new Error("first");
    const second = new Error("second", { cause: first });

    Object.defineProperty(first, "cause", { value: second });
    assert.equal(thrownRecord(second).traceback.split(CAUSE_SEPARATOR).length, 2);
  });

  test("the message is cut at 16 KiB and the traceback at 64 KiB, at a character boundary", () => {
    const error = new Error("é".repeat(MESSAGE_CUT_BYTES));

    error.stack = "ğ".repeat(TRACEBACK_CUT_BYTES);

    const record = thrownRecord(error);

    assert.equal(utf8Length(record.message), MESSAGE_CUT_BYTES);
    assert.equal(record.message, "é".repeat(MESSAGE_CUT_BYTES / 2));
    assert.equal(utf8Length(record.traceback), TRACEBACK_CUT_BYTES);
    assert.equal(cutAtCharacter("a✓", 3), "a");
    assert.equal(cutAtCharacter("a✓", 4), "a✓");
  });

  test("a ZodError stores the paths and codes of its issues, never a value", () => {
    const parsed = z.object({ email: z.string(), age: z.number() }).safeParse({ email: 4, age: "secret-value" });

    assert.equal(parsed.success, false);

    const record = thrownRecord(parsed.error);

    assert.equal(record.type, "ZodError");
    assert.equal(record.message, "email: [invalid_type]; age: [invalid_type]");
    assert.equal(record.repr, record.message);
    assert.equal(record.traceback, record.message);
    assert.doesNotMatch(`${record.message}${record.traceback}`, /secret-value/u);
  });
});

describe("the record of a thrown value that is not an Error", () => {
  test("each type rule", () => {
    assert.deepEqual(thrownRecord(null), { type: "null", message: "null", repr: "null", traceback: "" });
    assert.deepEqual(thrownRecord(undefined), { type: "undefined", message: "undefined", repr: "undefined", traceback: "" });
    assert.equal(thrownRecord({ code: 1 }).type, "Object");
    assert.equal(thrownRecord(Object.create(null)).type, "Object");
    assert.equal(thrownRecord(new MyFailure()).type, "MyFailure");
    assert.deepEqual(thrownRecord(42), { type: "number", message: "42", repr: "42", traceback: "" });
    assert.equal(thrownRecord("declined").type, "string");
    assert.equal(thrownRecord(5n).type, "bigint");
    assert.equal(thrownRecord(false).type, "boolean");
    assert.equal(thrownRecord(Symbol("s")).type, "symbol");
  });

  test("a value whose String throws gives the text of its type", () => {
    const record = thrownRecord(Object.create(null));

    assert.equal(record.message, "value of type Object");
    assert.equal(record.repr, "value of type Object");
  });
});
