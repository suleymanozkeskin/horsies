// A schema of another zod copy: `require("zod")` loads zod's CommonJS build, a copy apart from the ES module that
// horsies imports. The adapter reads the schema's own release and runs the schema through its own internals, so each
// form compiles, writes, reads and refuses as a schema of the ES module does. One test per run of the adapter.

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { describe, test } from "node:test";

import type * as ZodModule from "zod";
import { z } from "zod";

import { encodeWithReadBack } from "../../../horsies/src/codec/read-back.ts";
import { isPlainObject } from "../../../horsies/src/codec/values.ts";
import type { Kind } from "../../../horsies/src/wire/kinds.ts";
import { kindOf, presenceOf } from "../../../horsies/src/wire/wire-type.ts";
import { hz } from "../../../horsies/src/zod/codecs.ts";
import { zodMemberWire, zodValueWire } from "../../../horsies/src/zod/kind.ts";
import { ASYNC_ISSUE_CODE, checksRun, ONE_WAY_ISSUE_CODE } from "../../../horsies/src/zod/runs.ts";
import { kindText } from "./self-check-cases.ts";
import {
  computedDefaultText,
  decodeRefusal,
  encodeRefusal,
  runsOfCodec,
  storedText,
  workerValue,
} from "./support.ts";

// SAFETY: `require("zod")` gives the exports of zod's CommonJS build, which the zod declarations describe.
const commonJs = createRequire(import.meta.url)("zod") as typeof ZodModule;
const zc = commonJs.z;

/** The kind of a schema through `zodValueWire`; a refusal fails the test. */
function valueKind(schema: ZodModule.ZodType): Kind {
  const wire = zodValueWire(schema);

  assert.ok(wire.ok, wire.ok ? "" : JSON.stringify(wire.error));

  return kindOf(wire.value);
}

const AT_TEXT = "2026-08-12T09:10:12.654321Z";

/** An object with a refine, a default, an `hz.instant()` member and a codec whose output has a check. */
const order = zc
  .object({
    at: hz.instant(),
    count: zc.int().default(1),
    code: zc.codec(zc.string(), zc.number().max(100), {
      decode: (text) => Number(text),
      encode: (value) => String(value),
    }),
  })
  .refine((value) => value.count <= 5);

function orderKind(): Kind {
  return valueKind(order);
}

describe("a schema of zod's CommonJS build", () => {
  test("require gives another zod copy than import, of the same release", () => {
    const required = zc.string()._zod.version;
    const imported = z.string()._zod.version;

    assert.notEqual(required, imported);
    assert.deepEqual({ ...required }, { ...imported });
  });

  test("zodValueWire gives the kind of the object: its refine, its default, the hz.instant() member, the codec", () => {
    assert.equal(
      kindText(orderKind(), 0),
      "checked(object(at:instant,count:=checked(int),code:conversion(string)))",
    );
  });

  test("zodMemberWire gives the presence and the computed default of a member", () => {
    const member = zodMemberWire(zc.int().default(1));

    assert.ok(member.ok);
    assert.equal(computedDefaultText(presenceOf(member.value)), "1");
  });

  test("the stored text of a value, and its read-back through the codec", () => {
    const value = { at: Temporal.Instant.from(AT_TEXT), count: 2, code: 7 };
    const text = `{"at":"${AT_TEXT}","count":2,"code":"7"}`;

    assert.equal(storedText(orderKind(), value), text);
    assert.deepEqual(encodeWithReadBack(orderKind(), value), { ok: true, value: text });
  });

  test("the worker value of a stored text: the codec decodes, the reader fills the default", () => {
    const read = workerValue(orderKind(), `{"at":"${AT_TEXT}","code":"7"}`);

    assert.ok(isPlainObject(read));
    assert.ok(read["at"] instanceof Temporal.Instant);
    assert.deepEqual([String(read["at"]), read["count"], read["code"]], [AT_TEXT, 1, 7]);
  });

  test("the object refine refuses a value at encode and a stored text at decode: the check's own _zod.check", () => {
    assert.deepEqual(encodeRefusal(orderKind(), { at: Temporal.Instant.from(AT_TEXT), count: 9, code: 7 }), {
      kind: "encodeRefused",
      refusal: "CheckFailed",
      path: "$",
      detail: "the check refused the value: $ custom",
    });
    assert.deepEqual(decodeRefusal(orderKind(), `{"at":"${AT_TEXT}","count":9,"code":"7"}`, "worker"), {
      kind: "decodeRefused",
      refusal: "CheckFailed",
      path: "$",
      detail: "the check refused the read value: $ custom",
    });
  });

  test("the codec's output schema refuses a value at encode: the output's own _zod.run, backward", () => {
    assert.deepEqual(encodeRefusal(orderKind(), { at: Temporal.Instant.from(AT_TEXT), count: 2, code: 500 }), {
      kind: "encodeRefused",
      refusal: "CheckFailed",
      path: "$.code",
      detail: "the conversion refused the value: $ too_big",
    });
  });

  test("a prefault value decodes through the schema's own _zod.run, as an input value", () => {
    const counted = zc.codec(zc.string(), zc.number(), { decode: Number, encode: String });
    const member = zodMemberWire(counted.prefault("5"));

    assert.ok(member.ok);
    assert.equal(computedDefaultText(presenceOf(member.value)), '"5"');
  });

  test("a template literal tests its pattern with a copy that the schema's own constructor builds", () => {
    const kind = valueKind(zc.templateLiteral(["v", zc.int()]).refine((text) => text.length <= 3));

    assert.equal(storedText(kind, "v12"), '"v12"');
    assert.deepEqual(
      [encodeRefusal(kind, "w12").detail, encodeRefusal(kind, "v123").detail],
      ["the check refused the value: $ invalid_format", "the check refused the value: $ custom"],
    );
  });

  test("the keys of a record with an enum key run through the key schema's own _zod.run", () => {
    const kind = valueKind(zc.record(zc.enum(["a", "b"]), zc.boolean()));

    assert.equal(storedText(kind, { a: true, b: false }), '{"a":true,"b":false}');
    assert.deepEqual(
      [encodeRefusal(kind, { a: true }).detail, encodeRefusal(kind, { a: true, b: false, c: true }).detail],
      ["the check refused the value: $.b missing_key", "the check refused the value: $.<input> invalid_value"],
    );
  });

  test("a check that returns a promise is the issue async_check", () => {
    const later = zc.string().refine(async () => true);

    assert.deepEqual(checksRun(later._zod.def.checks ?? [])("x"), {
      ok: false,
      error: { issues: [{ path: "$", code: ASYNC_ISSUE_CODE }] },
    });
  });

  test("the copy's own error for a promise in a synchronous run is the issue async_check", () => {
    const later = zc.codec(zc.string(), zc.string().refine(async () => true), {
      decode: (text) => text,
      encode: (text) => text,
    });

    assert.deepEqual(runsOfCodec(later).decode("x"), {
      ok: false,
      error: { issues: [{ path: "$", code: ASYNC_ISSUE_CODE }] },
    });
  });

  test("the copy's own error for a one-way transform at encode is the issue one_way_transform", () => {
    const lossy = zc.codec(zc.string(), zc.string().transform((text) => text), {
      decode: (text) => text,
      encode: (text) => text,
    });

    assert.deepEqual(runsOfCodec(lossy).encode("x"), {
      ok: false,
      error: { issues: [{ path: "$", code: ONE_WAY_ISSUE_CODE }] },
    });
  });
});
