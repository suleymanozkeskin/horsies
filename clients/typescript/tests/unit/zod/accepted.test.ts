// One schema per accepted zod form: the kind it compiles to, the stored text of a value, and the value read back.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import { MAX_VALUE_JSON_DEPTH } from "../../../horsies/src/_catalog/codec_bounds.ts";
import { encodeWithReadBack } from "../../../horsies/src/codec/read-back.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import { checkKind, checkMemberKind } from "../../../horsies/src/wire/check.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";
import { hz } from "../../../horsies/src/zod/codecs.ts";
import { memberText } from "./self-check-cases.ts";
import {
  acceptedKind,
  acceptedMember,
  decoded,
  decodeRefusal,
  encodeRefusal,
  storedText,
  workerValue,
} from "./support.ts";

function kindTextOf(schema: Parameters<typeof acceptedMember>[0]): string {
  return memberText(acceptedMember(schema));
}

describe("zod adapter: primitive forms", () => {
  test("z.null(), z.boolean(), z.string() map to null, boolean, string", () => {
    const kind = acceptedKind(z.object({ none: z.null(), flag: z.boolean(), name: z.string() }));

    assert.equal(storedText(kind, { none: null, flag: true, name: "Zoë" }), '{"none":null,"flag":true,"name":"Zoë"}');
    assert.deepEqual(workerValue(kind, '{"none":null,"flag":false,"name":"a"}'), {
      none: null,
      flag: false,
      name: "a",
    });
  });

  test("a string format and a string check run at encode and at a worker decode, not at a stored decode", () => {
    const kind = acceptedKind(z.email());

    assert.equal(kindTextOf(z.email()), "checked(string)");
    assert.equal(storedText(kind, "a@b.co"), '"a@b.co"');
    assert.equal(encodeRefusal(kind, "no").refusal, "CheckFailed");
    assert.equal(decodeRefusal(kind, '"no"', "worker").refusal, "CheckFailed");
    assert.equal(kindTextOf(z.string().max(2)), "checked(string)");
    assert.equal(
      encodeRefusal(acceptedKind(z.string().max(2)), "abc").detail,
      "the check refused the value: $ too_big",
    );
  });

  test("z.int(), z.int32(), z.uint32(), z.number().int() map to int; zod's range check runs too", () => {
    for (const schema of [z.int(), z.int32(), z.uint32(), z.number().int()]) {
      assert.equal(kindTextOf(schema), "checked(int)");
      assert.equal(storedText(acceptedKind(schema), 7), "7");
      assert.equal(decodeRefusal(acceptedKind(schema), "7.0", "worker").refusal, "FloatForInteger");
    }

    assert.equal(encodeRefusal(acceptedKind(z.int32()), 2 ** 40).refusal, "CheckFailed");
    assert.equal(encodeRefusal(acceptedKind(z.uint32()), -1).refusal, "CheckFailed");
  });

  test("z.int() under a fraction and a whole number above the safe range: one text for each, with no builder name", () => {
    const kind = acceptedKind(z.object({ id: z.int() }));

    assert.equal(encodeRefusal(kind, { id: 7.5 }).detail, "the integer kind takes a whole number");
    assert.match(encodeRefusal(kind, { id: 2 ** 53 }).detail, /^the integer kind takes a whole number from -\(2\^53 - 1\)/u);
  });

  test("z.uuid() stores the text as given; h.uuid() refuses a text that is not lower-case", () => {
    const upper = "0190A3B4-0000-7000-8000-000000000001";

    assert.equal(storedText(acceptedKind(z.uuid()), upper), `"${upper}"`);
    assert.equal(encodeRefusal(kindOf(h.uuid()), upper).refusal, "NotCanonicalUuid");
  });

  test("z.number(), z.float64(), z.float32() map to float in Python's float text", () => {
    assert.equal(kindTextOf(z.number()), "float");
    assert.equal(storedText(acceptedKind(z.number()), 7), "7.0");
    assert.equal(storedText(acceptedKind(z.float64()), 1e-5), "1e-05");
    assert.equal(kindTextOf(z.float32()), "checked(float)");
    assert.equal(workerValue(acceptedKind(z.number()), "7"), 7);
  });

  test("z.bigint(), z.int64(), z.uint64() map to bigint with exact digits", () => {
    assert.equal(kindTextOf(z.bigint()), "bigint");
    assert.equal(storedText(acceptedKind(z.bigint()), 2n ** 70n), "1180591620717411303424");
    assert.equal(kindTextOf(z.int64()), "checked(bigint)");
    assert.equal(encodeRefusal(acceptedKind(z.uint64()), -1n).refusal, "CheckFailed");
    assert.equal(workerValue(acceptedKind(z.int64()), "9223372036854775807"), 9_223_372_036_854_775_807n);
  });

  test("z.literal() of strings, numbers, booleans and null, z.enum([...]) and z.enum(TsEnum) map to literal", () => {
    // The object that a TypeScript numeric enum compiles to, with its reverse keys.
    const Level = { Low: 1, High: 3, "1": "Low", "3": "High" } as const;

    assert.equal(kindTextOf(z.literal(["a", 1, true, null])), 'literal("a",1,true,null)');
    assert.equal(kindTextOf(z.enum(["red", "green"])), 'literal("red","green")');
    assert.equal(kindTextOf(z.enum(Level)), "literal(1,3)");
    assert.equal(storedText(acceptedKind(z.enum(Level)), Level.High), "3");
    assert.equal(decodeRefusal(acceptedKind(z.enum(["red"])), '"blue"', "worker").refusal, "NotAMember");
  });

  test("z.templateLiteral() maps to a checked string", () => {
    const kind = acceptedKind(z.templateLiteral(["order-", z.int()]));

    assert.equal(storedText(kind, "order-12"), '"order-12"');
    assert.equal(encodeRefusal(kind, "invoice-12").refusal, "CheckFailed");
  });
});

describe("zod adapter: containers", () => {
  test("z.object() and z.strictObject() map to object, written in declaration order", () => {
    const kind = acceptedKind(z.object({ b: z.int(), a: z.string() }));

    assert.equal(storedText(kind, { a: "x", b: 1 }), '{"b":1,"a":"x"}');
    assert.equal(kindTextOf(z.strictObject({ a: z.null() })), "object(a:null)");
  });

  test("an unknown key is refused at decode, also for a z.object() that zod would strip", () => {
    const kind = acceptedKind(z.object({ id: z.int() }));

    assert.deepEqual(z.object({ id: z.int() }).parse({ id: 1, extra: 2 }), { id: 1 });
    assert.equal(decodeRefusal(kind, '{"id":1,"extra":2}', "worker").refusal, "UnknownMember");
    assert.equal(decodeRefusal(kind, '{"id":1,"extra":2}', "stored").refusal, "UnknownMember");
    assert.equal(encodeRefusal(kind, { id: 1, extra: 2 }).refusal, "UnknownMember");
  });

  test("an explicit undefined is refused at encode, though zod takes it for an optional member", () => {
    const schema = z.object({ id: z.int(), note: z.string().optional() });

    assert.deepEqual(schema.parse({ id: 1, note: undefined }), { id: 1, note: undefined });
    assert.equal(encodeRefusal(acceptedKind(schema), { id: 1, note: undefined }).refusal, "ExplicitUndefined");
    assert.equal(storedText(acceptedKind(schema), { id: 1 }), '{"id":1}');
  });

  test("z.array() and z.tuple([...]) map to array and tuple", () => {
    assert.equal(storedText(acceptedKind(z.array(z.int())), [1, 2]), "[1,2]");
    assert.equal(storedText(acceptedKind(z.tuple([z.string(), z.number()])), ["a", 1]), '["a",1.0]');
    assert.equal(decodeRefusal(acceptedKind(z.tuple([z.string()])), '["a","b"]', "worker").refusal, "WrongType");
  });

  test("z.record(z.string(), v) maps to record, in the value's own key order", () => {
    const kind = acceptedKind(z.record(z.string(), z.int()));

    assert.equal(kindTextOf(z.record(z.string(), z.int())), "record(checked(int))");
    assert.equal(storedText(kind, { b: 1, a: 2 }), '{"b":1,"a":2}');
  });

  test("z.record(z.enum([...]), v) requires each enum key, as zod does; z.partialRecord() does not", () => {
    const full = acceptedKind(z.record(z.enum(["a", "b"]), z.int()));
    const partial = acceptedKind(z.partialRecord(z.enum(["a", "b"]), z.int()));

    assert.equal(storedText(full, { a: 1, b: 2 }), '{"a":1,"b":2}');
    assert.equal(encodeRefusal(full, { a: 1 }).detail, "the check refused the value: $.b missing_key");
    assert.equal(
      encodeRefusal(full, { a: 1, b: 2, c: 3 }).detail,
      "the check refused the value: $.<input> invalid_value",
    );
    assert.equal(storedText(partial, { b: 2 }), '{"b":2}');
    assert.equal(decodeRefusal(partial, '{"c":1}', "worker").refusal, "CheckFailed");
  });

  test("a record key with a string format checks each key", () => {
    const kind = acceptedKind(z.record(z.uuid(), z.boolean()));

    assert.equal(storedText(kind, { "00000000-0000-4000-8000-000000000000": true }).length > 0, true);
    assert.equal(encodeRefusal(kind, { key: true }).detail, "the check refused the value: $.<input> invalid_format");
  });
});

describe("zod adapter: modifiers", () => {
  test(".optional(), .nullable(), .nullish() map to the presence and the nullable kind", () => {
    assert.equal(kindTextOf(z.string().optional()), "?string");
    assert.equal(kindTextOf(z.string().nullable()), "nullable(string)");
    assert.equal(kindTextOf(z.string().nullish()), "?nullable(string)");
    assert.equal(kindTextOf(z.string().optional().nullable()), "?nullable(string)");
    assert.equal(kindTextOf(z.string().optional().nonoptional()), "string");
  });

  test(".default(v) applies at decode only: the sender leaves it out; the worker form fills it", () => {
    const kind = acceptedKind(z.object({ id: z.int(), retries: z.int().default(3) }));

    assert.equal(storedText(kind, { id: 1 }), '{"id":1}');
    assert.equal(storedText(kind, { id: 1, retries: 5 }), '{"id":1,"retries":5}');
    assert.deepEqual(workerValue(kind, '{"id":1}'), { id: 1, retries: 3 });
    assert.equal(z.encode(z.object({ retries: z.int().default(3) }), { retries: 4 }).retries, 4);
  });

  test(".prefault(v) is an input value: zod decodes it, and the reader fills its value", () => {
    const kind = acceptedKind(z.object({ at: hz.instant().prefault("2026-08-12T09:10:12+02:00") }));

    assert.equal(storedText(kind, {}), "{}");
    assert.deepEqual(workerValue(kind, "{}"), { at: Temporal.Instant.from("2026-08-12T07:10:12Z") });
  });

  test("a default is computed: checkKind reads it once and refuses a value that does not encode", () => {
    const member = acceptedMember(z.int().default(1.5));

    assert.equal(member.presence.mode === "defaulted" && member.presence.fallback.state, "computed");
    assert.deepEqual(
      checkMemberKind(member.kind, member.presence).map((refused) => refused.rule),
      ["DefaultNotStored"],
    );
  });

  test("a function default gives a new value at each worker decode and stays absent in the stored form", () => {
    let calls = 0;
    const kind = acceptedKind(z.object({ id: z.int().default(() => (calls += 1)), name: z.string() }));

    assert.equal(calls, 0);
    assert.deepEqual(workerValue(kind, '{"name":"a"}'), { id: 1, name: "a" });
    assert.deepEqual(workerValue(kind, '{"name":"a"}'), { id: 2, name: "a" });
    assert.deepEqual(decoded(kind, '{"name":"a"}', "stored"), { ok: true, value: { name: "a" } });
    assert.deepEqual(encodeWithReadBack(kind, { name: "a" }), { ok: true, value: '{"name":"a"}' });
    assert.equal(storedText(kind, { id: 7, name: "a" }), '{"id":7,"name":"a"}');
  });

  test("DefaultRefused: a function default whose value does not encode, at a worker decode", () => {
    const kind = acceptedKind(z.object({ id: z.int().default(() => 1.5) }));

    assert.equal(decodeRefusal(kind, "{}", "worker").refusal, "DefaultRefused");
    assert.deepEqual(decoded(kind, "{}", "stored"), { ok: true, value: {} });
  });
});

describe("zod adapter: recursive schemas and tuples with a rest item", () => {
  /** A tree of `levels` objects, each in the children array of the one above it. */
  function tree(levels: number): object {
    let node: object = { name: "leaf", children: [] };

    for (let level = 1; level < levels; level += 1) {
      node = { name: "node", children: [node] };
    }

    return node;
  }

  const lazyTree: z.ZodType = z.lazy(() => z.object({ name: z.string(), children: z.array(lazyTree) }));

  const getterTree = z.object({
    name: z.string(),
    get children(): z.ZodArray<typeof getterTree> {
      return z.array(getterTree);
    },
  });

  test("a tree with z.lazy() and with a getter member encodes and decodes", () => {
    for (const schema of [lazyTree, getterTree]) {
      const kind = acceptedKind(schema);
      const text = '{"name":"a","children":[{"name":"b","children":[]},{"name":"c","children":[]}]}';

      assert.equal(kindTextOf(schema), "object(name:string,children:array(lazy))");
      assert.deepEqual(checkKind(kind), []);
      assert.equal(storedText(kind, JSON.parse(text)), text);
      assert.deepEqual(workerValue(kind, text), JSON.parse(text));
      assert.equal(decodeRefusal(kind, '{"name":"a","children":[{"name":"b"}]}', "worker").path, "$.children[0].children");
    }
  });

  test("a value of 201 levels is TooDeep at encode and at decode; 200 levels pass", () => {
    const kind = acceptedKind(lazyTree);
    const deep = JSON.stringify(tree(MAX_VALUE_JSON_DEPTH / 2 + 1));

    assert.equal(storedText(kind, tree(MAX_VALUE_JSON_DEPTH / 2)).length > 0, true);
    assert.equal(encodeRefusal(kind, tree(MAX_VALUE_JSON_DEPTH / 2 + 1)).refusal, "TooDeep");
    assert.equal(decodeRefusal(kind, deep, "worker").refusal, "TooDeep");
  });

  test("z.tuple([...], rest) encodes and decodes with 0, 1 and many rest items", () => {
    const kind = acceptedKind(z.tuple([z.int(), z.string()], z.boolean()));

    assert.equal(kindTextOf(z.tuple([z.int(), z.string()], z.boolean())), "tuple(checked(int),string,...boolean)");

    for (const value of [[1, "a"], [1, "a", true], [1, "a", true, false, true]]) {
      assert.equal(storedText(kind, value), JSON.stringify(value));
      assert.deepEqual(workerValue(kind, JSON.stringify(value)), value);
    }
  });

  test("a wrong rest item is WrongType at its index; fewer items than the fixed items are WrongType", () => {
    const kind = acceptedKind(z.tuple([z.int()], z.boolean()));

    assert.deepEqual(pick(encodeRefusal(kind, [1, true, "x"])), ["WrongType", "$[2]"]);
    assert.deepEqual(pick(decodeRefusal(kind, '[1,true,"x"]', "worker")), ["WrongType", "$[2]"]);
    assert.deepEqual(pick(encodeRefusal(kind, [])), ["WrongType", "$"]);
    assert.deepEqual(pick(decodeRefusal(kind, "[]", "worker")), ["WrongType", "$"]);
  });
});

function pick(refused: { readonly refusal: string; readonly path: string }): readonly [string, string] {
  return [refused.refusal, refused.path];
}

describe("zod adapter: unions", () => {
  test("z.union([...]) of primitives maps to union", () => {
    const kind = acceptedKind(z.union([z.string(), z.int(), z.null()]));

    assert.equal(kindTextOf(z.union([z.string(), z.int(), z.null()])), "union(string|checked(int)|null)");
    assert.equal(storedText(kind, 7), "7");
    assert.equal(workerValue(kind, '"a"'), "a");
  });

  test("z.discriminatedUnion(tag, [...]) maps to a tagged union", () => {
    const schema = z.discriminatedUnion("method", [
      z.object({ method: z.literal("card"), last4: z.string() }),
      z.object({ method: z.literal("cash") }),
    ]);

    const kind = acceptedKind(schema);

    assert.equal(storedText(kind, { method: "cash" }), '{"method":"cash"}');
    assert.equal(decodeRefusal(kind, '{"method":"wire"}', "worker").refusal, "NotAMember");
  });

  test("a refinement of one option runs only for a value of that option", () => {
    const schema = z.discriminatedUnion("method", [
      z.object({ method: z.literal("card"), last4: z.string() }).refine((card) => card.last4.length === 4),
      z.object({ method: z.literal("cash") }),
    ]);

    const kind = acceptedKind(schema);

    assert.equal(storedText(kind, { method: "card", last4: "1234" }), '{"method":"card","last4":"1234"}');
    assert.equal(encodeRefusal(kind, { method: "card", last4: "12" }).refusal, "CheckFailed");
    assert.equal(storedText(kind, { method: "cash" }), '{"method":"cash"}');
  });

  test("a nested discriminated union of the same tag gives its options", () => {
    const inner = z.discriminatedUnion("kind", [
      z.object({ kind: z.literal("b") }),
      z.object({ kind: z.literal("c") }),
    ]);

    const schema = z.discriminatedUnion("kind", [z.object({ kind: z.literal("a") }), inner]);

    assert.equal(
      kindTextOf(schema),
      'tagged.kind(object(kind:literal("a"))|object(kind:literal("b"))|object(kind:literal("c")))',
    );
  });
});

describe("zod adapter: JSON, codecs, checks, lazy, h.* inside zod", () => {
  test("z.json() maps to json", () => {
    const kind = acceptedKind(z.json());

    assert.equal(kindTextOf(z.json()), "json");
    assert.equal(storedText(kind, { a: [1, 1.5, "x", null] }), '{"a":[1,1.5,"x",null]}');
  });

  test("z.codec(input, output, ...) maps to a conversion over the input's kind", () => {
    const cents = z.codec(z.int(), z.custom<{ cents: number }>(), {
      decode: (stored) => ({ cents: stored }),
      encode: (value) => value.cents,
    });

    const kind = acceptedKind(z.object({ price: cents }));

    assert.equal(kindTextOf(cents), "conversion(checked(int))");
    assert.equal(storedText(kind, { price: { cents: 250 } }), '{"price":250}');
    assert.deepEqual(workerValue(kind, '{"price":99}'), { price: { cents: 99 } });
    assert.equal(decodeRefusal(kind, '{"price":1.5}', "worker").refusal, "FloatForInteger");
  });

  test(".refine(), .check(), .superRefine() map to checks; .brand() and .readonly() change nothing", () => {
    const even = z.int().refine((value) => value % 2 === 0);

    assert.equal(kindTextOf(even), "checked(int)");
    assert.equal(encodeRefusal(acceptedKind(even), 3).detail, "the check refused the value: $ custom");
    assert.equal(decodeRefusal(acceptedKind(even), "3", "worker").refusal, "CheckFailed");
    assert.equal(kindTextOf(z.string().check(z.minLength(1))), "checked(string)");
    assert.equal(kindTextOf(z.string().superRefine(() => undefined)), "checked(string)");
    assert.equal(kindTextOf(z.string().brand<"Sku">()), "string");
    assert.equal(kindTextOf(z.array(z.string()).readonly()), "array(string)");
  });

  test("a check of an object sees the value that the user holds: codec outputs and defaults filled", () => {
    const schema = z
      .object({ start: hz.plainDate(), days: z.int().default(1) })
      .refine((range) => range.start.year > 2000 && range.days > 0, { path: ["start"] });

    const kind = acceptedKind(schema);

    assert.deepEqual(workerValue(kind, '{"start":"2026-08-12"}'), {
      start: Temporal.PlainDate.from("2026-08-12"),
      days: 1,
    });
    assert.equal(decodeRefusal(kind, '{"start":"1999-01-01"}', "worker").detail.endsWith("$.<input> custom"), true);
  });

  test("z.lazy() of a schema that does not contain itself maps to that schema's kind", () => {
    assert.equal(kindTextOf(z.lazy(() => z.array(z.string()))), "array(string)");
  });

  test("an h.* value inside zod (hz.wire) maps to its own kind", () => {
    const order = h.object({ id: h.uuid(), retries: h.int().default(3) });
    const schema = z.object({ order: hz.wire(order), note: hz.wire(h.string()).optional() });
    const kind = acceptedKind(schema);

    assert.equal(kindTextOf(hz.wire(order)), "object(id:uuid,retries:=int)");
    assert.deepEqual(acceptedKind(hz.wire(order)), kindOf(order));
    assert.equal(
      storedText(kind, { order: { id: "00000000-0000-4000-8000-000000000000" } }),
      '{"order":{"id":"00000000-0000-4000-8000-000000000000"}}',
    );
  });
});
