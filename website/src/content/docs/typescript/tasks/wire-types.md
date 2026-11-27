---
title: Wire Types
summary: The h.* wire types and the zod 4 schemas that a contract declares, the value and sent types they give, the stored form of each kind, and the refusals at Horsies.create.
related: [defining-tasks, sending-tasks, retrieving-results, ../../internals/serialization]
tags: [typescript, wire types, h, zod, hz, schema, Decimal, JsonValue, Temporal, stored form]
---

# Wire Types

A contract declares one schema for each input parameter and one for the output. A schema is an `h.*` wire type of `horsies`, or a zod 4 schema. The schema gives three facts: the value that a reader and a body get, the value that a sender gives, and the stored JSON text. The stored text of each kind is the text that the Python client stores for the same value. The shared rules of the stored form are on [Serialization](/horsies/internals/serialization/).

## The h.* Kinds

```ts
import { Decimal, h } from "horsies";

const OrderLine = h.object({
  sku: h.string(),
  quantity: h.int(),
  unitPrice: h.decimal(),
});

const Order = h.object({
  orderId: h.uuid(),
  placedAt: h.instant(),
  lines: h.array(OrderLine),
  note: h.string().optional(),
  priority: h.int().default(5),
  channel: h.enum(["web", "store"]),
});

type Order = h.Infer<typeof Order>;

const unitPrice = Decimal.parse("19.90");
```

| Builder | Value type | Stored form |
|---------|------------|-------------|
| `h.null()` | `null` | `null` |
| `h.boolean()` | `boolean` | `true` or `false` |
| `h.string()` | `string` | A JSON string. A lone surrogate is refused |
| `h.int()` | `number` | A JSON integer. Only a safe integer (`Number.isSafeInteger`); a read of an integer outside that range is refused |
| `h.bigint()` | `bigint` | A JSON integer of at most 4,300 digits, exact |
| `h.float()` | `number` | Python's float text: `7.0`, `1e-05`, `1e+16`, `-0.0`. `NaN` and the infinities are refused |
| `h.uuid()` | `string` | The lower-case 8-4-4-4-12 text, as `crypto.randomUUID()` gives it. Another form is refused |
| `h.decimal()` | `Decimal` | Python's `str(Decimal)` text in a JSON string: `"19.90"`, `"1E+3"` |
| `h.instant()` | `Temporal.Instant` | UTC: `"2026-08-12T09:10:12.654321Z"` |
| `h.offsetDateTime()` | `Temporal.ZonedDateTime` | With the offset: `"2026-08-12T09:10:12.654321+02:00"` (`Z` for offset 0). A named time zone, `UTC` too, is refused: use a fixed-offset zone |
| `h.plainDateTime()` | `Temporal.PlainDateTime` | No offset: `"2026-08-12T09:10:12.654321"` |
| `h.plainDate()` | `Temporal.PlainDate` | `"2026-08-12"` |
| `h.plainTime()` | `Temporal.PlainTime` | `"09:10:12.654321"` |
| `h.json()` | `JsonValue` | The JSON value. See [JsonValue](#jsonvalue) |
| `h.literal(...members)` | One of the members | The member. Members are strings, safe integers, booleans or `null` |
| `h.enum([...])` | One of the strings | The string |
| `h.union([...])` | One of the member values | The member's form. See [Unions](#unions) |
| `h.union(tag, [...])` | One of the member objects | The member object. See [Unions](#unions) |
| `h.object({...})` | An object of the members | A JSON object, members in declaration order |
| `h.array(item)` | `T[]` | A JSON array |
| `h.tuple([...])` | `[A, B, ...]` | A JSON array with one value per item type |
| `h.tuple([...], rest)` | `[A, B, ...R[]]` | The items, then zero or more `rest` values. Python's `tuple[R, ...]` when the item list is empty |
| `h.lazy(() => w)` | The value type of `w` | The stored form of `w`. See [Recursive Types](#recursive-types) |
| `h.record(value)` | `Readonly<Record<string, T>>` | A JSON object, keys in the value's own key order |
| `h.taskOutcome(contract)` | `TaskOutcome<O, E>` of the contract | Only for a workflow node. See [Upstream Results](#upstream-results) |

Date-time texts:

- The fraction of a second has no digits for a whole second, 6 digits for a whole microsecond, and 9 digits for a part below the microsecond. A value that Python can hold gets Python's text.
- A year outside 1 to 9999 is refused. A calendar other than `iso8601` is refused.

### Modifiers

| Modifier | Value type | Stored form |
|----------|------------|-------------|
| `.optional()` | The member may be absent | The key is left out of the stored text. It is never `null`, and an explicit `undefined` is refused |
| `.nullable()` | `T \| null` | `null` or the value |
| `.default(value)` | A sender may leave the member out; a reader and a body get `value` | The sender never stores the default: the worker fills it when it reads the input |
| `.default(() => value)` | As `.default(value)`. The reader calls the function at each read, so each read gets a new value | `Horsies.create` calls it once to check its value |

`.optional()` and `.default()` apply to an object member and to a task parameter only. An array, tuple or record item, a union member and an output are always present: `h.array(h.int().optional())` fails typecheck.

### Objects and Records

- `h.object` writes its members in declaration order, never in the key order of the value. A key that the type does not declare is refused at a write and at a read, at every depth.
- A member name that is an array index (`"0"`, `"10"`) is refused at `Horsies.create` (`IntegerLikeMember`): JavaScript puts such a key first in an object, so the declaration order cannot hold.
- A member name that only the engine writes (it starts with `__h_` or `__horsies_`) is refused (`EngineKeyMember`).
- `h.record` writes the value's own key order (`Object.keys`), so integer-like keys come first. Each value of the record has the item type.

### Unions

- `h.union([...])` takes primitive members, at most one for each stored JSON type: `h.union([h.string(), h.int(), h.null()])`. `h.int()` with `h.float()`, or `h.string()` with `h.instant()`, is refused (`AmbiguousUnion`): one stored value cannot tell which member it is. A literal member makes no union ambiguous. An array or object member is refused (`UnionMemberNotPrimitive`).
- `h.union(tag, [...])` takes `h.object` members that each declare the member `tag` as a required string literal, with no tag value twice (`UnionTagInvalid`). A value narrows on its tag:

```ts
const Payment = h.union("method", [
  h.object({ method: h.literal("card"), last4: h.string() }),
  h.object({ method: h.literal("bank"), iban: h.string() }),
]);

function paymentLabel(payment: h.Infer<typeof Payment>): string {
  switch (payment.method) {
    case "card":
      return `card ${payment.last4}`;
    case "bank":
      return `bank ${payment.iban.slice(-4)}`;
  }
}
```

## Value and Sent Types

Each wire type has two TypeScript types:

| Type | Is | For `Order` above |
|------|----|-------------------|
| `h.Infer<W>`, `ValueOf<W>` | The value that a reader and a body get: a default is filled | `priority: number`; `note?: string` |
| `SentOf<W>` | The value that a sender gives: a defaulted or optional member may be left out | `priority?: number`; `note?: string` |

The input of a contract has the same two forms, one member per parameter: `BodyInput<P>` for the body and `SendInput<P>` for `app.send`. A task with no parameters (`input: {}`) takes `{}`.

```ts
import type { SentOf } from "horsies";

const sentOrder: SentOf<typeof Order> = {
  orderId: crypto.randomUUID(),
  placedAt: Temporal.Now.instant(),
  lines: [],
  channel: "web",
};
```

## Recursive Types

A recursive type refers to itself through `h.lazy`. Give the constant its type with `Wire<T>`.

```ts
import type { Wire } from "horsies";

type Category = { name: string; children: Category[] };

const CategoryTree: Wire<Category> = h.object({
  name: h.string(),
  children: h.array(h.lazy(() => CategoryTree)),
});
```

- Each cycle must pass an array or an object. A nullable or a union of the type itself is refused (`RecursiveWithoutLevel`).
- A value that is deeper than 200 levels of arrays and objects is refused (`TooDeep`).

## Upstream Results

`h.taskOutcome(contract)` declares a parameter that a workflow node fills with the outcome of an upstream task: `{ kind: "succeeded", value }` or `{ kind: "failed", error }`. The rules:

- It is valid only as a whole task parameter, also with `.optional()` or `.nullable()`. Anywhere else it fails typecheck.
- A sender never gives it. A task with a required `h.taskOutcome` parameter cannot be sent: its sent input type is a type that no value has. An optional one is left out of the sent input.
- It takes a workflow contract too: the parameter then holds the outcome of a child workflow.

The workflow side is on [Workflow API](/horsies/typescript/concepts/workflows/workflow-api/).

## Decimal

`Decimal` holds an exact decimal as Python's `str(Decimal)` text. It has no arithmetic.

| Member | Returns |
|--------|---------|
| `Decimal.parse(text)` | `Result<Decimal, DecimalRefused>`. It takes the texts that Python's `Decimal` takes and normalises them: `1e3` is `1E+3`, `1.10` stays `1.10`, `1_000` is `1000`. `NotFinite` for an infinity or a NaN, `NotADecimal` for any other text |
| `Decimal.isDecimal(value)` | Whether an object is a `Decimal` of this package |
| `decimal.toString()` | The stored text, without the JSON quotes |
| `decimal.equals(other)` | Whether the texts are equal: `1.10` and `1.1` are different decimals |

`Decimal` has no public constructor: `new Decimal("1.0")` fails typecheck.

## JsonValue

`h.json()` names untyped JSON explicitly. Its value type is `JsonValue`: `null`, `boolean`, `number`, `bigint`, `string`, an array or an object of `JsonValue`.

- A write stores a `bigint`, and a `number` that is a safe integer (not `-0`), as a JSON integer. It stores every other finite `number` in Python's float text.
- A read gives a `number` for an integer in the safe range and for a float text, and a `bigint` for an integer outside the safe range. A stored float with a whole value in the safe range (`7.0`) reads as `7`.
- A key that only the engine writes is refused at every depth.

## zod Schemas

A parameter or an output can be a zod 4 schema. `defineTask` takes it as it takes an `h.*` wire type, and one input can mix both. The adapter reads the schema with no import of zod, so a bundled producer compiles its zod contracts too.

```ts
import { z } from "zod";
import { defineTask } from "horsies";
import { hz } from "horsies/zod";

const Shipment = z.object({
  shipmentId: z.uuid(),
  shippedAt: hz.instant(),
  weightKg: z.number().positive(),
  carrier: z.enum(["post", "courier"]).default("post"),
  insuredValue: hz.decimal().optional(),
});

const shipOrder = defineTask({
  name: "ship_order",
  input: { shipment: Shipment, order: hz.wire(Order) },
  output: z.object({ trackingCode: z.string() }),
});
```

- Supported zod releases: `>=4.1.13 <4.7.0`, the peer dependency range of the package. A schema of another zod 4 release is refused (`ZodVersionNotSupported`). A zod 3 schema, from zod 3 or from `zod/v3`, is refused (`Zod3Schema`). The refusal names zod 4 as the fix.
- A Standard Schema of another library is refused (`SchemaVendor`): only `h.*` wire types and zod 4 schemas have a stored form.
- `z.uuid()` stores the text as given: an upper-case UUID is stored in upper case. `h.uuid()` refuses it. Use `hz.wire(h.uuid())` for the lower-case form.
- A body gets zod's output type. A sender gives the same values (an `hz` codec takes the Temporal or `Decimal` value, not its text), and may leave out a member with `.optional()` or `.default()`. An explicit `undefined` fails typecheck.

### What Each zod Form Stores

| zod form | Kind |
|----------|------|
| `z.string()`, a string format (`z.email()`, `z.uuid()`, ...), `z.templateLiteral()` | `h.string()` with the zod check |
| `z.number()`, `z.float32()`, `z.float64()` | `h.float()` |
| `z.int()`, `z.int32()`, `z.uint32()`, `.int()` | `h.int()` |
| `z.bigint()` | `h.bigint()` |
| `z.boolean()`, `z.null()` | `h.boolean()`, `h.null()` |
| `z.literal()`, `z.enum()` | `h.literal()` |
| `z.object()`, `z.strictObject()` | `h.object()`. An undeclared key is refused at a read, also for `z.object()`, which zod would strip |
| `z.array()` | `h.array()` |
| `z.tuple()`, with or without a rest item | `h.tuple()` |
| `z.lazy()`, or an object with a getter member that returns its own schema | `h.lazy()` |
| `z.record()` with string keys | `h.record()` |
| `z.union()` of primitives | `h.union([...])` |
| `z.discriminatedUnion()` | `h.union(tag, [...])` |
| `z.json()` | `h.json()` |
| `z.codec(input, output, ...)` | The kind of `input`, with the codec around it |
| `.nullable()`, `.readonly()` | The inner kind |
| `hz.instant()`, `hz.offsetDateTime()`, `hz.plainDateTime()`, `hz.plainDate()`, `hz.plainTime()`, `hz.decimal()` | The `h.*` kind of the same name |
| `hz.wire(w)` | The `h.*` wire type `w`. Put `.optional()` and `.default()` on the result, not on `w` |

zod checks (`.refine()`, `.min()`, `.positive()`, formats) run at each write of a value (a send, the output of a body), on the value with its defaults filled, and when the worker reads the input. The structure of a value is checked before a check runs.

### Refused zod Forms

`Horsies.create` refuses each of these, with its path in the schema and the fix:

| Rule | Form | Fix |
|------|------|-----|
| `UNTYPED` | `z.any()`, `z.unknown()` | `h.json()` or `z.json()` |
| `TRANSFORM` | `.transform()`, `z.preprocess()`, a `.pipe()` that is not a codec, `z.coerce.*()`, `.trim()`, `.toLowerCase()`, `.toUpperCase()`, `.normalize()`, `.overwrite()` | `z.codec()` with a decode and an encode |
| `CATCH` | `.catch()` | Remove it: it hides a decode failure |
| `DATE` | `z.date()` | `hz.instant()`: a `Date` holds milliseconds only |
| `LOOSE_OBJECT` | `z.looseObject()`, `.passthrough()`, `.catchall()` | `z.object()` or `z.strictObject()` |
| `NOT_DATA` | `z.map()`, `z.set()`, `z.symbol()`, `z.function()`, `z.promise()`, `z.file()`, `z.instanceof()`, `z.custom()` outside a codec output, `z.nan()`, `z.never()`, `z.void()`, `z.undefined()` | A data type |
| `UNDISCRIMINATED_UNION` | `z.union()` with an object member | `z.discriminatedUnion()` |
| `MIXED_UNION` | `z.union()` of a primitive and an array or a record | `z.discriminatedUnion()` of objects |
| `INT_FLOAT_UNION` | A union of an integer and a float | One number kind |
| `EXCLUSIVE_UNION` | `z.xor()` | `z.union()` of members that store different JSON types |
| `INTERSECTION` | `z.intersection()`, `.and()` | One `z.object()` |
| `RECORD_KEY` | A record key that is not a string, a string format, a string enum or a string literal | String keys |
| `OPTIONAL_NOT_A_MEMBER` | `.optional()`, `.nullish()`, `.default()` or `.prefault()` outside an object member or a parameter | `.nullable()` |
| `UNION_FALLBACK` | `z.discriminatedUnion()` with `unionFallback` | Remove it: an unknown tag is refused |
| `DEFAULT_UNDER_NONOPTIONAL` | `.default()` or `.prefault()` under `.nonoptional()` | Remove one of them |
| `RECURSIVE_PRESENCE` | A recursive schema that is `.optional()`, `.default()` or `.prefault()` at its top | Put `.optional()` on the member that refers to the schema |
| `LITERAL_MEMBER` | A literal member that is not a string, number, boolean or `null` | A member of those types |
| `UNION_OPTION` | A `z.discriminatedUnion()` option that is not a `z.object()` or a discriminated union of the same tag | `z.object()` options with the tag member |
| `ENGINE_KEY` | A member name that only the engine writes | Another name |
| `UNKNOWN_DEFINITION`, `UNKNOWN_DEFINITION_FIELD`, `UNKNOWN_FORMAT`, `UNKNOWN_CHECK` | A zod part that this release of horsies does not know | A supported zod release, or `h.*` |
| `TOO_DEEP` | A schema nested deeper than the adapter walks | Fewer levels |

## Refusals at Horsies.create

The static types refuse most wrong schemas. `Horsies.create` refuses the rest, each with the code `HRS-105`, the place (`input.order.lines[*]`) and the rule:

| Rule | Cause |
|------|-------|
| `DefaultNotStored` | The value of `.default(value)` does not encode under its kind, for example `NaN` for `h.float()` |
| `EngineKeyMember` | A member name that only the engine writes |
| `IntegerLikeMember` | A member name that is an array index |
| `KindTooDeep` | More than 200 levels of arrays and objects |
| `LiteralNotSafeInteger` | A number member of a literal that is not a safe integer |
| `AmbiguousUnion` | Two union members store the same JSON type |
| `UnionMemberNotPrimitive` | An array or object member of `h.union([...])` |
| `UnionTagInvalid` | A tagged-union member without a required string-literal tag, or a tag value twice |
| `RecursiveWithoutLevel` | A recursive type that does not pass an array or an object |
| `TaskOutcomePlacement` | `h.taskOutcome` anywhere but as a whole task parameter |
| `UpstreamOutputRefused` | An `h.taskOutcome` parameter whose upstream output has no stored form |
| `KindNodesTooDeep` | A schema with more nodes than the check walks |
| `WireKind` | An output with `.optional()` or `.default()`: an output is always present |
| `SchemaVendor`, `Zod3Schema`, `ZodVersionNotSupported`, `ZodSchema` | A schema that the zod adapter refuses |

See [Defining Tasks](/horsies/typescript/tasks/defining-tasks/#the-task-list) for the other checks of `Horsies.create`.

## Things to Avoid

**Don't use `h.float()` for money.** A float cannot hold `0.10` exactly. Use `h.decimal()` or `h.int()` of the smallest unit.

<!-- ts-check: skip elided -- a Wrong / Correct pair of declarations -->
```ts
// Wrong: 0.1 + 0.2 is stored as 0.30000000000000004
const Price = h.object({ amount: h.float() });

// Correct
const Price = h.object({ amount: h.decimal() });
```

**Don't declare `z.date()` for a time.** It is refused: a `Date` holds milliseconds only, and Python stores microseconds. Use `hz.instant()` or `h.instant()`.

**Don't depend on the key order of an `h.record`.** Integer-like keys come first in a JavaScript object. Use an `h.array` of objects when the order counts.
