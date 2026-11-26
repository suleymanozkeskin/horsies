// The definition reader: a closed, typed view of a zod 4 schema definition (`schema._zod.def`).
//
// zod gives no stability statement for the fields of a definition. This module is the one place that reads them
// (and `_zod.traits`, `_zod.values`). It fails closed: a field, a check or a format outside the lists of
// `known-fields.ts` is read as an unknown part, which the adapter refuses. The self-check (`self-check.ts`) runs each
// rule of the adapter once on a known schema, so a renamed field fails loud instead of switching a rule off.

import * as core from "zod/v4/core";

import { isString } from "../codec/values.ts";
import {
  BIGINT_FORMATS,
  FLOAT_FORMATS,
  INTEGER_FORMATS,
  KNOWN_CHECKS,
  KNOWN_FIELDS,
  KNOWN_STRING_FORMATS,
} from "./known-fields.ts";

/** A zod 4 schema. */
export type ZodSchema = core.$ZodType;

/** A check that zod runs on a value: a refinement, a length or range check, a string or number format. */
export type ZodCheck = core.$ZodCheck<never>;

/** A zod codec function: the decode (`transform`) or the encode (`reverseTransform`) of `z.codec()`. */
export type ZodCodecFunction = NonNullable<core.$ZodPipeDef["transform"]>;

/** The test function of `z.custom()`. */
export type ZodCustomTest = core.$ZodCustomDef["fn"];

/** A check of a schema, by what it does to a value. */
export type CheckRead =
  /** It accepts or refuses the value and does not change it. */
  | { readonly effect: "check"; readonly check: ZodCheck }
  /** It replaces the value (`.trim()`, `.toLowerCase()`, `.overwrite()`): a transform. */
  | { readonly effect: "overwrite" }
  /** A check name that `KNOWN_CHECKS` does not list (a newer zod). */
  | { readonly effect: "unknownCheck"; readonly name: string }
  /** A string, number or bigint format that the known formats do not list (a newer zod). */
  | { readonly effect: "unknownFormat"; readonly name: string };

/** A member of a literal or an enum, as zod holds it. */
export type ZodLiteralValue = core.util.Primitive;

/**
 * How an object or a record treats a key that it does not declare: refused (`z.strictObject()`, and `z.object()`,
 * which zod strips and the codec refuses), or kept (`z.looseObject()`, `.catchall()`, `z.looseRecord()`).
 */
export type OtherKeys = "refused" | "kept";

/** Which keys a record must have: every member of its enum key (`z.record`), or any keys (`z.partialRecord`). */
export type KeyCoverage = "everyEnumKey" | "anyKeys";

/** The rest item of a tuple. */
export type TupleRest = { readonly state: "none" } | { readonly state: "items"; readonly item: ZodSchema };

/** A default value of `.default()` or `.prefault()`. zod reads it again at each use (it may be a function). */
export type DefaultRead = () => unknown;

/** The definition of a schema node: its form, the fields that the adapter reads, and its own checks. */
export type ZodDefinition = { readonly zodType: string; readonly checks: readonly CheckRead[] } & DefinitionForm;

/** The forms of a definition, one per zod definition type that the adapter tells apart. */
export type DefinitionForm =
  | { readonly form: "string" }
  | { readonly form: "number"; readonly number: "int" | "float" }
  | { readonly form: "bigint" }
  | { readonly form: "boolean" }
  | { readonly form: "null" }
  | { readonly form: "literal"; readonly members: readonly ZodLiteralValue[] }
  /** `pattern` is the template literal without its checks: its parse is the pattern test; the checks run apart. */
  | { readonly form: "templateLiteral"; readonly pattern: ZodSchema }
  | { readonly form: "object"; readonly members: readonly MemberRead[]; readonly otherKeys: OtherKeys }
  | { readonly form: "array"; readonly item: ZodSchema }
  | { readonly form: "tuple"; readonly items: readonly ZodSchema[]; readonly rest: TupleRest }
  | {
      readonly form: "record";
      readonly key: ZodSchema;
      readonly value: ZodSchema;
      readonly keyCoverage: KeyCoverage;
      readonly otherKeys: OtherKeys;
    }
  | { readonly form: "union"; readonly options: readonly ZodSchema[] }
  | {
      readonly form: "taggedUnion";
      readonly tag: string;
      readonly options: readonly ZodSchema[];
      readonly unmatchedTag: "refused" | "triesEachOption";
    }
  | { readonly form: "nullable"; readonly inner: ZodSchema }
  | { readonly form: "optional"; readonly inner: ZodSchema }
  | { readonly form: "nonoptional"; readonly inner: ZodSchema }
  | { readonly form: "default"; readonly inner: ZodSchema; readonly value: DefaultRead }
  | { readonly form: "prefault"; readonly inner: ZodSchema; readonly value: DefaultRead }
  | { readonly form: "readonly"; readonly inner: ZodSchema }
  | { readonly form: "lazy"; readonly inner: ZodSchema }
  | {
      readonly form: "codec";
      readonly input: ZodSchema;
      readonly output: ZodSchema;
      readonly decode: ZodCodecFunction;
      readonly encode: ZodCodecFunction;
    }
  | { readonly form: "custom"; readonly test: ZodCustomTest }
  /** `z.any()`, `z.unknown()`. */
  | { readonly form: "untyped" }
  /** `.transform()`, `z.preprocess()`, `.pipe()` that is not a codec, `z.success()`. */
  | { readonly form: "transform" }
  | { readonly form: "catch" }
  | { readonly form: "date" }
  | { readonly form: "intersection" }
  /** A value that is not data: a map, a set, a symbol, a function, a promise, a file, `NaN`, `undefined`, `never`. */
  | { readonly form: "notData" }
  /** `z.coerce.*()`: it converts the input before its check, a one-way transform. */
  | { readonly form: "coerced" }
  /** `z.xor()`: a union that refuses a value that two options take. */
  | { readonly form: "exclusiveUnion" }
  /** A definition type that this reader does not know (a newer zod). */
  | { readonly form: "unknownDefinition" }
  /** A field, or a field value, that `KNOWN_FIELDS` does not list for this definition type (a newer zod). */
  | { readonly form: "unknownField"; readonly field: string };

/** A declared member of a `z.object()`, in declaration order. */
export type MemberRead = { readonly name: string; readonly schema: ZodSchema };

/** The trait of a schema that is a check itself (a string, number or bigint format, `z.custom()`). */
const CHECK_TRAIT = "$ZodCheck";

/** The trait of a string format with its own name and test (`z.stringFormat()`, `z.hex()`, `z.hash()`). */
const CUSTOM_STRING_FORMAT_TRAIT = "$ZodCustomStringFormat";

/**
 * The definition of one schema node. Its checks are the ones that zod runs on the node's value, in zod's order: the
 * node itself when it is a check (a string, number or bigint format), then `def.checks`. Pure: it reads the
 * definition, and calls the getter of `z.lazy()` once.
 */
export function readDefinition(schema: ZodSchema): ZodDefinition {
  // SAFETY: every zod 4 schema is a member of `$ZodTypes`; a newer type reaches the `default` arm of `readRefusedForm`.
  const typed = schema as core.$ZodTypes;
  const def = typed._zod.def;
  const own = def.type === "custom" ? [] : selfChecks(typed);
  const checks = [...own, ...(def.checks ?? []).map(checkReadOf)];

  return { zodType: String(def.type), checks, ...knownForm(def, schema) };
}

/** The form of a definition whose fields the lists know; else the first unknown field. */
function knownForm(def: DefinitionData, schema: ZodSchema): DefinitionForm {
  const known = KNOWN_FIELDS.get(String(def.type));
  const unknown = known === undefined ? undefined : Object.keys(def).find((field) => !known.has(field));

  return unknown === undefined ? readForm(def, schema) : { form: "unknownField", field: unknown };
}

/** zod's definition type of a schema (`string`, `pipe`, `lazy`, ...), for a refusal. Pure. */
export function zodTypeOf(schema: ZodSchema): string {
  return String(schema._zod.def.type);
}

/** The node as a check, when it is one (a format). A `z.custom()` test is read as the custom form instead. */
function selfChecks(schema: ZodSchema): readonly CheckRead[] {
  // SAFETY: a schema with the check trait is a `$ZodCheck` too (zod's own rule in `$ZodType.init`).
  return schema._zod.traits.has(CHECK_TRAIT) ? [checkReadOf(schema as unknown as ZodCheck)] : [];
}

/** The values of an enum, as zod checks them (zod's own function): a numeric enum without its reverse keys. */
function enumValues(def: core.$ZodEnumDef): readonly ZodLiteralValue[] {
  return core.util.getEnumValues(def.entries);
}

/** What a check does, by its closed name and format. Pure. */
function checkReadOf(check: ZodCheck): CheckRead {
  const def = check._zod.def;

  if (!KNOWN_CHECKS.has(def.check)) {
    return { effect: "unknownCheck", name: def.check };
  }

  switch (def.check) {
    case "overwrite":
      return { effect: "overwrite" };
    case "string_format":
      return hasTrait(check._zod, CUSTOM_STRING_FORMAT_TRAIT)
        ? { effect: "check", check }
        : formatRead(check, KNOWN_STRING_FORMATS);
    case "number_format":
      return formatRead(check, new Set([...INTEGER_FORMATS, ...FLOAT_FORMATS]));
    case "bigint_format":
      return formatRead(check, BIGINT_FORMATS);
    default:
      return { effect: "check", check };
  }
}

/** Whether zod's internals of a schema or a check hold a trait (zod's `$constructor` sets `traits` on both). */
function hasTrait(internals: object, trait: string): boolean {
  return "traits" in internals && internals.traits instanceof Set && internals.traits.has(trait);
}

/** A format check whose format is in `known`, or the unknown format. */
function formatRead(check: ZodCheck, known: ReadonlySet<string>): CheckRead {
  const format = formatOf(check._zod.def);

  return known.has(format) ? { effect: "check", check } : { effect: "unknownFormat", name: format };
}

function formatOf(def: core.$ZodTypeDef | core.$ZodCheckDef): string {
  return "format" in def && isString(def.format) ? def.format : "";
}

/** The definition data of a zod schema: one member per definition type of zod's `$ZodTypes`. */
type DefinitionData = core.$ZodTypes["_zod"]["def"];

/** Definition data without the types that an earlier reader took. */
type Without<D extends DefinitionData, T extends DefinitionData["type"]> = Exclude<D, { readonly type: T }>;

function readForm(def: DefinitionData, schema: ZodSchema): DefinitionForm {
  switch (def.type) {
    case "string":
      return coerced(def) ? { form: "coerced" } : { form: "string" };
    case "number":
      return coerced(def) ? { form: "coerced" } : { form: "number", number: numberKindOf(def) };
    case "bigint":
      return coerced(def) ? { form: "coerced" } : { form: "bigint" };
    case "boolean":
      return coerced(def) ? { form: "coerced" } : { form: "boolean" };
    case "null":
      return { form: "null" };
    case "literal":
      return { form: "literal", members: [...def.values] };
    case "enum":
      return { form: "literal", members: enumValues(def) };
    case "template_literal":
      return { form: "templateLiteral", pattern: core.clone(schema, { ...def, checks: [] }) };
    case "object":
      return readObject(def);
    case "array":
      return { form: "array", item: def.element };
    case "tuple":
      return { form: "tuple", items: [...def.items], rest: def.rest === null ? { state: "none" } : restOf(def.rest) };
    case "record":
      return readRecord(def);
    case "union":
      return readUnion(def);
    default:
      return readWrapperForm(def);
  }
}

type WrapperData = Without<
  DefinitionData,
  | "string"
  | "number"
  | "bigint"
  | "boolean"
  | "null"
  | "literal"
  | "enum"
  | "template_literal"
  | "object"
  | "array"
  | "tuple"
  | "record"
  | "union"
>;

function readWrapperForm(def: WrapperData): DefinitionForm {
  switch (def.type) {
    case "nullable":
      return { form: "nullable", inner: def.innerType };
    case "optional":
      return { form: "optional", inner: def.innerType };
    case "nonoptional":
      return { form: "nonoptional", inner: def.innerType };
    case "default":
      return { form: "default", inner: def.innerType, value: () => def.defaultValue };
    case "prefault":
      return { form: "prefault", inner: def.innerType, value: () => def.defaultValue };
    case "readonly":
      return { form: "readonly", inner: def.innerType };
    case "lazy":
      return { form: "lazy", inner: def.getter() };
    case "pipe":
      return readPipe(def);
    case "custom":
      return { form: "custom", test: def.fn };
    default:
      return readRefusedForm(def);
  }
}

type RefusedData = Without<
  WrapperData,
  "nullable" | "optional" | "nonoptional" | "default" | "prefault" | "readonly" | "lazy" | "pipe" | "custom"
>;

function readRefusedForm(def: RefusedData): DefinitionForm {
  switch (def.type) {
    case "any":
    case "unknown":
      return { form: "untyped" };
    case "transform":
    case "success":
      return { form: "transform" };
    case "catch":
      return { form: "catch" };
    case "date":
      return { form: "date" };
    case "intersection":
      return { form: "intersection" };
    case "symbol":
    case "undefined":
    case "void":
    case "never":
    case "nan":
    case "map":
    case "set":
    case "file":
    case "promise":
    case "function":
      return { form: "notData" };
    default:
      return unlistedForm(def);
  }
}

/**
 * The form of a definition type that the zod types of this build do not list. The parameter is `never`: a type that
 * zod adds to `$ZodTypes` fails the type check until an arm reads it. At run time a newer zod reaches this arm.
 */
function unlistedForm(_unlisted: never): DefinitionForm {
  return UNKNOWN_DEFINITION;
}

const UNKNOWN_DEFINITION: DefinitionForm = Object.freeze({ form: "unknownDefinition" });

function restOf(rest: core.SomeType): TupleRest {
  // SAFETY: a tuple's rest item is a zod schema (`$ZodTupleDef.rest` is a `SomeType` or `null`).
  return { state: "items", item: rest as ZodSchema };
}

/** Whether a definition converts its input first (`z.coerce.*()`). */
function coerced(def: core.$ZodTypeDef): boolean {
  return "coerce" in def && def.coerce === true;
}

/**
 * `z.int()`, `z.int32()`, `z.uint32()` and `.int()` hold an integer; `z.float32()`, `z.float64()` and no format hold a
 * float. A number format outside both lists is an unknown format of the node's checks, which the adapter refuses.
 */
function numberKindOf(def: core.$ZodNumberDef): "int" | "float" {
  const formats = [def, ...(def.checks ?? []).map((check) => check._zod.def)].map(formatOf);

  return formats.some((format) => INTEGER_FORMATS.has(format)) ? "int" : "float";
}

function readObject(def: core.$ZodObjectDef): DefinitionForm {
  const members = Object.keys(def.shape).map((name) => ({ name, schema: memberSchema(def.shape, name) }));

  return { form: "object", members, otherKeys: otherKeysOf(def.catchall) };
}

function memberSchema(members: core.$ZodObjectDef["shape"], name: string): ZodSchema {
  const schema = members[name];

  if (schema === undefined) {
    throw new TypeError(`readDefinition: the object member ${name} has no schema`);
  }

  return schema;
}

/** No catchall (zod strips, the codec refuses) and `z.never()` (`z.strictObject`): refused; anything else: kept. */
function otherKeysOf(catchall: ZodSchema | undefined): OtherKeys {
  if (catchall === undefined) {
    return "refused";
  }

  // SAFETY: a catchall is a zod schema, a member of `$ZodTypes`.
  return (catchall as core.$ZodTypes)._zod.def.type === "never" ? "refused" : "kept";
}

/** The values of the record fields that zod 4.1 to 4.6 write: `partial` (a boolean), `mode` (`strict`, `loose`). */
function readRecord(def: core.$ZodRecordDef): DefinitionForm {
  const partial = "partial" in def ? def.partial : undefined;
  const mode = "mode" in def ? def.mode : undefined;

  if (partial !== undefined && partial !== true && partial !== false) {
    return { form: "unknownField", field: "partial" };
  }

  if (mode !== undefined && mode !== "strict" && mode !== "loose") {
    return { form: "unknownField", field: "mode" };
  }

  // SAFETY: a record key schema is a zod schema (`$ZodRecordKey` is a `$ZodType`).
  const key = def.keyType as ZodSchema;

  return {
    form: "record",
    key,
    value: def.valueType,
    keyCoverage: key._zod.values !== undefined && partial !== true ? "everyEnumKey" : "anyKeys",
    otherKeys: mode === "loose" ? "kept" : "refused",
  };
}

/**
 * A union: tagged when it has a discriminator. Without one, `inclusive: false` is `z.xor()` (zod 4.2 and later), which
 * refuses a value that two options take; a discriminated union is exclusive by its tag, so the field is not read there.
 */
function readUnion(def: core.$ZodUnionDef): DefinitionForm {
  const options = [...def.options];
  const inclusive = "inclusive" in def ? def.inclusive : true;

  if (inclusive !== true && inclusive !== false) {
    return { form: "unknownField", field: "inclusive" };
  }

  if (!("discriminator" in def) || !isString(def.discriminator)) {
    return inclusive ? { form: "union", options } : { form: "exclusiveUnion" };
  }

  const fallback = "unionFallback" in def && def.unionFallback === true;

  return {
    form: "taggedUnion",
    tag: def.discriminator,
    options,
    unmatchedTag: fallback ? "triesEachOption" : "refused",
  };
}

/** A pipe with both codec functions is `z.codec()`; any other pipe transforms one way. */
function readPipe(def: core.$ZodPipeDef): DefinitionForm {
  const { transform, reverseTransform } = def;

  return transform === undefined || reverseTransform === undefined
    ? { form: "transform" }
    : { form: "codec", input: def.in, output: def.out, decode: transform, encode: reverseTransform };
}
