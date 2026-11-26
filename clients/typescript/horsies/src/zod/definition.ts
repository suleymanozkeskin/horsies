// The definition reader: a closed, typed view of a zod 4 schema definition (`schema._zod.def`).
//
// zod gives no stability statement for the fields of a definition. This module is the one place that reads them
// (and `_zod.traits`, `_zod.values`). The self-check (`self-check.ts`) runs each rule of the adapter once on a known
// schema, so a renamed field fails loud at the first compile instead of switching a rule off.

import * as core from "zod/v4/core";

import { isString } from "../codec/values.ts";

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
  | { readonly effect: "overwrite" };

/** A member of a literal or an enum, as zod holds it. */
export type ZodLiteralValue = core.util.Primitive;

/** How an object or a record treats a key that it does not declare. */
export type OtherKeys = "stripped" | "refused" | "kept";

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
  | { readonly form: "templateLiteral"; readonly schema: ZodSchema }
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
  /** A definition type that this reader does not know (a newer zod). */
  | { readonly form: "unknownDefinition" };

/** A declared member of a `z.object()`, in declaration order. */
export type MemberRead = { readonly name: string; readonly schema: ZodSchema };

/** The number formats that hold an integer: `z.int()`, `z.int32()`, `z.uint32()`, `.int()`. */
const INTEGER_FORMATS: ReadonlySet<string> = new Set(["safeint", "int32", "uint32"]);

/** The check name of a check that replaces the value. */
const OVERWRITE_CHECK = "overwrite";

/** The check name of a number format. */
const NUMBER_FORMAT_CHECK = "number_format";

/** The trait of a schema that is a check itself (a string, number or bigint format, `z.custom()`). */
const CHECK_TRAIT = "$ZodCheck";

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

  return { zodType: String(def.type), checks: [...own, ...checksOf(def.checks ?? [])], ...readForm(def, schema) };
}

/** zod's definition type of a schema (`string`, `pipe`, `lazy`, ...), for a refusal. Pure. */
export function zodTypeOf(schema: ZodSchema): string {
  return String(schema._zod.def.type);
}

/** The node as a check, when it is one (a format). A `z.custom()` test is read as the custom form instead. */
function selfChecks(schema: ZodSchema): readonly CheckRead[] {
  // SAFETY: a schema with the check trait is a `$ZodCheck` too (zod's own rule in `$ZodType.init`).
  return schema._zod.traits.has(CHECK_TRAIT) ? [{ effect: "check", check: schema as unknown as ZodCheck }] : [];
}

/** The values of an enum, as zod checks them (zod's own function): a numeric enum without its reverse keys. */
function enumValues(def: core.$ZodEnumDef): readonly ZodLiteralValue[] {
  return core.util.getEnumValues(def.entries);
}

function checksOf(checks: readonly core.$ZodCheck<never>[]): readonly CheckRead[] {
  return checks.map((check) =>
    check._zod.def.check === OVERWRITE_CHECK ? { effect: "overwrite" } : { effect: "check", check },
  );
}

/** The definition data of a zod schema: one member per definition type of zod's `$ZodTypes`. */
type DefinitionData = core.$ZodTypes["_zod"]["def"];

/** Definition data without the types that an earlier reader took. */
type Without<D extends DefinitionData, T extends DefinitionData["type"]> = Exclude<D, { readonly type: T }>;

function readForm(def: DefinitionData, schema: ZodSchema): DefinitionForm {
  switch (def.type) {
    case "string":
      return { form: "string" };
    case "number":
      return { form: "number", number: numberKindOf(def) };
    case "bigint":
      return { form: "bigint" };
    case "boolean":
      return { form: "boolean" };
    case "null":
      return { form: "null" };
    case "literal":
      return { form: "literal", members: [...def.values] };
    case "enum":
      return { form: "literal", members: enumValues(def) };
    case "template_literal":
      return { form: "templateLiteral", schema };
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

/** `z.int()`, `z.int32()`, `z.uint32()` and `.int()` hold an integer; every other number is a float. */
function numberKindOf(def: core.$ZodNumberDef): "int" | "float" {
  const formats = [def, ...(def.checks ?? []).map((check) => check._zod.def)].map(numberFormatOf);

  return formats.some((format) => INTEGER_FORMATS.has(format)) ? "int" : "float";
}

function numberFormatOf(def: core.$ZodTypeDef | core.$ZodCheckDef): string {
  return "check" in def && def.check === NUMBER_FORMAT_CHECK && "format" in def && isString(def.format)
    ? def.format
    : "";
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

/** No catchall: unknown keys are stripped; `z.never()`: refused (`z.strictObject`); anything else: kept. */
function otherKeysOf(catchall: ZodSchema | undefined): OtherKeys {
  if (catchall === undefined) {
    return "stripped";
  }

  // SAFETY: a catchall is a zod schema, a member of `$ZodTypes`.
  return (catchall as core.$ZodTypes)._zod.def.type === "never" ? "refused" : "kept";
}

function readRecord(def: core.$ZodRecordDef): DefinitionForm {
  const partial = "partial" in def && def.partial === true;
  const loose = "mode" in def && def.mode === "loose";
  // SAFETY: a record key schema is a zod schema (`$ZodRecordKey` is a `$ZodType`).
  const key = def.keyType as ZodSchema;

  return {
    form: "record",
    key,
    value: def.valueType,
    keyCoverage: key._zod.values !== undefined && !partial ? "everyEnumKey" : "anyKeys",
    otherKeys: loose ? "kept" : "refused",
  };
}

function readUnion(def: core.$ZodUnionDef): DefinitionForm {
  const options = [...def.options];

  if (!("discriminator" in def) || !isString(def.discriminator)) {
    return { form: "union", options };
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
