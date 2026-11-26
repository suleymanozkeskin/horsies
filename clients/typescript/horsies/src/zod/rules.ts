// The refusals of the zod adapter: a zod form that has no stored form, or that the codec cannot read back exactly.
// `Horsies.create` reports each one with the code of a task signature refusal (`HRS-105`).

import { unreachable } from "../wire/unreachable.ts";

/** Why the adapter refuses a zod schema. */
export type ZodRule =
  /** `z.any()`, `z.unknown()`: the type names no stored form. */
  | "UNTYPED"
  /**
   * A one-way change of the value: `.transform()`, `z.preprocess()`, `.pipe()` that is not a codec, `z.success()`,
   * `z.coerce.*()`, and the checks that replace the value (`.trim()`, `.toLowerCase()`, `.toUpperCase()`,
   * `.normalize()`, `.overwrite()`).
   */
  | "TRANSFORM"
  /** `.catch()`: it hides a decode failure. */
  | "CATCH"
  /** `z.date()`: a `Date` holds milliseconds only. */
  | "DATE"
  /** An object or a record that keeps undeclared keys: `z.looseObject()`, `.passthrough()`, `.catchall()`. */
  | "LOOSE_OBJECT"
  /**
   * A value that is not data: `z.map()`, `z.set()`, `z.symbol()`, `z.function()`, `z.promise()`, `z.file()`,
   * `z.instanceof()`, `z.custom()` outside a codec output, `z.nan()`, `z.never()`, `z.void()`, `z.undefined()`.
   */
  | "NOT_DATA"
  /** `z.union()` with an object member, or `z.discriminatedUnion()` that tries each option for an unknown tag. */
  | "UNDISCRIMINATED_UNION"
  /** `z.union()` of a primitive and an array or a record. */
  | "MIXED_UNION"
  /** A union with an integer member and a float member: one JavaScript `number` cannot say which it is. */
  | "INT_FLOAT_UNION"
  /** A record key schema that is not a string, a string format, a string enum or a string literal. */
  | "RECORD_KEY"
  /** An object member whose name only the engine writes. */
  | "ENGINE_KEY"
  /** A definition type that this version of horsies does not know. */
  | "UNKNOWN_DEFINITION"
  /** A definition field, or a field value, that this version of horsies does not know (`name` is the field). */
  | "UNKNOWN_DEFINITION_FIELD"
  /** A string, number or bigint format that this version of horsies does not know (`name` is the format). */
  | "UNKNOWN_FORMAT"
  /** A check that this version of horsies does not know (`name` is zod's check name). */
  | "UNKNOWN_CHECK"
  /** `z.xor()`: zod refuses a value that two options take; the union kind takes the first. */
  | "EXCLUSIVE_UNION"
  /** `z.intersection()`, `.and()`. */
  | "INTERSECTION"
  /**
   * `.optional()`, `.nullish()`, `.default()` or `.prefault()` outside an object member or a task input parameter:
   * an array, tuple or record item, a union member, a codec input or a task output cannot be left out.
   */
  | "OPTIONAL_NOT_A_MEMBER"
  /** A `.default()` or `.prefault()` whose value changes from one read to the next (a function default). */
  | "CHANGING_DEFAULT"
  /** A schema that contains itself (`z.lazy()`, a getter in an object): the kind tree has no recursive node yet. */
  | "RECURSIVE"
  /** `z.tuple([...], rest)`: the kind tree has no tuple with a rest item. */
  | "TUPLE_REST"
  /** A member of `z.literal()` or `z.enum()` that is not a string, a number, a boolean or `null`. */
  | "LITERAL_MEMBER"
  /** An option of `z.discriminatedUnion()` that is not a `z.object()` or a discriminated union of the same tag. */
  | "UNION_OPTION"
  /** A schema nested deeper than the adapter walks (`MAX_SCHEMA_DEPTH` nodes). */
  | "TOO_DEEP";

/** The rules that name the unknown part of a definition. */
export type NamedZodRule = "UNKNOWN_DEFINITION_FIELD" | "UNKNOWN_FORMAT" | "UNKNOWN_CHECK";

/**
 * A refusal of a zod schema. `path` names the place in the schema (`$` is the whole schema, `.name` a member, `[*]` an
 * array item, `[i]` a tuple item or a union option, `.*` a record value). `zodType` is zod's definition type of the
 * refused node (`any`, `pipe`, `date`, ...). `fix` names the form to use instead. A rule of an unknown definition part
 * also has `name`: the field, the format or the check.
 */
export type ZodSchemaRefused =
  | {
      readonly rule: Exclude<ZodRule, NamedZodRule>;
      readonly path: string;
      readonly zodType: string;
      readonly fix: string;
    }
  | {
      readonly rule: NamedZodRule;
      readonly path: string;
      readonly zodType: string;
      readonly name: string;
      readonly fix: string;
    };

/** A refusal of a zod schema node at `path`. Pure. */
export function zodRefused(rule: Exclude<ZodRule, NamedZodRule>, path: string, zodType: string): ZodSchemaRefused {
  return Object.freeze({ rule, path, zodType, fix: fixOf(rule) });
}

/** A refusal of an unknown part of a zod definition at `path`: `name` is the field, the format or the check. Pure. */
export function zodPartRefused(rule: NamedZodRule, path: string, zodType: string, name: string): ZodSchemaRefused {
  return Object.freeze({ rule, path, zodType, name, fix: fixOf(rule) });
}

/** The text of a refusal: `<path>: zod type <zodType> is refused [<rule>]; <fix>`, with `<rule> <name>` if named. */
export function zodRefusalText(refused: ZodSchemaRefused): string {
  return `${refused.path}: zod type ${refused.zodType} is refused [${ruleText(refused)}]; ${refused.fix}`;
}

function ruleText(refused: ZodSchemaRefused): string {
  switch (refused.rule) {
    case "UNKNOWN_DEFINITION_FIELD":
    case "UNKNOWN_FORMAT":
    case "UNKNOWN_CHECK":
      return `${refused.rule} ${refused.name}`;
    case "UNTYPED":
    case "TRANSFORM":
    case "CATCH":
    case "DATE":
    case "LOOSE_OBJECT":
    case "NOT_DATA":
    case "UNDISCRIMINATED_UNION":
    case "MIXED_UNION":
    case "INT_FLOAT_UNION":
    case "RECORD_KEY":
    case "ENGINE_KEY":
    case "UNKNOWN_DEFINITION":
    case "EXCLUSIVE_UNION":
    case "INTERSECTION":
    case "OPTIONAL_NOT_A_MEMBER":
    case "CHANGING_DEFAULT":
    case "RECURSIVE":
    case "TUPLE_REST":
    case "LITERAL_MEMBER":
    case "UNION_OPTION":
    case "TOO_DEEP":
      return refused.rule;
    default:
      return unreachable(refused, "ruleText");
  }
}

/** The form to use instead of a refused one. Pure. */
function fixOf(rule: ZodRule): string {
  switch (rule) {
    case "UNTYPED":
      return "use h.json() or z.json() for untyped JSON";
    case "TRANSFORM":
      return "use z.codec() with a decode and an encode; put a check on the schema itself (z.email(), not .pipe())";
    case "CATCH":
      return "remove .catch(): it hides a decode failure";
    case "DATE":
      return "use hz.instant(): a Date holds milliseconds only";
    case "LOOSE_OBJECT":
      return "use z.object() or z.strictObject(): undeclared keys are refused";
    case "NOT_DATA":
      return "name a data type";
    case "UNDISCRIMINATED_UNION":
      return "use z.discriminatedUnion() for a union of objects";
    case "MIXED_UNION":
      return "use z.discriminatedUnion() of objects for a union of a primitive and a container";
    case "INT_FLOAT_UNION":
      return "use one number kind: z.int() or z.number()";
    case "RECORD_KEY":
      return "use string keys: z.string(), a string format, or z.enum([...]) of strings";
    case "ENGINE_KEY":
      return "rename the member: keys that start with __h_ or __horsies_ belong to the engine";
    case "UNKNOWN_DEFINITION":
    case "UNKNOWN_DEFINITION_FIELD":
    case "UNKNOWN_FORMAT":
    case "UNKNOWN_CHECK":
      return "use a zod release that this horsies supports, or h.* for this value";
    case "EXCLUSIVE_UNION":
      return "use z.union() of members that store different JSON types, or z.discriminatedUnion()";
    case "INTERSECTION":
      return "merge the members into one z.object()";
    case "OPTIONAL_NOT_A_MEMBER":
      return "use .nullable(), or make the value an object member";
    case "CHANGING_DEFAULT":
      return "give a fixed default value; a sender sets a value that changes";
    case "RECURSIVE":
      return "use h.json() for a recursive value";
    case "TUPLE_REST":
      return "use z.array(), or a tuple of fixed length";
    case "LITERAL_MEMBER":
      return "use string, number, boolean or null members";
    case "UNION_OPTION":
      return "make each option a z.object() with the tag member";
    case "TOO_DEEP":
      return "nest fewer schemas";
    default:
      return unreachable(rule, "fixOf");
  }
}
