// The zod adapter: the kind tree of a zod 4 schema, or a refusal for each form that has no stored form.
//
// Rules:
// - Each accepted form maps to one kind of the kind tree (`src/wire/kinds.ts`); each other form is a refusal with a
//   rule of `rules.ts`, the path in the schema and zod's definition type. There is no fallback to untyped JSON.
// - zod checks (`.refine()`, `.min()`, formats) map to `checked` nodes; a zod codec maps to a `conversion` node whose
//   stored kind is the kind of the codec's input; an `hz.*` codec maps to its `h.*` kind, found by its brand
//   (`brand.ts`).
// - `.optional()`, `.default()` and `.prefault()` are a member's presence: valid for an object member or a task input
//   parameter only. A default is a computed default: zod reads `defaultValue` again at each parse (a function default
//   gives a new value, a fixed one a copy), so the reader reads it at each worker-form decode, as zod does. `checkKind`
//   reads it once at `Horsies.create`, to refuse a default that does not encode.
// - A recursive schema (`z.lazy()`, an object with a getter member) compiles to a `lazy` kind: a schema that the walk
//   meets again inside itself is a lazy kind of that schema's compiled kind.
// - Unknown keys: every object kind refuses them at decode, also for a `z.object()`, which zod would strip.
// - The kinds that `checkKind` refuses are refused there, at `Horsies.create`, with the rules of `h.*`: an
//   integer-like member name (`IntegerLikeMember`: JavaScript puts it first, so the declared order cannot hold), an
//   ambiguous union, a literal that is not a safe integer, a default that does not encode, a kind deeper than a
//   stored value.

import { MAX_VALUE_JSON_DEPTH } from "../codec/bounds.ts";
import { issuesText, type ValueCheck } from "../codec/conversion.ts";
import { isEngineKey } from "../codec/engine-keys.ts";
import { encodeRefused, itemPath, memberPath, ROOT_PATH } from "../codec/refusal.ts";
import { isBoolean, isNumber, isPlainObject, isString } from "../codec/values.ts";
import { encodeValue } from "../codec/writer.ts";
import {
  BIGINT_KIND,
  BOOLEAN_KIND,
  checkedKind,
  type ComputeDefault,
  conversionKind,
  type DefaultText,
  FLOAT_KIND,
  INT_KIND,
  JSON_KIND,
  type Kind,
  type LazyKind,
  type LiteralMember,
  type Member,
  type MemberPresence,
  NO_TUPLE_REST,
  NULL_KIND,
  nullableKind,
  type ObjectKind,
  objectKind,
  OPTIONAL,
  REQUIRED,
  STRING_KIND,
  taggedUnionKind,
  tagValues,
  tupleKind,
  type TupleRest,
} from "../wire/kinds.ts";
import { lazyKind } from "../wire/lazy.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { hzKindOf } from "./brand.ts";
import {
  type DefinitionForm,
  type MemberRead,
  type OtherKeys,
  readDefinition,
  type TupleRest as ZodTupleRest,
  type ZodDefinition,
  type ZodLiteralValue,
  type ZodSchema,
  zodTypeOf,
} from "./definition.ts";
import { type NamedZodRule, type ZodRule, type ZodSchemaRefused, zodPartRefused, zodRefused } from "./rules.ts";
import { checksRun, codecRuns, decodeRun, parseRun, recordKeysRun } from "./runs.ts";

/** The nodes of one level of a stored value that the walk allows for: the array or object, and four wrappers. */
const NODES_PER_LEVEL = 5;

/**
 * The deepest schema that the adapter walks, in nodes: the core's bound of a user value (`MAX_VALUE_JSON_DEPTH`
 * levels of arrays and objects) with `NODES_PER_LEVEL` nodes per level.
 */
export const MAX_SCHEMA_DEPTH: number = MAX_VALUE_JSON_DEPTH * NODES_PER_LEVEL;

/** The kind of a zod schema, and how it is present as an object member or a task input parameter. */
export type ZodMemberKind = { readonly kind: Kind; readonly presence: MemberPresence };

/** The refusals of a schema, in walk order. */
type Refusals = readonly ZodSchemaRefused[];

type Compiled<T> = Result<T, Refusals>;

/** The definition of one form. */
type FormOf<F extends DefinitionForm["form"]> = Extract<ZodDefinition, { readonly form: F }>;

/**
 * A schema on the walk path: the lazy kind that a schema inside it refers to (a recursion), filled when the schema
 * has compiled.
 */
type RecursionPoint = {
  /** The lazy kind of the schema, for a reference from inside it; marks the point as referred to. */
  readonly reference: () => LazyKind;
  /** Whether a schema inside it referred to it. */
  readonly referred: () => boolean;
  /** Sets the compiled kind of the schema: the kind that the lazy kind gives. */
  readonly close: (kind: Kind) => void;
};

/** The kind of a recursion point: open while its schema compiles, then the compiled kind. */
type PointTarget = { readonly state: "open" } | { readonly state: "closed"; readonly kind: Kind };

const OPEN_POINT: PointTarget = Object.freeze({ state: "open" });

/** Where the walk is: the path in the schema, the depth in nodes, and the schemas on the path (to find recursion). */
type Walk = {
  readonly path: string;
  readonly depth: number;
  readonly onPath: ReadonlyMap<ZodSchema, RecursionPoint>;
};

const START: Walk = Object.freeze({ path: ROOT_PATH, depth: 0, onPath: new Map<ZodSchema, RecursionPoint>() });

/**
 * The kind and the presence of a schema as an object member or a task input parameter.
 *
 * Fails with every refusal of the schema, in walk order. Reads no default value (the reader and `checkKind` read
 * them) and calls the getter of each `z.lazy()` once. Does no I/O.
 */
export function compileMember(schema: ZodSchema): Compiled<ZodMemberKind> {
  return compileNode(schema, START);
}

/**
 * The kind of a schema as a whole value (a task output, a workflow parameter value): `.optional()`, `.default()` and
 * `.prefault()` are refused at its top. Fails and reads as `compileMember`.
 */
export function compileValue(schema: ZodSchema): Compiled<Kind> {
  return valueAt(schema, START);
}

function compileNode(schema: ZodSchema, walk: Walk): Compiled<ZodMemberKind> {
  if (walk.depth >= MAX_SCHEMA_DEPTH) {
    return refused("TOO_DEEP", walk.path, zodTypeOf(schema));
  }

  const point = walk.onPath.get(schema);

  if (point !== undefined) {
    return required(point.reference());
  }

  const own = recursionPoint();
  const definition = readDefinition(schema);
  const inside: Walk = { path: walk.path, depth: walk.depth + 1, onPath: new Map([...walk.onPath, [schema, own]]) };
  const compiled = compileForm(definition, schema, inside);
  const checked = compiled.ok ? withChecks(compiled.value, definition, walk.path) : compiled;

  return checked.ok ? closedAt(own, checked.value, walk.path, definition.zodType) : checked;
}

/**
 * A new recursion point. Its lazy kind gives the kind that `close` sets; a walk of that lazy kind before `close` is a
 * defect of the adapter (the compile is not done), so its getter throws a `TypeError` then.
 */
function recursionPoint(): RecursionPoint {
  let referred = false;
  let target: PointTarget = OPEN_POINT;

  const lazy = lazyKind(() => {
    if (target.state === "open") {
      throw new TypeError("compileMember: a lazy kind was walked before its recursive schema compiled");
    }

    return target.kind;
  });

  return {
    reference: () => {
      referred = true;

      return lazy;
    },
    referred: () => referred,
    close: (kind) => {
      target = { state: "closed", kind };
    },
  };
}

/**
 * The compiled schema at a recursion point that the walk left. A schema that a schema inside it referred to gives its
 * kind to the lazy kind of that reference; it must be always present there (`RECURSIVE_PRESENCE`): the reference is an
 * item or a member of its own.
 */
function closedAt(point: RecursionPoint, compiled: ZodMemberKind, path: string, zodType: string): Compiled<ZodMemberKind> {
  if (!point.referred()) {
    return ok(compiled);
  }

  if (compiled.presence.mode !== "required") {
    return refused("RECURSIVE_PRESENCE", path, zodType);
  }

  point.close(compiled.kind);

  return ok(compiled);
}

/** A schema as an item, a union option, a codec input or a whole value: it is always present. */
function valueAt(schema: ZodSchema, walk: Walk): Compiled<Kind> {
  const compiled = compileNode(schema, walk);

  if (!compiled.ok) {
    return compiled;
  }

  switch (compiled.value.presence.mode) {
    case "required":
      return ok(compiled.value.kind);
    case "optional":
    case "defaulted":
      return refused("OPTIONAL_NOT_A_MEMBER", walk.path, zodTypeOf(schema));
    default:
      return unreachable(compiled.value.presence, "valueAt");
  }
}

/** The node's own checks around its kind: a `checked` node, or the `TRANSFORM` refusal of a check that replaces. */
function withChecks(compiled: ZodMemberKind, definition: ZodDefinition, path: string): Compiled<ZodMemberKind> {
  const checks = [];

  for (const read of definition.checks) {
    switch (read.effect) {
      case "check":
        checks.push(read.check);
        break;
      case "overwrite":
        return refused("TRANSFORM", path, definition.zodType);
      case "unknownCheck":
        return partRefused("UNKNOWN_CHECK", path, definition.zodType, read.name);
      case "unknownFormat":
        return partRefused("UNKNOWN_FORMAT", path, definition.zodType, read.name);
      default:
        return unreachable(read, "withChecks");
    }
  }

  return checks.length === 0
    ? ok(compiled)
    : ok({ kind: checkedKind(compiled.kind, checksRun(checks)), presence: compiled.presence });
}

function compileForm(definition: ZodDefinition, schema: ZodSchema, walk: Walk): Compiled<ZodMemberKind> {
  switch (definition.form) {
    case "string":
      return required(STRING_KIND);
    case "number":
      return required(definition.number === "int" ? INT_KIND : FLOAT_KIND);
    case "bigint":
      return required(BIGINT_KIND);
    case "boolean":
      return required(BOOLEAN_KIND);
    case "null":
      return required(NULL_KIND);
    case "literal":
      return requiredOf(literalKind(definition.members, walk.path, definition.zodType));
    case "templateLiteral":
      return required(checkedKind(STRING_KIND, parseRun(definition.pattern)));
    case "object":
      return requiredOf(compileObject(definition, walk));
    case "array":
      return requiredOf(mapped(valueAt(definition.item, at(walk, `${walk.path}[*]`)), arrayKind));
    case "tuple":
      return requiredOf(compileTuple(definition, walk));
    case "record":
      return requiredOf(compileRecord(definition, walk));
    case "union":
      return requiredOf(compileUnion(definition.options, walk, definition.zodType));
    case "taggedUnion":
      return requiredOf(compileTagged(definition, walk));
    case "codec":
      return requiredOf(compileCodec(definition, walk));
    case "nullable":
    case "optional":
    case "nonoptional":
    case "default":
    case "prefault":
    case "readonly":
    case "lazy":
    case "custom":
    case "untyped":
    case "transform":
    case "catch":
    case "date":
    case "intersection":
    case "notData":
    case "coerced":
    case "exclusiveUnion":
    case "unknownDefinition":
    case "unknownField":
      return compileWrapper(definition, schema, walk);
    default:
      return unreachable(definition, "compileForm");
  }
}

/** The forms that `compileForm` passes on to `compileWrapper`: the wrappers, and the forms that have no kind. */
type WrapperForm =
  | "nullable"
  | "optional"
  | "nonoptional"
  | "default"
  | "prefault"
  | "readonly"
  | "lazy"
  | "custom"
  | "untyped"
  | "transform"
  | "catch"
  | "date"
  | "intersection"
  | "notData"
  | "coerced"
  | "exclusiveUnion"
  | "unknownDefinition"
  | "unknownField";

function compileWrapper(definition: FormOf<WrapperForm>, schema: ZodSchema, walk: Walk): Compiled<ZodMemberKind> {
  switch (definition.form) {
    case "nullable":
      return mapped(compileNode(definition.inner, walk), (inner) => ({
        kind: nullableKind(inner.kind),
        presence: inner.presence,
      }));
    case "optional":
      return mapped(compileNode(definition.inner, walk), (inner) => ({
        kind: inner.kind,
        presence: inner.presence.mode === "defaulted" ? inner.presence : OPTIONAL,
      }));
    case "nonoptional":
      return mapped(compileNode(definition.inner, walk), (inner) => ({ kind: inner.kind, presence: REQUIRED }));
    case "default":
    case "prefault":
      return compileDefault(definition, schema, walk);
    case "readonly":
      return compileNode(definition.inner, walk);
    case "lazy":
      return isZodJson(schema, definition.inner) ? required(JSON_KIND) : compileNode(definition.inner, walk);
    case "custom":
      return compileCustom(definition.test, walk.path, definition.zodType);
    case "untyped":
    case "transform":
    case "catch":
    case "date":
    case "intersection":
    case "notData":
    case "coerced":
    case "exclusiveUnion":
    case "unknownDefinition":
    case "unknownField":
      return refusedForm(definition, walk.path);
    default:
      return unreachable(definition, "compileWrapper");
  }
}

function refusedForm(definition: ZodDefinition, path: string): Compiled<ZodMemberKind> {
  return definition.form === "unknownField"
    ? partRefused("UNKNOWN_DEFINITION_FIELD", path, definition.zodType, definition.field)
    : refused(ruleOf(definition), path, definition.zodType);
}

/** The rule of a form that has no kind. A form that has one is a defect of the caller. */
function ruleOf(form: DefinitionForm): Exclude<ZodRule, NamedZodRule> {
  switch (form.form) {
    case "untyped":
      return "UNTYPED";
    case "transform":
      return "TRANSFORM";
    case "catch":
      return "CATCH";
    case "date":
      return "DATE";
    case "intersection":
      return "INTERSECTION";
    case "notData":
      return "NOT_DATA";
    case "unknownDefinition":
      return "UNKNOWN_DEFINITION";
    case "coerced":
      return "TRANSFORM";
    case "exclusiveUnion":
      return "EXCLUSIVE_UNION";
    case "unknownField":
    case "string":
    case "number":
    case "bigint":
    case "boolean":
    case "null":
    case "literal":
    case "templateLiteral":
    case "object":
    case "array":
    case "tuple":
    case "record":
    case "union":
    case "taggedUnion":
    case "nullable":
    case "optional":
    case "nonoptional":
    case "default":
    case "prefault":
    case "readonly":
    case "lazy":
    case "codec":
    case "custom":
      throw new TypeError(`ruleOf: the form ${form.form} has a kind or a named rule`);
    default:
      return unreachable(form, "ruleOf");
  }
}

function at(walk: Walk, path: string): Walk {
  return { ...walk, path };
}

function refused(rule: Exclude<ZodRule, NamedZodRule>, path: string, zodType: string): Compiled<never> {
  return err([zodRefused(rule, path, zodType)]);
}

function partRefused(rule: NamedZodRule, path: string, zodType: string, name: string): Compiled<never> {
  return err([zodPartRefused(rule, path, zodType, name)]);
}

function required(kind: Kind): Compiled<ZodMemberKind> {
  return ok({ kind, presence: REQUIRED });
}

function requiredOf(kind: Compiled<Kind>): Compiled<ZodMemberKind> {
  return kind.ok ? required(kind.value) : kind;
}

function mapped<T, U>(compiled: Compiled<T>, map: (value: T) => U): Compiled<U> {
  return compiled.ok ? ok(map(compiled.value)) : compiled;
}

/** Every value, or every refusal of the list, in list order. */
function allOf<T>(results: readonly Compiled<T>[]): Compiled<readonly T[]> {
  const refusals = results.flatMap((result) => (result.ok ? [] : result.error));

  return refusals.length > 0 ? err(refusals) : ok(results.flatMap((result) => (result.ok ? [result.value] : [])));
}

function arrayKind(item: Kind): Kind {
  return Object.freeze({ node: "array", item });
}

function isLiteralMember(value: ZodLiteralValue): value is LiteralMember {
  return value === null || isString(value) || isNumber(value) || isBoolean(value);
}

function literalKind(values: readonly ZodLiteralValue[], path: string, zodType: string): Compiled<Kind> {
  const members = values.filter(isLiteralMember);

  return members.length === values.length && members.length > 0
    ? ok(Object.freeze({ node: "literal", members: Object.freeze(members) }))
    : refused("LITERAL_MEMBER", path, zodType);
}

function compileObject(definition: FormOf<"object">, walk: Walk): Compiled<Kind> {
  const loose = otherKeysRefusals(definition.otherKeys, walk.path, definition.zodType);
  const compiled = allOf(definition.members.map((member) => compileObjectMember(member, walk, definition.zodType)));
  const refusals = [...loose, ...(compiled.ok ? [] : compiled.error)];

  return refusals.length > 0 || !compiled.ok ? err(refusals) : ok(objectKind(compiled.value));
}

/** The refusal of an object or a record that keeps undeclared keys. */
function otherKeysRefusals(otherKeys: OtherKeys, path: string, zodType: string): Refusals {
  switch (otherKeys) {
    case "refused":
      return [];
    case "kept":
      return [zodRefused("LOOSE_OBJECT", path, zodType)];
    default:
      return unreachable(otherKeys, "otherKeysRefusals");
  }
}

function compileObjectMember(member: MemberRead, walk: Walk, zodType: string): Compiled<Member> {
  const path = memberPath(walk.path, member.name);

  if (isEngineKey(member.name)) {
    return refused("ENGINE_KEY", path, zodType);
  }

  return mapped(compileNode(member.schema, at(walk, path)), (compiled) => ({ name: member.name, ...compiled }));
}

/** A tuple: its fixed items, then the rest item of `z.tuple([...], rest)` at `[*]`. */
function compileTuple(definition: FormOf<"tuple">, walk: Walk): Compiled<Kind> {
  const items = allOf(definition.items.map((item, index) => valueAt(item, at(walk, itemPath(walk.path, index)))));
  const rest = compileTupleRest(definition.rest, walk);
  const refusals = [...(items.ok ? [] : items.error), ...(rest.ok ? [] : rest.error)];

  return items.ok && rest.ok ? ok(tupleKind(items.value, rest.value)) : err(refusals);
}

function compileTupleRest(rest: ZodTupleRest, walk: Walk): Compiled<TupleRest> {
  switch (rest.state) {
    case "none":
      return ok(NO_TUPLE_REST);
    case "items":
      return mapped(valueAt(rest.item, at(walk, `${walk.path}[*]`)), (item) => Object.freeze({ state: "items", item }));
    default:
      return unreachable(rest, "compileTupleRest");
  }
}

/**
 * A record of string keys. A key schema with checks, a string enum or a string literal adds a check of the keys; an
 * enum key of `z.record()` (not `z.partialRecord()`) requires each member as a key, as zod does.
 */
function compileRecord(definition: FormOf<"record">, walk: Walk): Compiled<Kind> {
  const key = valueAt(definition.key, walk);
  const value = valueAt(definition.value, at(walk, `${walk.path}.*`));
  const loose = otherKeysRefusals(definition.otherKeys, walk.path, definition.zodType);
  const keys = key.ok ? recordKeyOf(key.value) : NOT_STRING_KEY;

  const keyRefusals: Refusals =
    keys.keys === "notString" ? [zodRefused("RECORD_KEY", walk.path, definition.zodType)] : [];

  const refusals = [...loose, ...(key.ok ? keyRefusals : key.error), ...(value.ok ? [] : value.error)];

  if (refusals.length > 0 || !key.ok || !value.ok) {
    return err(refusals);
  }

  const record: Kind = Object.freeze({ node: "record", value: value.value });
  const requiredKeys = definition.keyCoverage === "everyEnumKey" && keys.keys === "literal" ? keys.members : [];

  return ok(
    key.value.node === "string" && requiredKeys.length === 0
      ? record
      : checkedKind(record, recordKeysRun(definition.key, requiredKeys)),
  );
}

/** A key kind that a record stores as it is: a string, maybe checked, or a literal of strings. */
/** What a record key kind stores as it is: any string, the strings of a literal, or no string key. */
type RecordKey =
  | { readonly keys: "anyString" }
  | { readonly keys: "literal"; readonly members: readonly string[] }
  | { readonly keys: "notString" };

const ANY_STRING_KEY: RecordKey = Object.freeze({ keys: "anyString" });

const NOT_STRING_KEY: RecordKey = Object.freeze({ keys: "notString" });

function recordKeyOf(kind: Kind): RecordKey {
  switch (kind.node) {
    case "string":
      return ANY_STRING_KEY;
    case "checked":
      return recordKeyOf(kind.inner);
    case "literal": {
      const members = kind.members.filter(isString);

      return members.length === kind.members.length ? { keys: "literal", members } : NOT_STRING_KEY;
    }

    case "null":
    case "boolean":
    case "int":
    case "bigint":
    case "float":
    case "uuid":
    case "decimal":
    case "instant":
    case "offsetDateTime":
    case "plainDateTime":
    case "plainDate":
    case "plainTime":
    case "union":
    case "taggedUnion":
    case "nullable":
    case "array":
    case "tuple":
    case "record":
    case "object":
    case "json":
    case "taskOutcome":
    case "conversion":
    case "lazy":
      return NOT_STRING_KEY;
    default:
      return unreachable(kind, "recordKeyOf");
  }
}

/** What a union member stores, for the union rules. */
type StoredForm = "object" | "container" | "int" | "float" | "primitive";

function storedForms(kind: Kind): readonly StoredForm[] {
  switch (kind.node) {
    case "object":
    case "taggedUnion":
      return ["object"];
    case "array":
    case "tuple":
    case "record":
    case "json":
    case "taskOutcome":
      return ["container"];
    case "int":
    case "bigint":
      return ["int"];
    case "float":
      return ["float"];
    case "null":
    case "boolean":
    case "string":
    case "uuid":
    case "decimal":
    case "instant":
    case "offsetDateTime":
    case "plainDateTime":
    case "plainDate":
    case "plainTime":
    case "literal":
      return ["primitive"];
    case "nullable":
    case "checked":
      return storedForms(kind.inner);
    case "conversion":
      return storedForms(kind.stored);
    case "union":
      return kind.members.flatMap(storedForms);
    case "lazy":
      // A reference to a schema that holds it: an array, a record, an object, or a level-free cycle that `checkKind`
      // refuses. Its kind is not compiled yet.
      return ["container"];
    default:
      return unreachable(kind, "storedForms");
  }
}

/** A union of primitives. An object member needs a discriminated union; a container member, too. */
function compileUnion(options: readonly ZodSchema[], walk: Walk, zodType: string): Compiled<Kind> {
  const members = allOf(options.map((option, index) => valueAt(option, at(walk, itemPath(walk.path, index)))));

  if (!members.ok) {
    return members;
  }

  const forms = new Set(members.value.flatMap(storedForms));

  if (forms.has("object")) {
    return refused("UNDISCRIMINATED_UNION", walk.path, zodType);
  }

  if (forms.has("container")) {
    return refused("MIXED_UNION", walk.path, zodType);
  }

  if (forms.has("int") && forms.has("float")) {
    return refused("INT_FLOAT_UNION", walk.path, zodType);
  }

  return ok(Object.freeze({ node: "union", members: Object.freeze([...members.value]) }));
}

/** An object variant of a discriminated union, and the checks of its own node (a refine on the option). */
type Variant = { readonly object: ObjectKind; readonly checks: readonly ValueCheck[] };

function compileTagged(definition: FormOf<"taggedUnion">, walk: Walk): Compiled<Kind> {
  switch (definition.unmatchedTag) {
    case "refused":
      return compileVariants(definition, walk);
    case "triesEachOption":
      return refused("UNDISCRIMINATED_UNION", walk.path, definition.zodType);
    default:
      return unreachable(definition.unmatchedTag, "compileTagged");
  }
}

function compileVariants(definition: FormOf<"taggedUnion">, walk: Walk): Compiled<Kind> {
  const options = definition.options.map((option, index) => {
    const path = itemPath(walk.path, index);
    const compiled = valueAt(option, at(walk, path));

    return compiled.ok ? variantsOf(compiled.value, definition.tag, [], path, zodTypeOf(option)) : compiled;
  });

  const variants = allOf(options);

  if (!variants.ok) {
    return variants;
  }

  const flat = variants.value.flat();
  const tagged = taggedUnionKind(definition.tag, flat.map((variant) => variant.object));

  const checked = flat.some((variant) => variant.checks.length > 0);

  return ok(checked ? checkedKind(tagged, tagDispatch(definition.tag, flat)) : tagged);
}

/** The object variants of an option: an object, its checks peeled off, or a tagged union of the same tag. */
function variantsOf(
  kind: Kind,
  tag: string,
  checks: readonly ValueCheck[],
  path: string,
  zodType: string,
): Compiled<readonly Variant[]> {
  switch (kind.node) {
    case "object":
      return ok([{ object: kind, checks }]);
    case "checked":
      return variantsOf(kind.inner, tag, [...checks, kind.check], path, zodType);
    case "taggedUnion":
      return kind.tag === tag
        ? ok(kind.variants.map((object) => ({ object, checks })))
        : refused("UNION_OPTION", path, zodType);
    case "null":
    case "boolean":
    case "int":
    case "bigint":
    case "float":
    case "string":
    case "uuid":
    case "decimal":
    case "instant":
    case "offsetDateTime":
    case "plainDateTime":
    case "plainDate":
    case "plainTime":
    case "literal":
    case "union":
    case "nullable":
    case "array":
    case "tuple":
    case "record":
    case "json":
    case "taskOutcome":
    case "conversion":
    case "lazy":
      return refused("UNION_OPTION", path, zodType);
    default:
      return unreachable(kind, "variantsOf");
  }
}

/** A check of a tagged union value: the checks of the variant that its tag names. Another value passes. */
function tagDispatch(tag: string, variants: readonly Variant[]): ValueCheck {
  const byTag = new Map<string, readonly ValueCheck[]>();

  for (const variant of variants) {
    for (const value of tagValues(variant.object, tag)) {
      if (!byTag.has(value)) {
        byTag.set(value, variant.checks);
      }
    }
  }

  return (value) => {
    const tagValue = isPlainObject(value) ? value[tag] : undefined;
    const checks = isString(tagValue) ? (byTag.get(tagValue) ?? []) : [];

    for (const check of checks) {
      const checked = check(value);

      if (!checked.ok) {
        return checked;
      }
    }

    return ok(true);
  };
}

function compileCodec(definition: FormOf<"codec">, walk: Walk): Compiled<Kind> {
  const known = hzKindOf(definition.decode);

  switch (known.found) {
    case "hz":
      return ok(known.kind);
    case "none":
      return mapped(valueAt(definition.input, walk), (stored) => {
        const runs = codecRuns(definition.output, definition.decode, definition.encode);

        return conversionKind(stored, runs.decode, runs.encode);
      });
    default:
      return unreachable(known, "compileCodec");
  }
}

function compileCustom(test: object, path: string, zodType: string): Compiled<ZodMemberKind> {
  const known = hzKindOf(test);

  switch (known.found) {
    case "hz":
      return required(known.kind);
    case "none":
      return refused("NOT_DATA", path, zodType);
    default:
      return unreachable(known, "compileCustom");
  }
}

/**
 * A member that a sender may leave out and a reader fills: a computed default. zod reads `defaultValue` again at each
 * parse, and its definition does not tell a function default from a fixed value, so each worker-form decode reads it
 * the same way (`computeDefault`). The schema of the default node is the default's `source`.
 */
function compileDefault(
  definition: FormOf<"default" | "prefault">,
  schema: ZodSchema,
  walk: Walk,
): Compiled<ZodMemberKind> {
  const inner = compileNode(definition.inner, walk);

  if (!inner.ok) {
    return inner;
  }

  const fallback: DefaultText = Object.freeze({
    state: "computed",
    source: schema,
    compute: computeDefault(definition, inner.value.kind),
  });

  return ok({ kind: inner.value.kind, presence: Object.freeze({ mode: "defaulted", fallback }) });
}

/**
 * One read of a default as zod reads it at a parse, as its stored text. A `.prefault()` value is an input value: zod
 * decodes it first, as zod does when the key is missing; a value that does not decode is `CheckFailed`.
 */
function computeDefault(definition: FormOf<"default" | "prefault">, kind: Kind): ComputeDefault {
  switch (definition.form) {
    case "default":
      return () => encodeValue(kind, definition.value());
    case "prefault": {
      const decode = decodeRun(definition.inner);

      return () => {
        const decoded = decode(definition.value());
        const detail = decoded.ok ? "" : `the prefault value does not decode: ${issuesText(decoded.error)}`;

        return decoded.ok ? encodeValue(kind, decoded.value) : err(encodeRefused("CheckFailed", ROOT_PATH, detail));
      };
    }

    default:
      return unreachable(definition, "computeDefault");
  }
}

/** Whether a lazy schema is zod's `z.json()`: a union of string, number, boolean, null, an array and a record of it. */
function isZodJson(lazy: ZodSchema, inner: ZodSchema): boolean {
  const union = readDefinition(inner);

  if (union.form !== "union" || union.checks.length > 0) {
    return false;
  }

  const [text, number, flag, empty, list, map, ...rest] = union.options.map(readDefinition);

  return (
    rest.length === 0 &&
    [text, number, flag, empty, list, map].every((option) => option !== undefined && option.checks.length === 0) &&
    text?.form === "string" &&
    number?.form === "number" &&
    number.number === "float" &&
    flag?.form === "boolean" &&
    empty?.form === "null" &&
    list?.form === "array" &&
    list.item === lazy &&
    map?.form === "record" &&
    map.value === lazy &&
    readDefinition(map.key).form === "string"
  );
}
