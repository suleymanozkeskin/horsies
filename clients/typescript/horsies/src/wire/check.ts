// The checks of a kind tree that the static types cannot make. `Horsies.create` reports each refusal with the code
// of a task signature refusal.

import { MAX_VALUE_JSON_DEPTH } from "../codec/bounds.ts";
import { isEngineKey } from "../codec/engine-keys.ts";
import { itemPath, memberPath, ROOT_PATH } from "../codec/refusal.ts";
import { isNumber } from "../codec/values.ts";
import {
  type DefaultText,
  type Kind,
  type LiteralMember,
  type Member,
  type MemberPresence,
  type ObjectKind,
  tagValues,
  type UpstreamOutput,
} from "./kinds.ts";
import { unreachable } from "./unreachable.ts";

/** The canonical decimal text of a non-negative integer of at most 10 digits: `0`, or digits with no leading zero. */
const CANONICAL_INDEX_TEXT = /^(?:0|[1-9][0-9]{0,9})$/;

/** The highest JavaScript array index, 2^32 - 2. A key of this value or lower is put first in an object. */
const MAX_ARRAY_INDEX = 2 ** 32 - 2;

/** Why a kind tree cannot be a task's wire type. */
export type KindRule =
  /** The value of a `.default(value)` does not encode under its kind (for example `NaN` for `h.float()`). */
  | "DefaultNotStored"
  /** An object member has a name that only the engine writes. */
  | "EngineKeyMember"
  /**
   * An object member has an integer-like name: a JavaScript array index, the canonical decimal text of an integer
   * from 0 to 2^32 - 2 (`"0"`, `"10"`). JavaScript puts such a key first in an object, so the declared member order
   * of `h.object` cannot hold.
   */
  | "IntegerLikeMember"
  /** The kind nests more arrays and objects than a stored value may have, so no value fits it. */
  | "KindTooDeep"
  /** A number member of a literal that is not a safe integer. */
  | "LiteralNotSafeInteger"
  /**
   * Two members of a union store the same JSON type and are not literals: one value cannot say which member it is
   * (`h.int()` with `h.float()`, `h.string()` with `h.instant()`).
   */
  | "AmbiguousUnion"
  /** A member of `h.union([...])` is an array or an object; use `h.union(tag, [...])` for objects. */
  | "UnionMemberNotPrimitive"
  /** A member of `h.union(tag, [...])` has no required string-literal tag member, or repeats a tag value. */
  | "UnionTagInvalid"
  /** `h.taskOutcome` anywhere but as a whole task input parameter (optional or nullable too). */
  | "TaskOutcomePlacement"
  /** An `h.taskOutcome` parameter whose upstream contract has an output with no stored form. */
  | "UpstreamOutputRefused";

/** A refusal of a kind tree. `path` names the place in the type: `[*]` an array item, `.*` a record value. */
export type KindRefused = { readonly rule: KindRule; readonly path: string; readonly detail: string };

/** Every refusal of the kind tree of a value (an output, a member), in walk order. Empty: the kind is valid. Pure. */
export function checkKind(kind: Kind): readonly KindRefused[] {
  const refusals: KindRefused[] = [];

  walk(kind, ROOT_PATH, 0, refusals);

  return refusals;
}

/**
 * Every refusal of the kind of one object member or task input parameter whose name the caller checks: the member's
 * default, then its kind tree, in walk order, at `$`. Pure.
 */
export function checkMemberKind(kind: Kind, presence: MemberPresence): readonly KindRefused[] {
  const refusals: KindRefused[] = [...defaultRefusals(presence, ROOT_PATH)];

  walk(kind, ROOT_PATH, 0, refusals);

  return refusals;
}

/**
 * Every refusal of the kind of a task input, in walk order: a parameter may be an upstream outcome (`h.taskOutcome`),
 * nothing below a parameter may. Pure.
 */
export function checkParameters(parameters: ObjectKind): readonly KindRefused[] {
  const refusals: KindRefused[] = [];

  for (const member of parameters.members) {
    const at = memberPath(ROOT_PATH, member.name);

    refusals.push(...memberRefusals(member, at));
    walkParameter(parameterValue(member.kind), at, refusals);
  }

  return refusals;
}

/** The value of a parameter: its own kind, or the upstream output of an upstream outcome. */
type ParameterValue =
  | { readonly kind: "value"; readonly value: Kind }
  | { readonly kind: "upstream"; readonly output: UpstreamOutput };

function walkParameter(value: ParameterValue, at: string, refusals: KindRefused[]): void {
  switch (value.kind) {
    case "value":
      return walk(value.value, at, 0, refusals);
    case "upstream":
      return walkUpstream(value.output, at, refusals);
    default:
      return unreachable(value, "walkParameter");
  }
}

/** The refusals of the upstream output of a parameter: its kind tree, or the refusal of an output with no kind. */
function walkUpstream(output: UpstreamOutput, at: string, refusals: KindRefused[]): void {
  switch (output.state) {
    case "compiled":
      return walk(output.kind, at, 0, refusals);
    case "refused":
      refusals.push({ rule: "UpstreamOutputRefused", path: at, detail: output.detail });

      return;
    default:
      return unreachable(output, "walkUpstream");
  }
}

/** The value of a parameter of `kind`: the upstream output of an upstream outcome, or the parameter's own kind. */
function parameterValue(kind: Kind): ParameterValue {
  switch (kind.node) {
    case "taskOutcome":
      return { kind: "upstream", output: kind.output };
    case "nullable":
      return kind.inner.node === "taskOutcome"
        ? { kind: "upstream", output: kind.inner.output }
        : { kind: "value", value: kind };
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
    case "taggedUnion":
    case "array":
    case "tuple":
    case "record":
    case "object":
    case "json":
    case "conversion":
    case "checked":
      return { kind: "value", value: kind };
    default:
      return unreachable(kind, "parameterValue");
  }
}

function walk(kind: Kind, path: string, level: number, refusals: KindRefused[]): void {
  switch (kind.node) {
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
    case "json":
      return;
    case "literal":
      return walkLiteral(kind.members, path, refusals);
    case "union":
      return walkUnion(kind.members, path, level, refusals);
    case "taggedUnion":
      return walkTagged(kind.tag, kind.variants, path, level, refusals);
    case "taskOutcome":
      refusals.push({
        rule: "TaskOutcomePlacement",
        path,
        detail: "h.taskOutcome is valid only as a whole task parameter",
      });

      return walkUpstream(kind.output, path, refusals);
    case "conversion":
      return walk(kind.stored, path, level, refusals);
    case "checked":
      return walk(kind.inner, path, level, refusals);
    case "nullable":
      return walk(kind.inner, path, level, refusals);
    case "array":
      return walkInside([kind.item], () => `${path}[*]`, path, level, refusals);
    case "tuple":
      return walkInside(kind.items, (index) => itemPath(path, index), path, level, refusals);
    case "record":
      return walkInside([kind.value], () => `${path}.*`, path, level, refusals);
    case "object":
      return walkMembers(kind.members, path, level, refusals);
    default:
      return unreachable(kind, "checkKind");
  }
}

/** The kinds inside an array, a tuple or a record, one level down. */
function walkInside(
  kinds: readonly Kind[],
  pathOf: (index: number) => string,
  path: string,
  level: number,
  refusals: KindRefused[],
): void {
  if (level + 1 > MAX_VALUE_JSON_DEPTH) {
    refusals.push(tooDeep(path));

    return;
  }

  for (const [index, inner] of kinds.entries()) {
    walk(inner, pathOf(index), level + 1, refusals);
  }
}

function walkMembers(members: readonly Member[], path: string, level: number, refusals: KindRefused[]): void {
  if (level + 1 > MAX_VALUE_JSON_DEPTH) {
    refusals.push(tooDeep(path));

    return;
  }

  for (const member of members) {
    const at = memberPath(path, member.name);

    refusals.push(...memberRefusals(member, at));
    walk(member.kind, at, level + 1, refusals);
  }
}

/** The refusals of a member itself: its name and its default. */
function memberRefusals(member: Member, path: string): readonly KindRefused[] {
  return [...nameRefusals(member.name, path), ...defaultRefusals(member.presence, path)];
}

/** The refusals of a member name: a name that only the engine writes, an integer-like name. */
function nameRefusals(name: string, path: string): readonly KindRefused[] {
  const engineKey: readonly KindRefused[] = isEngineKey(name)
    ? [{ rule: "EngineKeyMember", path, detail: "a member name that only the engine writes" }]
    : [];

  const integerLike: readonly KindRefused[] = isIntegerLikeName(name)
    ? [{ rule: "IntegerLikeMember", path, detail: "an integer-like member name, which JavaScript puts first" }]
    : [];

  return [...engineKey, ...integerLike];
}

/** Whether a name is a JavaScript array index, which an object puts before its other keys. Pure. */
function isIntegerLikeName(name: string): boolean {
  return CANONICAL_INDEX_TEXT.test(name) && Number(name) <= MAX_ARRAY_INDEX;
}

/** The refusal of a default value that the writer refused. */
function defaultRefusals(presence: MemberPresence, path: string): readonly KindRefused[] {
  switch (presence.mode) {
    case "required":
    case "optional":
      return [];
    case "defaulted":
      return storedDefaultRefusals(presence.fallback, path);
    default:
      return unreachable(presence, "defaultRefusals");
  }
}

function storedDefaultRefusals(fallback: DefaultText, path: string): readonly KindRefused[] {
  switch (fallback.state) {
    case "stored":
      return [];
    case "refused":
      return [{ rule: "DefaultNotStored", path, detail: `the default is refused: ${fallback.refusal.refusal}` }];
    default:
      return unreachable(fallback, "storedDefaultRefusals");
  }
}

function walkLiteral(members: readonly LiteralMember[], path: string, refusals: KindRefused[]): void {
  for (const member of members) {
    if (isNumber(member) && !Number.isSafeInteger(member)) {
      refusals.push({ rule: "LiteralNotSafeInteger", path, detail: "a number member that is not a safe integer" });
    }
  }
}

/** The JSON type that a kind stores, for the union rules. A literal is its own class: it makes no union ambiguous. */
type StoredClass = "null" | "boolean" | "number" | "string" | "literal" | "container";

function storedClasses(kind: Kind): readonly StoredClass[] {
  switch (kind.node) {
    case "null":
      return ["null"];
    case "boolean":
      return ["boolean"];
    case "int":
    case "bigint":
    case "float":
      return ["number"];
    case "string":
    case "uuid":
    case "decimal":
    case "instant":
    case "offsetDateTime":
    case "plainDateTime":
    case "plainDate":
    case "plainTime":
      return ["string"];
    case "literal":
      return ["literal"];
    case "nullable":
      return ["null", ...storedClasses(kind.inner)];
    case "union":
      return kind.members.flatMap(storedClasses);
    case "conversion":
      return storedClasses(kind.stored);
    case "checked":
      return storedClasses(kind.inner);
    case "array":
    case "tuple":
    case "record":
    case "object":
    case "taggedUnion":
    case "json":
    case "taskOutcome":
      return ["container"];
    default:
      return unreachable(kind, "storedClasses");
  }
}

function walkUnion(members: readonly Kind[], path: string, level: number, refusals: KindRefused[]): void {
  const counts = new Map<StoredClass, number>();

  for (const member of members) {
    for (const stored of storedClasses(member)) {
      counts.set(stored, (counts.get(stored) ?? 0) + 1);
    }

    walk(member, path, level, refusals);
  }

  for (const [stored, count] of counts) {
    refusals.push(...unionClassRefusals(stored, count, path));
  }
}

function unionClassRefusals(stored: StoredClass, count: number, path: string): readonly KindRefused[] {
  switch (stored) {
    case "container":
      return [{ rule: "UnionMemberNotPrimitive", path, detail: "an array or object member; use h.union(tag, [...])" }];
    case "literal":
      return [];
    case "null":
    case "boolean":
    case "number":
    case "string":
      return count > 1 ? [{ rule: "AmbiguousUnion", path, detail: `${count} members store a JSON ${stored}` }] : [];
    default:
      return unreachable(stored, "unionClassRefusals");
  }
}

function walkTagged(
  tag: string,
  variants: readonly ObjectKind[],
  path: string,
  level: number,
  refusals: KindRefused[],
): void {
  const seen = new Set<string>();

  for (const [index, variant] of variants.entries()) {
    const at = itemPath(path, index);
    const values = tagValues(variant, tag);
    const presence = variant.byName.get(tag)?.presence.mode;

    if (values.length === 0 || presence !== "required") {
      refusals.push({ rule: "UnionTagInvalid", path: at, detail: `no required string-literal member ${tag}` });
    }

    for (const value of values) {
      if (seen.has(value)) {
        refusals.push({ rule: "UnionTagInvalid", path: at, detail: "a tag value that another member also names" });
      }

      seen.add(value);
    }

    walk(variant, at, level, refusals);
  }
}

function tooDeep(path: string): KindRefused {
  return { rule: "KindTooDeep", path, detail: `more than ${MAX_VALUE_JSON_DEPTH} levels of arrays and objects` };
}
