// The stored-form relations of two kind trees, for the checks that the compiler cannot make on a build that runs at
// run time: a node's output against an `h.resultOf(T)` parameter (`HRS-024`), and an output node against the declared
// output of its workflow (`HRS-025`).
//
// - The same form (`matchAt`): the two kinds write and read the same stored text: the same node at every place, the same
//   members in the same order with the same presence, the same literal values and the same tags.
// - `storedFormFits`, the relation of both checks: the target's reader reads every text that the source's writer gives. It is the rule of the
//   Python client's `_is_ok_type_compatible` (`horsies/core/models/workflow/typing_utils.py`) on kinds: the same form
//   fits; a JSON target takes any source; a nullable or union target takes a source that fits one member; a union
//   source fits when each member fits. Two widenings that the TypeScript type allows and the reader reads are added:
//   a literal fits its base scalar, and an integer fits a float.
//
// A user-side check (`checked`) and a conversion compare by the kind of their stored value: the stored form does not
// hold the check, and the same zod schema compiled for two contracts gives two check functions.

import { isBoolean, isNumber, isString } from "../codec/values.ts";
import {
  type DefaultText,
  type Kind,
  type LiteralMember,
  type Member,
  type MemberPresence,
  NULL_KIND,
  type ObjectKind,
} from "../wire/kinds.ts";
import { unreachable } from "../wire/unreachable.ts";

/** The deepest place that the comparison follows. Past it the kinds do not match (fail closed). */
export const KIND_MATCH_DEPTH_LIMIT = 300;

/** The kind that holds the stored form of `kind`: a check and a conversion are left out. Pure. */
function storedKind(kind: Kind, depth: number): Kind {
  if (depth > KIND_MATCH_DEPTH_LIMIT) {
    return kind;
  }

  switch (kind.node) {
    case "checked":
      return storedKind(kind.inner, depth + 1);
    case "conversion":
      return storedKind(kind.stored, depth + 1);
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
    case "literal":
    case "union":
    case "taggedUnion":
    case "nullable":
    case "resultOf":
    case "array":
    case "tuple":
    case "record":
    case "object":
      return kind;
    default:
      return unreachable(kind, "storedKind");
  }
}

function matchAt(firstGiven: Kind, secondGiven: Kind, depth: number): boolean {
  if (depth > KIND_MATCH_DEPTH_LIMIT) {
    return false;
  }

  const first = storedKind(firstGiven, depth);
  const second = storedKind(secondGiven, depth);

  if (first === second) {
    return true;
  }

  switch (first.node) {
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
      return first.node === second.node;
    case "checked":
    case "conversion":
      // `storedKind` leaves these out, unless the depth limit stopped it.
      return false;
    case "literal":
      return second.node === "literal" && literalsMatch(first.members, second.members);
    case "union":
      return second.node === "union" && listsMatch(first.members, second.members, depth);
    case "taggedUnion":
      return (
        second.node === "taggedUnion" && first.tag === second.tag && listsMatch(first.variants, second.variants, depth)
      );
    case "nullable":
    case "resultOf":
      return second.node === first.node && matchAt(first.inner, second.inner, depth + 1);
    case "array":
      return second.node === "array" && matchAt(first.item, second.item, depth + 1);
    case "tuple":
      return second.node === "tuple" && listsMatch(first.items, second.items, depth);
    case "record":
      return second.node === "record" && matchAt(first.value, second.value, depth + 1);
    case "object":
      return second.node === "object" && objectsMatch(first, second, depth);
    default:
      return unreachable(first, "matchAt");
  }
}

function literalsMatch(first: readonly LiteralMember[], second: readonly LiteralMember[]): boolean {
  return first.length === second.length && first.every((member, index) => Object.is(member, second[index]));
}

function listsMatch(first: readonly Kind[], second: readonly Kind[], depth: number): boolean {
  return first.length === second.length && first.every((kind, index) => itemMatches(kind, second[index], depth));
}

function itemMatches(kind: Kind, other: Kind | undefined, depth: number): boolean {
  return other !== undefined && matchAt(kind, other, depth + 1);
}

function objectsMatch(first: ObjectKind, second: ObjectKind, depth: number): boolean {
  return (
    first.members.length === second.members.length &&
    first.members.every((member, index) => membersMatch(member, second.members[index], depth))
  );
}

function membersMatch(member: Member, other: Member | undefined, depth: number): boolean {
  return (
    other !== undefined &&
    member.name === other.name &&
    presencesMatch(member.presence, other.presence) &&
    matchAt(member.kind, other.kind, depth + 1)
  );
}

/** Whether two members are present alike: the same mode, and for a default the same stored default text. */
function presencesMatch(first: MemberPresence, second: MemberPresence): boolean {
  switch (first.mode) {
    case "required":
    case "optional":
      return second.mode === first.mode;
    case "defaulted":
      return second.mode === "defaulted" && defaultText(first.fallback) === defaultText(second.fallback);
    default:
      return unreachable(first, "presencesMatch");
  }
}

/** The stored text of a default, or none for a default that the writer refused. Pure. */
function defaultText(fallback: DefaultText): string | undefined {
  return fallback.state === "stored" ? fallback.text : undefined;
}

/**
 * Whether the reader of `target` reads every stored text that the writer of `source` gives. Fails closed: a JSON
 * source fits only a JSON target, and past `KIND_MATCH_DEPTH_LIMIT` nothing fits. Pure.
 */
export function storedFormFits(source: Kind, target: Kind): boolean {
  return fitsAt(source, target, 0);
}

function fitsAt(sourceGiven: Kind, targetGiven: Kind, depth: number): boolean {
  if (depth > KIND_MATCH_DEPTH_LIMIT) {
    return false;
  }

  const source = storedKind(sourceGiven, depth);
  const target = storedKind(targetGiven, depth);

  if (target.node === "json" || matchAt(source, target, depth)) {
    return true;
  }

  // A source of several forms fits when each form fits. Checked before a target of several forms, so a union fits a
  // wider union.
  switch (source.node) {
    case "union":
      return source.members.every((member) => fitsAt(member, target, depth + 1));
    case "taggedUnion":
      return source.variants.every((variant) => fitsAt(variant, target, depth + 1));
    case "nullable":
      return fitsAt(source.inner, target, depth + 1) && fitsAt(NULL_KIND, target, depth + 1);
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
    case "checked":
    case "conversion":
    case "literal":
    case "resultOf":
    case "array":
    case "tuple":
    case "record":
    case "object":
      return fitsTarget(source, target, depth);
    default:
      return unreachable(source, "fitsAt");
  }
}

/** Whether a source of one form fits `target`. Pure. */
function fitsTarget(source: Kind, target: Kind, depth: number): boolean {
  switch (target.node) {
    case "nullable":
      return source.node === "null" || fitsAt(source, target.inner, depth + 1);
    case "union":
      return target.members.some((member) => fitsAt(source, member, depth + 1));
    case "taggedUnion":
      return target.variants.some((variant) => fitsAt(source, variant, depth + 1));
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
    case "checked":
    case "conversion":
    case "literal":
    case "resultOf":
    case "array":
    case "tuple":
    case "record":
    case "object":
      return fitsSameNode(source, target, depth);
    default:
      return unreachable(target, "fitsTarget");
  }
}

/** Whether a source fits a target of one form: a literal or an integer widens, a container fits by its parts. */
function fitsSameNode(source: Kind, target: Kind, depth: number): boolean {
  switch (source.node) {
    case "literal":
      return source.members.every((member) => literalFits(member, target));
    case "int":
      return target.node === "float";
    case "array":
      return target.node === "array" && fitsAt(source.item, target.item, depth + 1);
    case "tuple":
      return (
        target.node === "tuple" &&
        source.items.length === target.items.length &&
        source.items.every((item, index) => itemFits(item, target.items[index], depth))
      );
    case "record":
      return target.node === "record" && fitsAt(source.value, target.value, depth + 1);
    case "object":
      return target.node === "object" && objectFits(source, target, depth);
    case "resultOf":
      return target.node === "resultOf" && fitsAt(source.inner, target.inner, depth + 1);
    case "null":
    case "boolean":
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
    case "checked":
    case "conversion":
    case "nullable":
    case "union":
    case "taggedUnion":
      // Every other form fits only its own form, which `matchAt` compared.
      return false;
    default:
      return unreachable(source, "fitsSameNode");
  }
}

/** Whether a literal value reads as a value of `target`, a kind of one form. Pure. */
function literalFits(member: LiteralMember, target: Kind): boolean {
  switch (target.node) {
    case "literal":
      return target.members.some((value) => Object.is(value, member));
    case "string":
      return isString(member);
    case "int":
    case "float":
      return isNumber(member);
    case "boolean":
      return isBoolean(member);
    case "null":
      return member === null;
    case "bigint":
    case "uuid":
    case "decimal":
    case "instant":
    case "offsetDateTime":
    case "plainDateTime":
    case "plainDate":
    case "plainTime":
    case "json":
    case "checked":
    case "conversion":
    case "nullable":
    case "union":
    case "taggedUnion":
    case "resultOf":
    case "array":
    case "tuple":
    case "record":
    case "object":
      return false;
    default:
      return unreachable(target, "literalFits");
  }
}

function itemFits(item: Kind, other: Kind | undefined, depth: number): boolean {
  return other !== undefined && fitsAt(item, other, depth + 1);
}

/** Whether each member of `source` fits the member of `target` at the same place, with the same name. Pure. */
function objectFits(source: ObjectKind, target: ObjectKind, depth: number): boolean {
  return (
    source.members.length === target.members.length &&
    source.members.every((member, index) => memberFits(member, target.members[index], depth))
  );
}

function memberFits(member: Member, other: Member | undefined, depth: number): boolean {
  return (
    other !== undefined &&
    member.name === other.name &&
    presenceFits(member.presence, other.presence) &&
    fitsAt(member.kind, other.kind, depth + 1)
  );
}

/** Whether a member present as `source` reads as `target`: the same presence, or an always-present member. Pure. */
function presenceFits(source: MemberPresence, target: MemberPresence): boolean {
  return presencesMatch(source, target) || source.mode === "required";
}
