// Whether two kind trees have the same stored form: the check that a node's output fits an `h.resultOf(T)` parameter
// at run time (the type of a node built at run time is not checked by the compiler).
//
// Two kinds match when they write and read the same stored text: the same node at every place, the same members in
// the same order with the same presence, the same literal values and the same tags. A user-side check (`checked`) and
// a conversion compare by the kind of their stored value: the stored form does not hold the check, and the same zod
// schema compiled for two contracts gives two check functions.

import type { DefaultText, Kind, LiteralMember, Member, MemberPresence, ObjectKind } from "../wire/kinds.ts";
import { unreachable } from "../wire/unreachable.ts";

/** The deepest place that the comparison follows. Past it the kinds do not match (fail closed). */
export const KIND_MATCH_DEPTH_LIMIT = 300;

/** Whether `first` and `second` have the same stored form. Pure. */
export function storedFormsMatch(first: Kind, second: Kind): boolean {
  return matchAt(first, second, 0);
}

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
    default:
      return kind;
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
      return unreachable(first, "storedFormsMatch");
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
