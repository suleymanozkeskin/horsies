// The stored-form relations of two kind trees, for the checks that the compiler cannot make on a build that runs at
// run time: a node's output against an `h.taskOutcome(contract)` parameter (`HRS-024`), and an output node against
// the declared output of its workflow (`HRS-025`).
//
// - The same form (`matchAt`): the two kinds write and read the same stored text: the same node at every place, the same
//   members in the same order with the same presence, the same literal values and the same tags.
// - `storedFormFits`, the relation of both checks: the target's reader reads every text that the source's writer gives. It is the rule of the
//   Python client's `_is_ok_type_compatible` (`clients/python/horsies/core/models/workflow/typing_utils.py`) on kinds: the same form
//   fits; a JSON target takes any source; a nullable or union target takes a source that fits one member; a union
//   source fits when each member fits. Two widenings that the TypeScript type allows and the reader reads are added:
//   a literal fits its base scalar, and an integer fits a float.
//
// A lazy node (a recursive type) compares by its kind; a pair of lazy nodes that the comparison meets again holds, so
// two recursive kinds compare to the end of their cycles.
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
  type TupleRest,
} from "../wire/kinds.ts";
import { outcomeValueKind } from "../wire/task-outcome.ts";
import { unreachable } from "../wire/unreachable.ts";

/** The place that a defect text names for an upstream outcome kind that the comparison meets. */
const MATCHED_PLACE = "a compared kind";

/** The deepest place that the comparison follows. Past it the kinds do not match (fail closed). */
export const KIND_MATCH_DEPTH_LIMIT = 300;

/**
 * Where a comparison is: the depth (at most `KIND_MATCH_DEPTH_LIMIT`), and the pairs of lazy nodes that it compares
 * above this place. A pair met again is a cycle of both kinds: it holds by assumption (the relation of two recursive
 * types), so the comparison of two recursive kinds ends.
 */
type Step = { readonly depth: number; readonly assumed: readonly (readonly [Kind, Kind])[] };

/** A step one level down. Pure. */
function deeper(at: Step): Step {
  return { depth: at.depth + 1, assumed: at.assumed };
}

/**
 * The relation of two kinds of which one or both are lazy nodes: it holds for a pair that the comparison meets again,
 * else `relation` compares the kinds of the nodes, with the pair assumed. Pure apart from the getters.
 */
function lazyStep(first: Kind, second: Kind, at: Step, relation: (a: Kind, b: Kind, at: Step) => boolean): boolean {
  if (at.assumed.some(([a, b]) => a === first && b === second)) {
    return true;
  }

  return relation(lazyOpened(first), lazyOpened(second), {
    depth: at.depth + 1,
    assumed: [...at.assumed, [first, second]],
  });
}

/** The kind of a lazy node; another kind as it is. Pure apart from the getter. */
function lazyOpened(kind: Kind): Kind {
  return kind.node === "lazy" ? kind.target() : kind;
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
    case "taskOutcome":
    case "array":
    case "tuple":
    case "record":
    case "object":
    case "lazy":
      return kind;
    default:
      return unreachable(kind, "storedKind");
  }
}

function matchAt(firstGiven: Kind, secondGiven: Kind, at: Step): boolean {
  if (at.depth > KIND_MATCH_DEPTH_LIMIT) {
    return false;
  }

  const first = storedKind(firstGiven, at.depth);
  const second = storedKind(secondGiven, at.depth);

  if (first === second) {
    return true;
  }

  if (first.node === "lazy" || second.node === "lazy") {
    return lazyStep(first, second, at, matchAt);
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
      return second.node === "union" && listsMatch(first.members, second.members, at);
    case "taggedUnion":
      return (
        second.node === "taggedUnion" && first.tag === second.tag && listsMatch(first.variants, second.variants, at)
      );
    case "nullable":
      return second.node === "nullable" && matchAt(first.inner, second.inner, deeper(at));
    case "taskOutcome":
      return (
        second.node === "taskOutcome" &&
        matchAt(outcomeValueKind(first, MATCHED_PLACE), outcomeValueKind(second, MATCHED_PLACE), deeper(at))
      );
    case "array":
      return second.node === "array" && matchAt(first.item, second.item, deeper(at));
    case "tuple":
      return (
        second.node === "tuple" && listsMatch(first.items, second.items, at) && restsMatch(first.rest, second.rest, at)
      );
    case "record":
      return second.node === "record" && matchAt(first.value, second.value, deeper(at));
    case "object":
      return second.node === "object" && objectsMatch(first, second, at);
    default:
      return unreachable(first, "matchAt");
  }
}

function restsMatch(first: TupleRest, second: TupleRest, at: Step): boolean {
  switch (first.state) {
    case "none":
      return second.state === "none";
    case "items":
      return second.state === "items" && matchAt(first.item, second.item, deeper(at));
    default:
      return unreachable(first, "restsMatch");
  }
}

function literalsMatch(first: readonly LiteralMember[], second: readonly LiteralMember[]): boolean {
  return first.length === second.length && first.every((member, index) => Object.is(member, second[index]));
}

function listsMatch(first: readonly Kind[], second: readonly Kind[], at: Step): boolean {
  return first.length === second.length && first.every((kind, index) => itemMatches(kind, second[index], at));
}

function itemMatches(kind: Kind, other: Kind | undefined, at: Step): boolean {
  return other !== undefined && matchAt(kind, other, deeper(at));
}

function objectsMatch(first: ObjectKind, second: ObjectKind, at: Step): boolean {
  return (
    first.members.length === second.members.length &&
    first.members.every((member, index) => membersMatch(member, second.members[index], at))
  );
}

function membersMatch(member: Member, other: Member | undefined, at: Step): boolean {
  return (
    other !== undefined &&
    member.name === other.name &&
    presencesMatch(member.presence, other.presence) &&
    matchAt(member.kind, other.kind, deeper(at))
  );
}

/** Whether two members are present alike: the same mode, and for a default the same stored default text. */
function presencesMatch(first: MemberPresence, second: MemberPresence): boolean {
  switch (first.mode) {
    case "required":
    case "optional":
      return second.mode === first.mode;
    case "defaulted":
      return second.mode === "defaulted" && defaultsMatch(first.fallback, second.fallback);
    default:
      return unreachable(first, "presencesMatch");
  }
}

/** Whether two defaults are alike: the same stored text, or the same default function (its `source`). Pure. */
function defaultsMatch(first: DefaultText, second: DefaultText): boolean {
  switch (first.state) {
    case "stored":
      return second.state === "stored" && second.text === first.text;
    case "computed":
      return second.state === "computed" && second.source === first.source;
    case "refused":
      return second.state === "refused";
    default:
      return unreachable(first, "defaultsMatch");
  }
}

/**
 * Whether the reader of `target` reads every stored text that the writer of `source` gives. Fails closed: a JSON
 * source fits only a JSON target, and past `KIND_MATCH_DEPTH_LIMIT` nothing fits. Pure.
 */
export function storedFormFits(source: Kind, target: Kind): boolean {
  return fitsAt(source, target, { depth: 0, assumed: [] });
}

function fitsAt(sourceGiven: Kind, targetGiven: Kind, at: Step): boolean {
  if (at.depth > KIND_MATCH_DEPTH_LIMIT) {
    return false;
  }

  const source = storedKind(sourceGiven, at.depth);
  const target = storedKind(targetGiven, at.depth);

  if (target.node === "json" || matchAt(source, target, { depth: at.depth, assumed: [] })) {
    return true;
  }

  if (source.node === "lazy" || target.node === "lazy") {
    return lazyStep(source, target, at, fitsAt);
  }

  // A source of several forms fits when each form fits. Checked before a target of several forms, so a union fits a
  // wider union.
  switch (source.node) {
    case "union":
      return source.members.every((member) => fitsAt(member, target, deeper(at)));
    case "taggedUnion":
      return source.variants.every((variant) => fitsAt(variant, target, deeper(at)));
    case "nullable":
      return fitsAt(source.inner, target, deeper(at)) && fitsAt(NULL_KIND, target, deeper(at));
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
    case "taskOutcome":
    case "array":
    case "tuple":
    case "record":
    case "object":
      return fitsTarget(source, target, at);
    default:
      return unreachable(source, "fitsAt");
  }
}

/** Whether a source of one form fits `target`. Pure. */
function fitsTarget(source: Kind, target: Kind, at: Step): boolean {
  switch (target.node) {
    case "nullable":
      return source.node === "null" || fitsAt(source, target.inner, deeper(at));
    case "union":
      return target.members.some((member) => fitsAt(source, member, deeper(at)));
    case "taggedUnion":
      return target.variants.some((variant) => fitsAt(source, variant, deeper(at)));
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
    case "taskOutcome":
    case "array":
    case "tuple":
    case "record":
    case "object":
      return fitsSameNode(source, target, at);
    case "lazy":
      // `fitsAt` compared it with `lazyStep`.
      return false;
    default:
      return unreachable(target, "fitsTarget");
  }
}

/** Whether a source fits a target of one form: a literal or an integer widens, a container fits by its parts. */
function fitsSameNode(source: Kind, target: Kind, at: Step): boolean {
  switch (source.node) {
    case "literal":
      return source.members.every((member) => literalFits(member, target));
    case "int":
      return target.node === "float";
    case "array":
      return target.node === "array" && fitsAt(source.item, target.item, deeper(at));
    case "tuple":
      return (
        target.node === "tuple" &&
        source.items.length === target.items.length &&
        source.items.every((item, index) => itemFits(item, target.items[index], at)) &&
        restFits(source.rest, target.rest, at)
      );
    case "record":
      return target.node === "record" && fitsAt(source.value, target.value, deeper(at));
    case "object":
      return target.node === "object" && objectFits(source, target, at);
    case "taskOutcome":
      return (
        target.node === "taskOutcome" &&
        fitsAt(outcomeValueKind(source, MATCHED_PLACE), outcomeValueKind(target, MATCHED_PLACE), deeper(at))
      );
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
    case "lazy":
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
    case "taskOutcome":
    case "array":
    case "tuple":
    case "record":
    case "object":
    case "lazy":
      return false;
    default:
      return unreachable(target, "literalFits");
  }
}

/** Whether the rest items of a source tuple read as the rest items of a target tuple: none and none, or kinds that fit. */
function restFits(source: TupleRest, target: TupleRest, at: Step): boolean {
  switch (source.state) {
    case "none":
      return target.state === "none";
    case "items":
      return target.state === "items" && fitsAt(source.item, target.item, deeper(at));
    default:
      return unreachable(source, "restFits");
  }
}

function itemFits(item: Kind, other: Kind | undefined, at: Step): boolean {
  return other !== undefined && fitsAt(item, other, deeper(at));
}

/** Whether each member of `source` fits the member of `target` at the same place, with the same name. Pure. */
function objectFits(source: ObjectKind, target: ObjectKind, at: Step): boolean {
  return (
    source.members.length === target.members.length &&
    source.members.every((member, index) => memberFits(member, target.members[index], at))
  );
}

function memberFits(member: Member, other: Member | undefined, at: Step): boolean {
  return (
    other !== undefined &&
    member.name === other.name &&
    presenceFits(member.presence, other.presence) &&
    fitsAt(member.kind, other.kind, deeper(at))
  );
}

/** Whether a member present as `source` reads as `target`: the same presence, or an always-present member. Pure. */
function presenceFits(source: MemberPresence, target: MemberPresence): boolean {
  return presencesMatch(source, target) || source.mode === "required";
}
