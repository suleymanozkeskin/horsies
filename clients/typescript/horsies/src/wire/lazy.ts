// The lazy kind: the node of a recursive type. It refers to a kind that is built later (the type that holds it), so it
// holds a getter, and a walk reads the kind through the node.
//
// A cycle through a lazy node must pass an array or object level: the writer and the reader count those levels and
// stop at the core's bound of a user value (`TooDeep`). A cycle with no such level (a nullable or a union of itself)
// has no stored value of a finite depth: `level` finds it with a walk of bounded length, and `checkKind` refuses it.

import { MAX_VALUE_JSON_DEPTH } from "../_catalog/codec_bounds.ts";
import type { Kind, LazyKind, LazyLevel } from "./kinds.ts";
import { unreachable } from "./unreachable.ts";

/** The kinds of one level that a kind walk allows for: the array or object, and four wrappers. */
const KIND_NODES_PER_LEVEL = 5;

/**
 * The most kinds that a kind walk follows in depth (`checkKind`), and the most kinds that the cycle check of one lazy
 * node walks without an array or object level: the core's bound of a user value (`MAX_VALUE_JSON_DEPTH` levels) with
 * `KIND_NODES_PER_LEVEL` kinds per level.
 */
export const MAX_KIND_WALK_DEPTH: number = MAX_VALUE_JSON_DEPTH * KIND_NODES_PER_LEVEL;

const PASSES_LEVEL: LazyLevel = Object.freeze({ state: "passesLevel" });

/** A value that a lazy node computes at its first use. */
type Once<T> = { readonly state: "pending" } | { readonly state: "done"; readonly value: T };

const PENDING: Once<never> = Object.freeze({ state: "pending" });

/**
 * A lazy kind whose kind `getter` gives. The node calls the getter at the first walk that reads it (not here), and
 * keeps the kind; it computes its cycle check at the first `level()` call, and keeps it. A getter that throws (a
 * wire type read before its module set it) throws out of that walk, and the next walk calls it again. Pure apart
 * from the getter.
 */
export function lazyKind(getter: () => Kind): LazyKind {
  let target: Once<Kind> = PENDING;
  let level: Once<LazyLevel> = PENDING;

  const node: LazyKind = Object.freeze({
    node: "lazy",
    target: (): Kind => {
      if (target.state === "pending") {
        target = { state: "done", value: getter() };
      }

      return target.value;
    },
    level: (): LazyLevel => {
      if (level.state === "pending") {
        level = { state: "done", value: levelOf(node) };
      }

      return level.value;
    },
  });

  return node;
}

/**
 * The kind of a lazy node for a value walk (the writer, the reader, the check view) at `subject`. A node whose cycle
 * passes no array or object level is a defect of the caller: `checkKind` refuses that kind before any value walk, so
 * this throws a `TypeError` with the subject and the cause.
 */
export function lazyTarget(kind: LazyKind, subject: string): Kind {
  const level = kind.level();

  switch (level.state) {
    case "passesLevel":
      return kind.target();
    case "noLevel":
      throw new TypeError(`${subject}: ${level.detail}; checkKind refuses this wire type`);
    default:
      return unreachable(level, "lazyTarget");
  }
}

/**
 * Whether each path from `start` back to itself passes an array or object level. The walk follows only the kinds that
 * hold no level (a nullable, a check, a conversion, a union, another lazy node), each lazy node once, at most
 * `MAX_KIND_WALK_DEPTH` kinds; past that bound it fails closed (`noLevel`). Pure apart from the getters.
 */
function levelOf(start: LazyKind): LazyLevel {
  const pending: Kind[] = [start.target()];
  const seen = new Set<LazyKind>([start]);

  for (let walked = 0; walked < MAX_KIND_WALK_DEPTH; walked += 1) {
    const kind = pending.pop();

    if (kind === undefined) {
      return PASSES_LEVEL;
    }

    if (kind === start) {
      return noLevel("the lazy kind reaches itself through no array or object, so no stored value has it");
    }

    pending.push(...innerWithoutLevel(kind, seen));
  }

  return pending.length === 0
    ? PASSES_LEVEL
    : noLevel(`the lazy kind reaches more than ${MAX_KIND_WALK_DEPTH} kinds through no array or object`);
}

function noLevel(detail: string): LazyLevel {
  return Object.freeze({ state: "noLevel", detail });
}

/** The kinds inside `kind` at the same level (no array or object around them). Adds each new lazy node to `seen`. */
function innerWithoutLevel(kind: Kind, seen: Set<LazyKind>): readonly Kind[] {
  switch (kind.node) {
    case "nullable":
    case "checked":
      return [kind.inner];
    case "conversion":
      return [kind.stored];
    case "union":
      return kind.members;
    case "lazy":
      return lazyInner(kind, seen);
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
    case "json":
    case "taggedUnion":
    case "array":
    case "tuple":
    case "record":
    case "object":
    case "taskOutcome":
      return [];
    default:
      return unreachable(kind, "innerWithoutLevel");
  }
}

/** The kind of a lazy node that the cycle check meets: the start node itself, or the kind of a node not seen yet. */
function lazyInner(kind: LazyKind, seen: Set<LazyKind>): readonly Kind[] {
  if (seen.has(kind)) {
    return [];
  }

  seen.add(kind);

  return [kind.target()];
}
