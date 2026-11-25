// The checks of a kind tree that the static types cannot make. `Horsies.create` reports each refusal with the code
// of a task signature refusal.

import { MAX_VALUE_JSON_DEPTH } from "../codec/bounds.ts";
import { isEngineKey } from "../codec/engine-keys.ts";
import { itemPath, memberPath, ROOT_PATH } from "../codec/refusal.ts";
import type { DefaultText, Kind, Member, MemberPresence } from "./kinds.ts";
import { unreachable } from "./unreachable.ts";

/** Why a kind tree cannot be a task's wire type. */
export type KindRule =
  /** The value of a `.default(value)` does not encode under its kind (for example `NaN` for `h.float()`). */
  | "DefaultNotStored"
  /** An object member has a name that only the engine writes. */
  | "EngineKeyMember"
  /** The kind nests more arrays and objects than a stored value may have, so no value fits it. */
  | "KindTooDeep";

/** A refusal of a kind tree. `path` names the place in the type: `[*]` an array item, `.*` a record value. */
export type KindRefused = { readonly rule: KindRule; readonly path: string; readonly detail: string };

/** Every refusal of a kind tree, in walk order. An empty list: the kind is valid. Pure. */
export function checkKind(kind: Kind): readonly KindRefused[] {
  const refusals: KindRefused[] = [];

  walk(kind, ROOT_PATH, 0, refusals);

  return refusals;
}

function walk(kind: Kind, path: string, level: number, refusals: KindRefused[]): void {
  switch (kind.node) {
    case "null":
    case "boolean":
    case "int":
    case "bigint":
    case "float":
    case "string":
    case "instant":
    case "offsetDateTime":
    case "plainDateTime":
    case "plainDate":
      return;
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

    if (isEngineKey(member.name)) {
      refusals.push({ rule: "EngineKeyMember", path: at, detail: "a member name that only the engine writes" });
    }

    refusals.push(...defaultRefusals(member.presence, at));
    walk(member.kind, at, level + 1, refusals);
  }
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
      return [{ rule: "DefaultNotStored", path, detail: `the default is refused: ${fallback.refusal.kind}` }];
    default:
      return unreachable(fallback, "storedDefaultRefusals");
  }
}

function tooDeep(path: string): KindRefused {
  return { rule: "KindTooDeep", path, detail: `more than ${MAX_VALUE_JSON_DEPTH} levels of arrays and objects` };
}
