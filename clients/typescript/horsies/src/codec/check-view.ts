// The value that a user-side check sees at encode: the sent value with each default filled, as zod's parse gives it.
//
// A sender may leave out a member with a default, and the writer stores only the keys that the sender gave. A check
// (a zod refinement) is written against zod's output type, in which that member is present. So the writer runs a
// check on this view of the value, and stores the value as sent.
//
// Precondition: the writer wrote `value` under `kind` without a refusal, so the value has the structure of the kind.

import type { DefaultText, Kind, MemberPresence, ObjectKind, TaggedUnionKind } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { MAX_VALUE_JSON_DEPTH } from "./bounds.ts";
import { decodeValue } from "./reader.ts";
import { encodeRefused, type EncodeRefused, entryPath, itemPath, memberPath, refusalText } from "./refusal.ts";
import { isArray, isPlainObject, isString, setOwn } from "./values.ts";

type Viewed = Result<unknown, EncodeRefused>;

/**
 * The value of `value` under `kind` with each left-out defaulted member filled with its default, at `depth` arrays
 * and objects. A member that the value gives, and every value that is not an array or an object, stays as it is.
 *
 * Fails with `CheckFailed` when a default does not read back in the worker form (a conversion that refuses its own
 * stored default). A default that the writer refused is a defect: `checkKind` refuses that kind (`TypeError`). Pure.
 */
export function checkView(kind: Kind, value: unknown, path: string, depth: number): Viewed {
  if (depth > MAX_VALUE_JSON_DEPTH) {
    throw new TypeError(`checkView: ${path} is deeper than the writer allows; the writer refuses it first`);
  }

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
    case "literal":
    case "union":
    case "json":
    case "resultOf":
    case "conversion":
      return ok(value);
    case "nullable":
      return value === null ? ok(value) : checkView(kind.inner, value, path, depth);
    case "checked":
      return checkView(kind.inner, value, path, depth);
    case "array":
      return viewItems(() => kind.item, value, path, depth);
    case "tuple":
      return viewItems((index) => tupleKind(kind.items, index), value, path, depth);
    case "record":
      return viewRecord(kind.value, value, path, depth);
    case "object":
      return viewObject(kind, value, path, depth);
    case "taggedUnion":
      return viewTagged(kind, value, path, depth);
    default:
      return unreachable(kind, "checkView");
  }
}

function tupleKind(items: readonly Kind[], index: number): Kind {
  const kind = items[index];

  if (kind === undefined) {
    throw new TypeError(`checkView: the tuple has no item ${index}`);
  }

  return kind;
}

function viewItems(kinds: (index: number) => Kind, value: unknown, path: string, depth: number): Viewed {
  if (!isArray(value)) {
    return ok(value);
  }

  const items: unknown[] = [];

  for (const [index, item] of value.entries()) {
    const viewed = checkView(kinds(index), item, itemPath(path, index), depth + 1);

    if (!viewed.ok) {
      return viewed;
    }

    items.push(viewed.value);
  }

  return ok(items);
}

function viewRecord(entryKind: Kind, value: unknown, path: string, depth: number): Viewed {
  if (!isPlainObject(value)) {
    return ok(value);
  }

  const record: Record<string, unknown> = {};

  for (const key of Object.keys(value)) {
    const viewed = checkView(entryKind, value[key], entryPath(path), depth + 1);

    if (!viewed.ok) {
      return viewed;
    }

    setOwn(record, key, viewed.value);
  }

  return ok(record);
}

function viewTagged(kind: TaggedUnionKind, value: unknown, path: string, depth: number): Viewed {
  const tagValue = isPlainObject(value) ? value[kind.tag] : undefined;
  const variant = isString(tagValue) ? kind.byTag.get(tagValue) : undefined;

  return variant === undefined ? ok(value) : viewObject(variant, value, path, depth);
}

function viewObject(kind: ObjectKind, value: unknown, path: string, depth: number): Viewed {
  if (!isPlainObject(value)) {
    return ok(value);
  }

  const object: Record<string, unknown> = {};

  for (const member of kind.members) {
    const at = memberPath(path, member.name);

    const viewed = Object.hasOwn(value, member.name)
      ? givenMember(checkView(member.kind, value[member.name], at, depth + 1))
      : absentMember(member.kind, member.presence, at);

    if (!viewed.ok) {
      return viewed;
    }

    switch (viewed.value.state) {
      case "filled":
        setOwn(object, member.name, viewed.value.value);
        break;
      case "absent":
        break;
      default:
        return unreachable(viewed.value, "viewObject");
    }
  }

  return ok(object);
}

/** The view of a member: a value, or absent (an `.optional()` member that the value leaves out). */
type MemberView = { readonly state: "filled"; readonly value: unknown } | { readonly state: "absent" };

const ABSENT: MemberView = Object.freeze({ state: "absent" });

function givenMember(viewed: Viewed): Result<MemberView, EncodeRefused> {
  return viewed.ok ? ok({ state: "filled", value: viewed.value }) : viewed;
}

/** The view of a member that the value leaves out: absent, or its default as the worker form reads it. */
function absentMember(kind: Kind, presence: MemberPresence, path: string): Result<MemberView, EncodeRefused> {
  switch (presence.mode) {
    case "required":
    case "optional":
      return ok(ABSENT);
    case "defaulted":
      return givenMember(filledDefault(kind, presence.fallback, path));
    default:
      return unreachable(presence, "absentMember");
  }
}

function filledDefault(kind: Kind, fallback: DefaultText, path: string): Viewed {
  switch (fallback.state) {
    case "stored": {
      const read = decodeValue(kind, fallback.text, "worker");

      return read.ok
        ? read
        : err(encodeRefused("CheckFailed", path, `the default does not read back: ${refusalText(read.error)}`));
    }

    case "refused":
      throw new TypeError(`checkView: the default of ${path} does not encode; checkKind refuses this wire type`);
    default:
      return unreachable(fallback, "filledDefault");
  }
}
