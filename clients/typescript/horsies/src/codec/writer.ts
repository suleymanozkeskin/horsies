// The writer: Python's stored text of a value under its declared kind. It builds the text from the kind tree and
// calls `JSON.stringify` for one string at a time only.
//
// Rules:
// - Object members are written in declaration order, records in the value's own key order (`Object.keys`).
// - A key that the value leaves out is left out of the text; `null` is written only for a nullable kind; a key whose
//   value is `undefined` is refused. A default is never written by the sender.
// - An undeclared key, a key that only the engine writes, and a value deeper than the core's bound are refused.

import { Decimal } from "../wire/decimal.ts";
import type { Kind, LiteralMember, Member, ObjectKind, TaggedUnionKind } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { MAX_VALUE_JSON_DEPTH } from "./bounds.ts";
import { isEngineKey } from "./engine-keys.ts";
import { type JsonText, writtenJsonText } from "./json-text.ts";
import { jsonValueText } from "./json-value.ts";
import { bigintText, floatText, type ScalarRefused, stringText } from "./scalar-text.ts";
import { isLiteralType, literalText, unionRefusal } from "./union.ts";
import {
  encodeRefused,
  type EncodeRefusalKind,
  type EncodeRefused,
  entryPath,
  itemPath,
  memberPath,
  ROOT_PATH,
} from "./refusal.ts";
import {
  instantText,
  isInstant,
  isPlainDate,
  isPlainDateTime,
  isZonedDateTime,
  offsetDateTimeText,
  plainDateText,
  plainDateTimeText,
  type TemporalWriteRefused,
} from "./temporal-text.ts";
import {
  isArray,
  isBigint,
  isBoolean,
  isNumber,
  isObjectLike,
  isPlainObject,
  isString,
  isUuidText,
} from "./values.ts";

/** The text that one call builds. Never shared between calls. */
type Sink = { text: string };

type Written = Result<true, EncodeRefused>;

const WRITTEN: Written = Object.freeze(ok<true>(true));

/** The depth of a value at the top: it is inside no array or object. */
const TOP_DEPTH = 0;

/** The depth of the parameters object of a task input: the engine's level, so each parameter starts at the top. */
const PARAMETERS_DEPTH = -1;

/**
 * The stored text of `value` under `kind`. Pure.
 *
 * Fails with the first refusal of the walk (`EncodeRefused`); no text is returned then. Does no I/O.
 */
export function encodeValue(kind: Kind, value: unknown): Result<JsonText, EncodeRefused> {
  const sink: Sink = { text: "" };
  const written = writeValue(kind, value, ROOT_PATH, TOP_DEPTH, sink);

  return written.ok ? ok(writtenJsonText(sink.text)) : written;
}

/** The stored text of a task input: one member per given parameter, in declaration order. Pure. */
export function encodeParameters(parameters: ObjectKind, value: unknown): Result<JsonText, EncodeRefused> {
  const sink: Sink = { text: "" };
  const written = writeObject(parameters, value, ROOT_PATH, PARAMETERS_DEPTH, sink);

  return written.ok ? ok(writtenJsonText(sink.text)) : written;
}

function refuse(kind: EncodeRefusalKind, path: string, detail: string): Written {
  return err(encodeRefused(kind, path, detail));
}

function append(sink: Sink, text: string): Written {
  sink.text += text;

  return WRITTEN;
}

function writeValue(kind: Kind, value: unknown, path: string, depth: number, sink: Sink): Written {
  if (value === undefined) {
    return refuse("ExplicitUndefined", path, "undefined is not a stored value; leave the key out");
  }

  switch (kind.node) {
    case "null":
      return value === null ? append(sink, "null") : refuse("WrongType", path, "expected null");
    case "boolean":
      return isBoolean(value) ? append(sink, String(value)) : refuse("WrongType", path, "expected a boolean");
    case "int":
      return writeInt(value, path, sink);
    case "bigint":
      return isBigint(value)
        ? writeScalar(bigintText(value), path, sink)
        : refuse("WrongType", path, "expected a bigint");
    case "float":
      return isNumber(value)
        ? writeScalar(floatText(value), path, sink)
        : refuse("WrongType", path, "expected a number");
    case "string":
      return isString(value)
        ? writeScalar(stringText(value), path, sink)
        : refuse("WrongType", path, "expected a string");
    case "uuid":
      return writeUuid(value, path, sink);
    case "decimal":
      return isObjectLike(value) && Decimal.isDecimal(value)
        ? append(sink, JSON.stringify(value.toString()))
        : refuse("WrongType", path, "expected a Decimal");
    case "instant":
      return isInstant(value)
        ? writeTemporal(instantText(value), path, sink)
        : refuse("WrongType", path, "expected a Temporal.Instant");
    case "offsetDateTime":
      return isZonedDateTime(value)
        ? writeTemporal(offsetDateTimeText(value), path, sink)
        : refuse("WrongType", path, "expected a Temporal.ZonedDateTime");
    case "plainDateTime":
      return isPlainDateTime(value)
        ? writeTemporal(plainDateTimeText(value), path, sink)
        : refuse("WrongType", path, "expected a Temporal.PlainDateTime");
    case "plainDate":
      return isPlainDate(value)
        ? writeTemporal(plainDateText(value), path, sink)
        : refuse("WrongType", path, "expected a Temporal.PlainDate");
    case "literal":
      return writeLiteral(kind.members, value, path, sink);
    case "union":
      return writeUnion(kind.members, value, path, depth, sink);
    case "taggedUnion":
      return writeTagged(kind, value, path, depth, sink);
    case "nullable":
      return value === null ? append(sink, "null") : writeValue(kind.inner, value, path, depth, sink);
    case "array":
      return writeArray(kind.item, value, path, depth, sink);
    case "tuple":
      return writeTuple(kind.items, value, path, depth, sink);
    case "record":
      return writeRecord(kind.value, value, path, depth, sink);
    case "object":
      return writeObject(kind, value, path, depth, sink);
    case "json":
      return writeJsonText(jsonValueText(value, path, depth, MAX_VALUE_JSON_DEPTH), sink);
    default:
      return unreachable(kind, "writeValue");
  }
}

function writeInt(value: unknown, path: string, sink: Sink): Written {
  if (!isNumber(value)) {
    return refuse("WrongType", path, "expected a number");
  }

  if (!Number.isSafeInteger(value)) {
    return refuse("NotSafeInteger", path, "h.int() takes a safe integer; use h.bigint() for a larger one");
  }

  return append(sink, String(value));
}

/** The text of a scalar, or its refusal at `path`. */
function writeScalar(text: Result<string, ScalarRefused>, path: string, sink: Sink): Written {
  return text.ok ? append(sink, text.value) : refuse(text.error.kind, path, text.error.detail);
}

/** The text of a JSON value, or the JSON writer's refusal, which names its own path. */
function writeJsonText(text: Result<string, EncodeRefused>, sink: Sink): Written {
  return text.ok ? append(sink, text.value) : text;
}

/** A UUID text in the lower-case 8-4-4-4-12 form, as it is. */
function writeUuid(value: unknown, path: string, sink: Sink): Written {
  if (!isString(value)) {
    return refuse("WrongType", path, "expected a UUID string");
  }

  return isUuidText(value)
    ? append(sink, `"${value}"`)
    : refuse("NotCanonicalUuid", path, "not the lower-case 8-4-4-4-12 form");
}

/** A date-time text as a JSON string; the text holds no character that JSON escapes. */
function writeTemporal(text: Result<string, TemporalWriteRefused>, path: string, sink: Sink): Written {
  return text.ok ? append(sink, `"${text.value}"`) : refuse(text.error.kind, path, text.error.detail);
}

function writeLiteral(members: readonly LiteralMember[], value: unknown, path: string, sink: Sink): Written {
  if (!isLiteralType(value)) {
    return refuse("WrongType", path, "expected a string, a number, a boolean or null");
  }

  return members.includes(value)
    ? append(sink, literalText(value))
    : refuse("NotAMember", path, "not one of the literal members");
}

/** The text of the first member that takes the value. */
function writeUnion(members: readonly Kind[], value: unknown, path: string, depth: number, sink: Sink): Written {
  const refusals: EncodeRefused[] = [];

  for (const member of members) {
    const trial: Sink = { text: "" };
    const written = writeValue(member, value, path, depth, trial);

    if (written.ok) {
      return append(sink, trial.text);
    }

    refusals.push(written.error);
  }

  return err(unionRefusal(refusals, encodeRefused("NotAMember", path, "no member of the union takes the value")));
}

/** The member object that the tag names. */
function writeTagged(kind: TaggedUnionKind, value: unknown, path: string, depth: number, sink: Sink): Written {
  if (!isPlainObject(value)) {
    return notContainer(value, path, "an object");
  }

  const tagValue = value[kind.tag];
  const variant = isString(tagValue) ? kind.byTag.get(tagValue) : undefined;

  return variant === undefined
    ? refuse("NotAMember", memberPath(path, kind.tag), "the tag names no member of the union")
    : writeObject(variant, value, path, depth, sink);
}

/** The refusal of a value that is not the container that `expected` names. */
function notContainer(value: unknown, path: string, expected: string): Written {
  return isObjectLike(value) && !isArray(value) && !isPlainObject(value)
    ? refuse("NotJsonNative", path, "not data: a Date, Map, Set, function, class instance or symbol key")
    : refuse("WrongType", path, `expected ${expected}`);
}

/** The depth of the arrays and objects inside a container at `depth`, or `TooDeep`. */
function enter(depth: number, path: string): Result<number, EncodeRefused> {
  const level = depth + 1;

  return level > MAX_VALUE_JSON_DEPTH
    ? err(encodeRefused("TooDeep", path, `more than ${MAX_VALUE_JSON_DEPTH} levels`))
    : ok(level);
}

/** The items of an array or a tuple; `kinds` gives the kind of each index. */
function writeItems(
  kinds: (index: number) => Kind,
  items: readonly unknown[],
  path: string,
  level: number,
  sink: Sink,
): Written {
  sink.text += "[";

  for (let index = 0; index < items.length; index += 1) {
    sink.text += index > 0 ? "," : "";
    const written = writeValue(kinds(index), items[index], itemPath(path, index), level, sink);

    if (!written.ok) {
      return written;
    }
  }

  return append(sink, "]");
}

function writeArray(item: Kind, value: unknown, path: string, depth: number, sink: Sink): Written {
  if (!isArray(value)) {
    return notContainer(value, path, "an array");
  }

  const level = enter(depth, path);

  return level.ok ? writeItems(() => item, value, path, level.value, sink) : level;
}

function writeTuple(items: readonly Kind[], value: unknown, path: string, depth: number, sink: Sink): Written {
  if (!isArray(value)) {
    return notContainer(value, path, "an array");
  }

  if (value.length !== items.length) {
    return refuse("WrongType", path, `expected an array of ${items.length} items`);
  }

  const level = enter(depth, path);

  return level.ok ? writeItems((index) => tupleItem(items, index), value, path, level.value, sink) : level;
}

/** The kind of a tuple item. The caller checked the length, so a missing kind is a defect. */
export function tupleItem(items: readonly Kind[], index: number): Kind {
  const kind = items[index];

  if (kind === undefined) {
    throw new TypeError(`tupleItem: the tuple has no item ${index}`);
  }

  return kind;
}

/** A record key: a well-formed string that no engine writes. */
function writeKey(key: string, path: string, sink: Sink): Written {
  if (isEngineKey(key)) {
    return refuse("EngineKey", path, "a key that only the engine writes");
  }

  return writeScalar(stringText(key), path, sink);
}

function writeRecord(entryKind: Kind, value: unknown, path: string, depth: number, sink: Sink): Written {
  if (!isPlainObject(value)) {
    return notContainer(value, path, "an object");
  }

  const level = enter(depth, path);

  if (!level.ok) {
    return level;
  }

  sink.text += "{";
  let first = true;

  for (const key of Object.keys(value)) {
    sink.text += first ? "" : ",";
    first = false;
    const at = entryPath(path);
    const keyWritten = writeKey(key, at, sink);
    const written = keyWritten.ok ? writeEntry(entryKind, value[key], at, level.value, sink) : keyWritten;

    if (!written.ok) {
      return written;
    }
  }

  return append(sink, "}");
}

function writeEntry(kind: Kind, value: unknown, path: string, level: number, sink: Sink): Written {
  sink.text += ":";

  return writeValue(kind, value, path, level, sink);
}

function writeObject(kind: ObjectKind, value: unknown, path: string, depth: number, sink: Sink): Written {
  if (!isPlainObject(value)) {
    return notContainer(value, path, "an object");
  }

  for (const key of Object.keys(value)) {
    if (!kind.byName.has(key)) {
      return refuse("UnknownMember", entryPath(path), "a key that the type does not declare");
    }
  }

  const level = enter(depth, path);

  return level.ok ? writeMembers(kind.members, value, path, level.value, sink) : level;
}

function writeMembers(
  members: readonly Member[],
  value: Readonly<Record<string, unknown>>,
  path: string,
  level: number,
  sink: Sink,
): Written {
  sink.text += "{";
  let first = true;

  for (const member of members) {
    const at = memberPath(path, member.name);

    if (!Object.hasOwn(value, member.name)) {
      switch (member.presence.mode) {
        case "required":
          return refuse("MissingMember", at, "a required member is missing");
        case "optional":
        case "defaulted":
          continue;
        default:
          return unreachable(member.presence, "writeMembers");
      }
    }

    sink.text += `${first ? "" : ","}${JSON.stringify(member.name)}:`;
    first = false;
    const written = writeValue(member.kind, value[member.name], at, level, sink);

    if (!written.ok) {
      return written;
    }
  }

  return append(sink, "}");
}
