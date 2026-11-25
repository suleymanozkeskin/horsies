// The reader: the value of a stored text under its declared kind.
//
// Precondition: the text is a `JsonText` (`json-text.ts`): the core's strict read rule ran on it, or the codec wrote
// it. The reader refuses every typed mismatch; it does not check the strict rule again.
//
// Rules:
// - An undeclared key is refused at every depth. A key that only the engine writes is refused in a record.
// - A member that the text leaves out: a required one is refused; an `.optional()` one stays absent; a defaulted one
//   is filled in the worker form and stays absent in the stored form (the form that a read-back writes again).
// - Records are built with own data properties: a stored key `__proto__` never changes a prototype.

import { Decimal } from "../wire/decimal.ts";
import type { DefaultText, Kind, Member, MemberPresence, ObjectKind } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { MAX_VALUE_JSON_DEPTH } from "./bounds.ts";
import { isEngineKey } from "./engine-keys.ts";
import type { JsonText } from "./json-text.ts";
import { IntegralFloat, isTreeArray, isTreeObject, readTree, type TreeObject, type TreeValue } from "./number-text.ts";
import {
  decodeRefused,
  type DecodeRefusalKind,
  type DecodeRefused,
  entryPath,
  itemPath,
  memberPath,
  ROOT_PATH,
} from "./refusal.ts";
import {
  readInstant,
  readOffsetDateTime,
  readPlainDate,
  readPlainDateTime,
  type TemporalReadRefused,
} from "./temporal-text.ts";
import { isBigint, isBoolean, isNumber, isString, isUuidText, setOwn } from "./values.ts";

/**
 * Which value a decode gives:
 * - `worker`: the value that a task body gets: each defaulted member that the text leaves out is filled;
 * - `stored`: the value as stored: a defaulted member that the text leaves out stays absent.
 */
export type DecodeForm = "worker" | "stored";

type Decoded = Result<unknown, DecodeRefused>;

/** The depth of a value at the top: it is inside no array or object. */
const TOP_DEPTH = 0;

/** The depth of the parameters object of a task input: the engine's level, so each parameter starts at the top. */
const PARAMETERS_DEPTH = -1;

/**
 * The value of `text` under `kind`. Pure.
 *
 * Fails with the first refusal of the walk (`DecodeRefused`). A text that is not JSON is a defect of the path that
 * made the `JsonText` (`readTree` throws a `TypeError`).
 */
export function decodeValue(kind: Kind, text: JsonText, form: DecodeForm): Decoded {
  return decodeNode(kind, readTree(text), ROOT_PATH, TOP_DEPTH, form);
}

/** The parameters of a stored task input. Pure; see `decodeValue`. */
export function decodeParameters(parameters: ObjectKind, text: JsonText, form: DecodeForm): Decoded {
  return decodeObject(parameters, readTree(text), ROOT_PATH, PARAMETERS_DEPTH, form);
}

function refuse(kind: DecodeRefusalKind, path: string, detail: string): Decoded {
  return err(decodeRefused(kind, path, detail));
}

/** The value of one tree node at `depth` (the number of arrays and objects around it) under `kind`. Pure. */
export function decodeNode(kind: Kind, node: TreeValue, path: string, depth: number, form: DecodeForm): Decoded {
  switch (kind.node) {
    case "null":
      return node === null ? ok(null) : refuse("WrongType", path, "expected null");
    case "boolean":
      return isBoolean(node) ? ok(node) : refuse("WrongType", path, "expected a boolean");
    case "int":
      return decodeInt(node, path);
    case "bigint":
      return decodeBigint(node, path);
    case "float":
      return decodeFloat(node, path);
    case "string":
      return isString(node) ? ok(node) : refuse("WrongType", path, "expected a string");
    case "uuid":
      return decodeText(node, path, readUuid);
    case "decimal":
      return decodeText(node, path, readDecimal);
    case "instant":
      return decodeText(node, path, readInstant);
    case "offsetDateTime":
      return decodeText(node, path, readOffsetDateTime);
    case "plainDateTime":
      return decodeText(node, path, readPlainDateTime);
    case "plainDate":
      return decodeText(node, path, readPlainDate);
    case "nullable":
      return node === null ? ok(null) : decodeNode(kind.inner, node, path, depth, form);
    case "array":
      return decodeItems(() => kind.item, node, path, depth, form);
    case "tuple":
      return decodeTuple(kind.items, node, path, depth, form);
    case "record":
      return decodeRecord(kind.value, node, path, depth, form);
    case "object":
      return decodeObject(kind, node, path, depth, form);
    default:
      return unreachable(kind, "decodeNode");
  }
}

/** A refusal of a text form: the reader's kind and the rule. */
type TextRefused = { readonly kind: "BadTemporalText" | "BadDecimalText" | "BadUuidText"; readonly detail: string };

function readUuid(text: string): Result<string, TextRefused> {
  return isUuidText(text)
    ? ok(text)
    : err({ kind: "BadUuidText", detail: "not a UUID in the lower-case 8-4-4-4-12 form" });
}

function readDecimal(text: string): Result<Decimal, TextRefused> {
  const parsed = Decimal.parse(text);

  return parsed.ok ? parsed : err({ kind: "BadDecimalText", detail: parsed.error.detail });
}

/** A value stored as a JSON string with its own text form. */
function decodeText<T>(
  node: TreeValue,
  path: string,
  read: (text: string) => Result<T, TextRefused | TemporalReadRefused>,
): Decoded {
  if (!isString(node)) {
    return refuse("WrongType", path, "expected a string");
  }

  const value = read(node);

  return value.ok ? value : refuse(value.error.kind, path, value.error.detail);
}

function decodeInt(node: TreeValue, path: string): Decoded {
  if (isNumber(node)) {
    return Number.isInteger(node) ? ok(node) : refuse("FloatForInteger", path, "a float text for an integer");
  }

  if (isBigint(node)) {
    return refuse("IntegerOutOfRange", path, "outside the safe integer range of h.int()");
  }

  return node instanceof IntegralFloat
    ? refuse("FloatForInteger", path, "a float text for an integer")
    : refuse("WrongType", path, "expected an integer");
}

function decodeBigint(node: TreeValue, path: string): Decoded {
  if (isBigint(node)) {
    return ok(node);
  }

  if (isNumber(node)) {
    return Number.isInteger(node) ? ok(BigInt(node)) : refuse("FloatForInteger", path, "a float text for an integer");
  }

  return node instanceof IntegralFloat
    ? refuse("FloatForInteger", path, "a float text for an integer")
    : refuse("WrongType", path, "expected an integer");
}

/** A float kind takes a float text and an integer text, as Python's `float` takes `7`. */
function decodeFloat(node: TreeValue, path: string): Decoded {
  if (isNumber(node)) {
    return ok(node);
  }

  if (node instanceof IntegralFloat) {
    return ok(node.value);
  }

  if (!isBigint(node)) {
    return refuse("WrongType", path, "expected a number");
  }

  const value = Number(node);

  return Number.isFinite(value) ? ok(value) : refuse("WrongType", path, "an integer outside the finite float range");
}

/** The depth of the arrays and objects inside a container at `depth`, or `TooDeep`. */
function enter(depth: number, path: string): Result<number, DecodeRefused> {
  const level = depth + 1;

  return level > MAX_VALUE_JSON_DEPTH
    ? err(decodeRefused("TooDeep", path, `more than ${MAX_VALUE_JSON_DEPTH} levels`))
    : ok(level);
}

/** The items of an array or a tuple; `kinds` gives the kind of each index. */
function decodeItems(
  kinds: (index: number) => Kind,
  node: TreeValue,
  path: string,
  depth: number,
  form: DecodeForm,
): Decoded {
  if (!isTreeArray(node)) {
    return refuse("WrongType", path, "expected an array");
  }

  const level = enter(depth, path);

  if (!level.ok) {
    return level;
  }

  const items: unknown[] = [];

  for (const [index, item] of node.entries()) {
    const decoded = decodeNode(kinds(index), item, itemPath(path, index), level.value, form);

    if (!decoded.ok) {
      return decoded;
    }

    items.push(decoded.value);
  }

  return ok(items);
}

function decodeTuple(items: readonly Kind[], node: TreeValue, path: string, depth: number, form: DecodeForm): Decoded {
  if (isTreeArray(node) && node.length !== items.length) {
    return refuse("WrongType", path, `expected an array of ${items.length} items`);
  }

  return decodeItems((index) => tupleItemKind(items, index), node, path, depth, form);
}

/** The kind of a tuple item. The caller checked the length, so a missing kind is a defect. */
function tupleItemKind(items: readonly Kind[], index: number): Kind {
  const kind = items[index];

  if (kind === undefined) {
    throw new TypeError(`decodeTuple: the tuple has no item ${index}`);
  }

  return kind;
}

function decodeRecord(entryKind: Kind, node: TreeValue, path: string, depth: number, form: DecodeForm): Decoded {
  if (!isTreeObject(node)) {
    return refuse("WrongType", path, "expected an object");
  }

  const level = enter(depth, path);

  if (!level.ok) {
    return level;
  }

  const record: Record<string, unknown> = {};

  for (const [key, entry] of Object.entries(node)) {
    const at = entryPath(path);

    if (isEngineKey(key)) {
      return refuse("EngineKey", at, "a key that only the engine writes");
    }

    const decoded = decodeNode(entryKind, entry, at, level.value, form);

    if (!decoded.ok) {
      return decoded;
    }

    setOwn(record, key, decoded.value);
  }

  return ok(record);
}

function decodeObject(kind: ObjectKind, node: TreeValue, path: string, depth: number, form: DecodeForm): Decoded {
  if (!isTreeObject(node)) {
    return refuse("WrongType", path, "expected an object");
  }

  for (const key of Object.keys(node)) {
    if (!kind.byName.has(key)) {
      return refuse("UnknownMember", entryPath(path), "a key that the type does not declare");
    }
  }

  const level = enter(depth, path);

  return level.ok ? decodeMembers(kind.members, node, path, level.value, form) : level;
}

function decodeMembers(
  members: readonly Member[],
  node: TreeObject,
  path: string,
  level: number,
  form: DecodeForm,
): Decoded {
  const object: Record<string, unknown> = {};

  for (const member of members) {
    const at = memberPath(path, member.name);
    const stored = node[member.name];

    const decoded =
      stored === undefined
        ? absentMember(member.presence, member.kind, at, level, form)
        : presentMember(decodeNode(member.kind, stored, at, level, form));

    if (!decoded.ok) {
      return decoded;
    }

    switch (decoded.value.state) {
      case "filled":
        setOwn(object, member.name, decoded.value.value);
        break;
      case "absent":
        break;
      default:
        return unreachable(decoded.value, "decodeMembers");
    }
  }

  return ok(object);
}

/** A member of a decoded object: it has a value, or it stays absent. */
type MemberValue = { readonly state: "filled"; readonly value: unknown } | { readonly state: "absent" };

const ABSENT: MemberValue = Object.freeze({ state: "absent" });

function presentMember(decoded: Decoded): Result<MemberValue, DecodeRefused> {
  return decoded.ok ? ok({ state: "filled", value: decoded.value }) : decoded;
}

/** A member that the text leaves out: refused, absent, or filled with its default. */
function absentMember(
  presence: MemberPresence,
  kind: Kind,
  path: string,
  level: number,
  form: DecodeForm,
): Result<MemberValue, DecodeRefused> {
  switch (presence.mode) {
    case "required":
      return err(decodeRefused("MissingMember", path, "a required member is missing"));
    case "optional":
      return ok(ABSENT);
    case "defaulted":
      return defaultedMember(presence.fallback, kind, path, level, form);
    default:
      return unreachable(presence, "absentMember");
  }
}

/** A defaulted member that the text leaves out: filled in the worker form, absent in the stored form. */
function defaultedMember(
  fallback: DefaultText,
  kind: Kind,
  path: string,
  level: number,
  form: DecodeForm,
): Result<MemberValue, DecodeRefused> {
  switch (form) {
    case "worker":
      return presentMember(filledDefault(fallback, kind, path, level));
    case "stored":
      return ok(ABSENT);
    default:
      return unreachable(form, "defaultedMember");
  }
}

/** The default, decoded from its stored text, so that each value is new. */
function filledDefault(fallback: DefaultText, kind: Kind, path: string, level: number): Decoded {
  switch (fallback.state) {
    case "stored":
      return decodeNode(kind, readTree(fallback.text), path, level, "worker");
    case "refused":
      throw new TypeError(`decode: the default of ${path} does not encode; checkKind refuses this wire type`);
    default:
      return unreachable(fallback, "filledDefault");
  }
}
