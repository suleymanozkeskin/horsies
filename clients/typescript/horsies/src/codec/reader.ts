// The reader: the value of a stored text under its declared kind.
//
// Precondition: the text is a `JsonText` (`json-text.ts`): the core's strict read rule ran on it, or the codec wrote
// it. The reader refuses every typed mismatch; it does not check the strict rule again.
//
// Rules:
// - An undeclared key is refused at every depth. A key that only the engine writes is refused in a record.
// - A member that the text leaves out: a required one is refused; an `.optional()` one stays absent; a defaulted one
//   is filled in the worker form and stays absent in the stored form (the form that a read-back writes again). A
//   computed default (a default function) runs once per worker-form decode.
// - Records are built with own data properties: a stored key `__proto__` never changes a prototype.

import { Decimal } from "../wire/decimal.ts";
import type {
  CheckedKind,
  ConversionKind,
  DefaultText,
  Kind,
  LiteralMember,
  Member,
  MemberPresence,
  ObjectKind,
  TaggedUnionKind,
  TupleKind,
} from "../wire/kinds.ts";
import { lazyTarget } from "../wire/lazy.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { TaskOutcome } from "../wire/task-error.ts";
import { outcomeValueKind } from "../wire/task-outcome.ts";
import { unreachable } from "../wire/unreachable.ts";
import { MAX_VALUE_JSON_DEPTH } from "./bounds.ts";
import { isEngineKey } from "./engine-keys.ts";
import { issuesText } from "./conversion.ts";
import { readEnvelopeSlots } from "./envelope.ts";
import type { JsonText } from "./json-text.ts";
import { decodeJsonNode } from "./json-value.ts";
import { IntegralFloat, isTreeArray, isTreeObject, readTree, type TreeObject, type TreeValue } from "./number-text.ts";
import {
  decodeRefused,
  type DecodeRefusalKind,
  type DecodeRefused,
  entryPath,
  itemPath,
  memberPath,
  refusalText,
  ROOT_PATH,
} from "./refusal.ts";
import {
  readInstant,
  readOffsetDateTime,
  readPlainDate,
  readPlainDateTime,
  readPlainTime,
  type TemporalReadRefused,
} from "./temporal-text.ts";
import { tupleItem, tupleLength } from "./tuple-items.ts";
import { isLiteralType, unionRefusal } from "./union.ts";
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

/**
 * The outcome of a stored result envelope whose `ok` value has the kind `okKind` (a task read, a workflow read).
 * Pure; see `decodeValue`.
 */
export function decodeEnvelope(
  okKind: Kind,
  text: JsonText,
  form: DecodeForm,
): Result<TaskOutcome<unknown>, DecodeRefused> {
  return decodeOutcome(okKind, readTree(text), ROOT_PATH, form);
}

/**
 * The outcome of the stored result envelope `node` at `path` whose `ok` value has the kind `okKind` (a node of the
 * workflow context). Pure; see `decodeValue`.
 */
export function decodeEnvelopeNode(
  okKind: Kind,
  node: TreeValue,
  path: string,
  form: DecodeForm,
): Result<TaskOutcome<unknown>, DecodeRefused> {
  return decodeOutcome(okKind, node, path, form);
}

/** The parameters of a stored task input. Pure; see `decodeValue`. */
export function decodeParameters(parameters: ObjectKind, text: JsonText, form: DecodeForm): Decoded {
  return decodeParameterTree(parameters, readTree(text), form);
}

/** The parameters of the tree of a stored task input (`readTree` of its text). Pure; see `decodeValue`. */
export function decodeParameterTree(parameters: ObjectKind, tree: TreeValue, form: DecodeForm): Decoded {
  return decodeObject(parameters, tree, ROOT_PATH, PARAMETERS_DEPTH, form);
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
    case "plainTime":
      return decodeText(node, path, readPlainTime);
    case "literal":
      return decodeLiteral(kind.members, node, path);
    case "union":
      return decodeUnion(kind.members, node, path, depth, form);
    case "taggedUnion":
      return decodeTagged(kind, node, path, depth, form);
    case "nullable":
      return node === null ? ok(null) : decodeNode(kind.inner, node, path, depth, form);
    case "array":
      return decodeItems(() => kind.item, node, path, depth, form);
    case "tuple":
      return decodeTuple(kind, node, path, depth, form);
    case "record":
      return decodeRecord(kind.value, node, path, depth, form);
    case "object":
      return decodeObject(kind, node, path, depth, form);
    case "json":
      return decodeJsonNode(node, path, depth, MAX_VALUE_JSON_DEPTH);
    case "taskOutcome":
      // A stored error whose custom code the upstream contract does not list (for example from an older deployment
      // of the upstream task) decodes as any other custom code, so the parameter's code type does not hold for it.
      // Whether the input decode refuses such an error instead is open (#2557).
      return decodeOutcome(outcomeValueKind(kind, path), node, path, form);
    case "conversion":
      return decodeConverted(kind, node, path, depth, form);
    case "checked":
      return decodeChecked(kind, node, path, depth, form);
    case "lazy":
      return decodeNode(lazyTarget(kind, `decodeNode at ${path}`), node, path, depth, form);
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

/**
 * The outcome of a result envelope at `path`. The envelope and the error object are the engine's levels, so the `ok`
 * value starts at the top.
 */
function decodeOutcome(
  okKind: Kind,
  node: TreeValue,
  path: string,
  form: DecodeForm,
): Result<TaskOutcome<unknown>, DecodeRefused> {
  const slots = readEnvelopeSlots(node, path);

  if (!slots.ok) {
    return slots;
  }

  switch (slots.value.slot) {
    case "ok": {
      const value = decodeNode(okKind, slots.value.value, memberPath(path, "ok"), TOP_DEPTH, form);

      return value.ok ? ok(Object.freeze({ kind: "succeeded", value: value.value })) : value;
    }

    case "err":
      return ok(Object.freeze({ kind: "failed", error: slots.value.error }));
    default:
      return unreachable(slots.value, "decodeOutcome");
  }
}

/** The value that a user-side conversion gives from the stored value; it runs in both forms. */
function decodeConverted(
  kind: ConversionKind,
  node: TreeValue,
  path: string,
  depth: number,
  form: DecodeForm,
): Decoded {
  const stored = decodeNode(kind.stored, node, path, depth, form);

  if (!stored.ok) {
    return stored;
  }

  const value = kind.decode(stored.value);

  return value.ok
    ? value
    : refuse("CheckFailed", path, `the conversion refused the stored value: ${issuesText(value.error)}`);
}

/** A value of the inner kind; the worker form runs the user-side check, the stored form does not. */
function decodeChecked(kind: CheckedKind, node: TreeValue, path: string, depth: number, form: DecodeForm): Decoded {
  const value = decodeNode(kind.inner, node, path, depth, form);

  if (!value.ok) {
    return value;
  }

  switch (form) {
    case "worker": {
      const checked = kind.check(value.value);

      return checked.ok
        ? value
        : refuse("CheckFailed", path, `the check refused the read value: ${issuesText(checked.error)}`);
    }

    case "stored":
      return value;
    default:
      return unreachable(form, "decodeChecked");
  }
}

function decodeInt(node: TreeValue, path: string): Decoded {
  if (isNumber(node)) {
    return Number.isInteger(node) ? ok(node) : refuse("FloatForInteger", path, "a float text for an integer");
  }

  if (isBigint(node)) {
    return refuse("IntegerOutOfRange", path, "outside the safe integer range of the integer kind");
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

function decodeLiteral(members: readonly LiteralMember[], node: TreeValue, path: string): Decoded {
  if (node instanceof IntegralFloat) {
    return refuse("FloatForInteger", path, "a float text for an integer member");
  }

  if (!isLiteralType(node)) {
    return isBigint(node)
      ? refuse("NotAMember", path, "not one of the literal members")
      : refuse("WrongType", path, "expected a string, a number, a boolean or null");
  }

  return members.includes(node) ? ok(node) : refuse("NotAMember", path, "not one of the literal members");
}

/** The value of the first member that reads the node. */
function decodeUnion(
  members: readonly Kind[],
  node: TreeValue,
  path: string,
  depth: number,
  form: DecodeForm,
): Decoded {
  const refusals: DecodeRefused[] = [];

  for (const member of members) {
    const decoded = decodeNode(member, node, path, depth, form);

    if (decoded.ok) {
      return decoded;
    }

    refusals.push(decoded.error);
  }

  return err(unionRefusal(refusals, decodeRefused("NotAMember", path, "no member of the union reads the value")));
}

/** The member object that the stored tag names. */
function decodeTagged(kind: TaggedUnionKind, node: TreeValue, path: string, depth: number, form: DecodeForm): Decoded {
  if (!isTreeObject(node)) {
    return refuse("WrongType", path, "expected an object");
  }

  const tagValue = node[kind.tag];
  const variant = isString(tagValue) ? kind.byTag.get(tagValue) : undefined;

  return variant === undefined
    ? refuse("NotAMember", memberPath(path, kind.tag), "the tag names no member of the union")
    : decodeObject(variant, node, path, depth, form);
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

function decodeTuple(kind: TupleKind, node: TreeValue, path: string, depth: number, form: DecodeForm): Decoded {
  if (!isTreeArray(node)) {
    return refuse("WrongType", path, "expected an array");
  }

  const length = tupleLength(kind, node.length);

  if (length.state === "wrongLength") {
    return refuse("WrongType", path, length.detail);
  }

  return decodeItems((index) => tupleItem(kind, index), node, path, depth, form);
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

/**
 * The default, decoded from its stored text, so that each value is new. A computed default calls its function here,
 * once per decode: a value that does not encode is `DefaultRefused`; a function that throws is not caught.
 */
function filledDefault(fallback: DefaultText, kind: Kind, path: string, level: number): Decoded {
  switch (fallback.state) {
    case "stored":
      return decodeNode(kind, readTree(fallback.text), path, level, "worker");
    case "computed": {
      const text = fallback.compute();
      const detail = text.ok ? "" : `the default function gave a value that does not encode: ${refusalText(text.error)}`;

      return text.ok ? decodeNode(kind, readTree(text.value), path, level, "worker") : refuse("DefaultRefused", path, detail);
    }

    case "refused":
      throw new TypeError(`decode: the default of ${path} does not encode; checkKind refuses this wire type`);
    default:
      return unreachable(fallback, "filledDefault");
  }
}
