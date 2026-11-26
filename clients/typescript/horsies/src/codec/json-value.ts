// `h.json()`: untyped JSON that a type names explicitly.
//
// JavaScript has one number type and Python two, so the writer chooses by the value: a `bigint` is an integer; a
// `number` that is a safe integer, and not `-0`, is an integer; every other finite `number` is Python's float text
// (`1e+16`, `1e-05`, `-0.0`). The reader gives a `number` for an integer text in the safe range and for a float text,
// and a `bigint` for an integer text outside the safe range. A stored float with an integral value in the safe range
// (`7.0`) reads as `7` and writes as `7` again.

import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { isEngineKey } from "./engine-keys.ts";
import { IntegralFloat, isTreeArray, isTreeObject, type TreeValue } from "./number-text.ts";
import {
  decodeRefused,
  type DecodeRefused,
  encodeRefused,
  type EncodeRefusalKind,
  type EncodeRefused,
  entryPath,
  itemPath,
} from "./refusal.ts";
import { bigintText, floatText, type ScalarRefused, stringText } from "./scalar-text.ts";
import { isArray, isPlainObject, setOwn } from "./values.ts";

/** A JSON value: a `bigint` holds an integer outside the safe range of a `number`. */
export type JsonValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

type Written = Result<true, EncodeRefused>;

const WRITTEN: Written = Object.freeze(ok<true>(true));

/**
 * What a walk does with a key that only the engine writes: refuse it (a user value), or keep it (the `data` of a task
 * error, which the core replaces when it stores the error).
 */
type EngineKeys = "refused" | "kept";

/** The state of one walk: the text built so far and its rules. */
type JsonWalk = { text: string; readonly levelLimit: number; readonly engineKeys: EngineKeys };

/** The stored text of a JSON value at `depth` (the arrays and objects around it), with at most `levelLimit` levels. */
export function jsonValueText(
  value: unknown,
  path: string,
  depth: number,
  levelLimit: number,
): Result<string, EncodeRefused> {
  const walk: JsonWalk = { text: "", levelLimit, engineKeys: "refused" };
  const written = writeJson(value, path, depth, walk);

  return written.ok ? ok(walk.text) : written;
}

/**
 * The text of a JSON value that keeps a key that only the engine writes, with at most `levelLimit` levels. The other
 * rules of `jsonValueText` hold. Pure.
 */
export function engineKeysKeptText(value: unknown, path: string, levelLimit: number): Result<string, EncodeRefused> {
  const walk: JsonWalk = { text: "", levelLimit, engineKeys: "kept" };
  const written = writeJson(value, path, 0, walk);

  return written.ok ? ok(walk.text) : written;
}

function refuse(kind: EncodeRefusalKind, path: string, detail: string): Written {
  return err(encodeRefused(kind, path, detail));
}

function append(walk: JsonWalk, text: string): Written {
  walk.text += text;

  return WRITTEN;
}

function appendScalar(walk: JsonWalk, text: Result<string, ScalarRefused>, path: string): Written {
  return text.ok ? append(walk, text.value) : refuse(text.error.kind, path, text.error.detail);
}

function writeJson(value: unknown, path: string, depth: number, walk: JsonWalk): Written {
  switch (typeof value) {
    case "undefined":
      return refuse("ExplicitUndefined", path, "undefined is not a stored value; leave the key out");
    case "boolean":
      return append(walk, String(value));
    case "string":
      return appendScalar(walk, stringText(value), path);
    case "bigint":
      return appendScalar(walk, bigintText(value), path);
    case "number":
      return Number.isSafeInteger(value) && !Object.is(value, -0)
        ? append(walk, String(value))
        : appendScalar(walk, floatText(value), path);
    case "object":
      return value === null ? append(walk, "null") : writeJsonContainer(value, path, depth, walk);
    case "function":
    case "symbol":
      return refuse("NotJsonNative", path, "not data: a function or a symbol");
  }
}

function writeJsonContainer(value: unknown, path: string, depth: number, walk: JsonWalk): Written {
  const level = depth + 1;

  if (!isArray(value) && !isPlainObject(value)) {
    return refuse("NotJsonNative", path, "not data: a Date, Map, Set, class instance or symbol key");
  }

  if (level > walk.levelLimit) {
    return refuse("TooDeep", path, `more than ${walk.levelLimit} levels`);
  }


  return isArray(value) ? writeJsonArray(value, path, level, walk) : writeJsonObject(value, path, level, walk);
}

function writeJsonArray(items: readonly unknown[], path: string, level: number, walk: JsonWalk): Written {
  walk.text += "[";

  for (let index = 0; index < items.length; index += 1) {
    walk.text += index > 0 ? "," : "";
    const written = writeJson(items[index], itemPath(path, index), level, walk);

    if (!written.ok) {
      return written;
    }
  }

  return append(walk, "]");
}

function writeJsonObject(
  object: Readonly<Record<string, unknown>>,
  path: string,
  level: number,
  walk: JsonWalk,
): Written {
  walk.text += "{";
  let first = true;

  for (const key of Object.keys(object)) {
    const at = entryPath(path);

    if (isEngineKey(key)) {
      const kept = engineKey(walk, at);

      if (!kept.ok) {
        return kept;
      }
    }

    walk.text += first ? "" : ",";
    first = false;
    const keyWritten = appendScalar(walk, stringText(key), at);
    const written = keyWritten.ok ? writeJsonEntry(object[key], at, level, walk) : keyWritten;

    if (!written.ok) {
      return written;
    }
  }

  return append(walk, "}");
}

/** A key that only the engine writes: refused, or kept. */
function engineKey(walk: JsonWalk, path: string): Written {
  switch (walk.engineKeys) {
    case "refused":
      return refuse("EngineKey", path, "a key that only the engine writes");
    case "kept":
      return WRITTEN;
    default:
      return unreachable(walk.engineKeys, "engineKey");
  }
}

function writeJsonEntry(value: unknown, path: string, level: number, walk: JsonWalk): Written {
  walk.text += ":";

  return writeJson(value, path, level, walk);
}

/** The JSON value of a tree node at `depth`, with at most `levelLimit` levels; an engine key is refused. Pure. */
export function decodeJsonNode(
  node: TreeValue,
  path: string,
  depth: number,
  levelLimit: number,
): Result<JsonValue, DecodeRefused> {
  if (node instanceof IntegralFloat) {
    return ok(node.value);
  }

  if (!isTreeArray(node) && !isTreeObject(node)) {
    return ok(node);
  }

  const level = depth + 1;

  if (level > levelLimit) {
    return err(decodeRefused("TooDeep", path, `more than ${levelLimit} levels`));
  }

  return isTreeArray(node)
    ? decodeJsonArray(node, path, level, levelLimit)
    : decodeJsonObject(node, path, level, levelLimit);
}

function decodeJsonArray(
  items: readonly TreeValue[],
  path: string,
  level: number,
  levelLimit: number,
): Result<JsonValue, DecodeRefused> {
  const values: JsonValue[] = [];

  for (const [index, item] of items.entries()) {
    const decoded = decodeJsonNode(item, itemPath(path, index), level, levelLimit);

    if (!decoded.ok) {
      return decoded;
    }

    values.push(decoded.value);
  }

  return ok(values);
}

function decodeJsonObject(
  node: { readonly [key: string]: TreeValue },
  path: string,
  level: number,
  levelLimit: number,
): Result<JsonValue, DecodeRefused> {
  const object: Record<string, JsonValue> = {};

  for (const [key, entry] of Object.entries(node)) {
    const at = entryPath(path);

    if (isEngineKey(key)) {
      return err(decodeRefused("EngineKey", at, "a key that only the engine writes"));
    }

    const decoded = decodeJsonNode(entry, at, level, levelLimit);

    if (!decoded.ok) {
      return decoded;
    }

    setOwn(object, key, decoded.value);
  }

  return ok(object);
}
