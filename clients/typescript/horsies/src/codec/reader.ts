// The reader: the value of a stored text under its declared kind.
//
// Precondition: the text is a `JsonText` (`json-text.ts`): the core's strict read rule ran on it, or the codec wrote
// it. The reader refuses every typed mismatch; it does not check the strict rule again.

import { kindOf } from "../wire/builders.ts";
import type { AnyWire, ValueOf } from "../wire/infer.ts";
import type { Kind } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { JsonText } from "./json-text.ts";
import { IntegralFloat, readTree, type TreeValue } from "./number-text.ts";
import { decodeRefused, type DecodeRefusalKind, type DecodeRefused, ROOT_PATH } from "./refusal.ts";
import { isBigint, isBoolean, isNumber, isString } from "./values.ts";

type Decoded = Result<unknown, DecodeRefused>;

/**
 * The value of `text` under `kind`. Pure.
 *
 * Fails with the first refusal of the walk (`DecodeRefused`). A text that is not JSON is a defect of the path that
 * made the `JsonText` (`readTree` throws a `TypeError`).
 */
export function decodeValue(kind: Kind, text: JsonText): Decoded {
  return decodeNode(kind, readTree(text), ROOT_PATH);
}

/** The value of `text` under a wire type. Pure; see `decodeValue`. */
export function decodeWire<W extends AnyWire>(wire: W, text: JsonText): Result<ValueOf<W>, DecodeRefused> {
  const decoded = decodeValue(kindOf(wire), text);

  // SAFETY: the reader builds the value from the kind tree of `wire`, and `ValueOf<W>` is the value type of that tree.
  return decoded as Result<ValueOf<W>, DecodeRefused>;
}

function refuse(kind: DecodeRefusalKind, path: string, detail: string): Decoded {
  return err(decodeRefused(kind, path, detail));
}

/** The value of one tree node under `kind`. Pure. */
export function decodeNode(kind: Kind, node: TreeValue, path: string): Decoded {
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
    default:
      return unreachable(kind, "decodeNode");
  }
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
