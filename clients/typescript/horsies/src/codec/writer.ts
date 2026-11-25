// The writer: Python's stored text of a value under its declared kind. It builds the text from the kind tree and
// calls `JSON.stringify` for one string at a time only.

import { kindOf } from "../wire/builders.ts";
import type { AnyWire, SentOf } from "../wire/infer.ts";
import type { Kind } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { MAX_INT_DIGITS } from "./bounds.ts";
import { pythonFloatText } from "./float-text.ts";
import { type JsonText, writtenJsonText } from "./json-text.ts";
import { encodeRefused, type EncodeRefusalKind, type EncodeRefused, ROOT_PATH } from "./refusal.ts";
import { isBigint, isBoolean, isNumber, isString } from "./values.ts";

/** The text that one call builds. Never shared between calls. */
type Sink = { text: string };

type Written = Result<true, EncodeRefused>;

const WRITTEN: Written = Object.freeze(ok<true>(true));

/**
 * The stored text of `value` under `kind`. Pure.
 *
 * Fails with the first refusal of the walk (`EncodeRefused`); no text is returned then. Does no I/O.
 */
export function encodeValue(kind: Kind, value: unknown): Result<JsonText, EncodeRefused> {
  const sink: Sink = { text: "" };
  const written = writeValue(kind, value, ROOT_PATH, sink);

  return written.ok ? ok(writtenJsonText(sink.text)) : written;
}

/** The stored text of a sender's value under a wire type. Pure; see `encodeValue`. */
export function encodeWire<W extends AnyWire>(wire: W, value: SentOf<W>): Result<JsonText, EncodeRefused> {
  return encodeValue(kindOf(wire), value);
}

function refuse(kind: EncodeRefusalKind, path: string, detail: string): Written {
  return err(encodeRefused(kind, path, detail));
}

function append(sink: Sink, text: string): Written {
  sink.text += text;

  return WRITTEN;
}

function writeValue(kind: Kind, value: unknown, path: string, sink: Sink): Written {
  if (value === undefined) {
    return refuse("ExplicitUndefined", path, "undefined is not a stored value; leave the key out");
  }

  switch (kind.node) {
    case "null":
      return value === null ? append(sink, "null") : refuse("WrongType", path, "expected null");
    case "boolean":
      return isBoolean(value) ? append(sink, value ? "true" : "false") : refuse("WrongType", path, "expected a boolean");
    case "int":
      return writeInt(value, path, sink);
    case "bigint":
      return isBigint(value) ? writeBigint(value, path, sink) : refuse("WrongType", path, "expected a bigint");
    case "float":
      return isNumber(value) ? writeFloat(value, path, sink) : refuse("WrongType", path, "expected a number");
    case "string":
      return isString(value) ? writeString(value, path, sink) : refuse("WrongType", path, "expected a string");
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

/** An integer of at most `MAX_INT_DIGITS` digits. */
function writeBigint(value: bigint, path: string, sink: Sink): Written {
  const text = value.toString();
  const digits = value < 0n ? text.length - 1 : text.length;

  if (digits > MAX_INT_DIGITS) {
    return refuse("IntegerTooLong", path, `more than ${MAX_INT_DIGITS} digits`);
  }

  return append(sink, text);
}

/** Python's float text of a finite number. */
function writeFloat(value: number, path: string, sink: Sink): Written {
  if (!Number.isFinite(value)) {
    return refuse("NonFiniteFloat", path, "NaN and the infinities are not stored");
  }

  return append(sink, pythonFloatText(value));
}

/**
 * A well-formed string as `JSON.stringify` writes it, which is Python's text: `"`, `\` and U+0000 to U+001F are
 * escaped (`\b`, `\f`, `\n`, `\r`, `\t`, else `\u00xx`); other characters are written as they are.
 */
function writeString(value: string, path: string, sink: Sink): Written {
  if (!value.isWellFormed()) {
    return refuse("LoneSurrogate", path, "a string holds a lone surrogate");
  }

  return append(sink, JSON.stringify(value));
}
