// The codec entry points that take a wire type: the static type of the value follows the wire type.

import type { AnyWire, SentOf, ValueOf } from "../wire/infer.ts";
import type { Result } from "../wire/result.ts";
import { kindOf } from "../wire/wire-type.ts";
import type { JsonText } from "./json-text.ts";
import { type DecodeForm, decodeValue } from "./reader.ts";
import type { DecodeRefused, EncodeRefused } from "./refusal.ts";
import { encodeValue } from "./writer.ts";

/** The stored text of a sender's value under a wire type. Pure; see `encodeValue`. */
export function encodeWire<W extends AnyWire>(wire: W, value: SentOf<W>): Result<JsonText, EncodeRefused> {
  return encodeValue(kindOf(wire), value);
}

/** The value of a stored text under a wire type. Pure; see `decodeValue`. */
export function decodeWire<W extends AnyWire>(
  wire: W,
  text: JsonText,
  form: DecodeForm,
): Result<ValueOf<W>, DecodeRefused> {
  const decoded = decodeValue(kindOf(wire), text, form);

  // SAFETY: the reader builds the value from the kind tree of `wire`, and `ValueOf<W>` is the value type of that tree.
  return decoded as Result<ValueOf<W>, DecodeRefused>;
}
