// The refusal catalog of the codec: a closed union by `kind`. `path` names the place in the value (`$` is the whole
// value). `detail` names the rule, never a value.

/** Why the writer refuses a value. */
export type EncodeRefusalKind =
  /** A value is `undefined`. Leave the key out instead. */
  | "ExplicitUndefined"
  /** The value is not of the declared kind. */
  | "WrongType"
  /** An `h.int()` value is not a safe integer. */
  | "NotSafeInteger"
  /** An integer has more digits than the core stores. */
  | "IntegerTooLong"
  /** A float is `NaN` or an infinity. */
  | "NonFiniteFloat"
  /** A string holds a lone UTF-16 surrogate. */
  | "LoneSurrogate";

/** Why the reader refuses a stored text under the declared kind. */
export type DecodeRefusalKind =
  /** The stored value is not of the declared kind. */
  | "WrongType"
  /** An integer kind holds a float text (`7.0`). */
  | "FloatForInteger"
  /** An `h.int()` value is outside the safe integer range. */
  | "IntegerOutOfRange";

/** The writer refuses a value. */
export type EncodeRefused = {
  readonly side: "encode";
  readonly kind: EncodeRefusalKind;
  readonly path: string;
  readonly detail: string;
};

/** The reader refuses a stored text. */
export type DecodeRefused = {
  readonly side: "decode";
  readonly kind: DecodeRefusalKind;
  readonly path: string;
  readonly detail: string;
};

/** A refusal of the codec. */
export type CodecRefused = EncodeRefused | DecodeRefused;

/** The path of a whole value. */
export const ROOT_PATH = "$";

/** A refusal of the writer. Pure. */
export function encodeRefused(kind: EncodeRefusalKind, path: string, detail: string): EncodeRefused {
  return Object.freeze({ side: "encode", kind, path, detail });
}

/** A refusal of the reader. Pure. */
export function decodeRefused(kind: DecodeRefusalKind, path: string, detail: string): DecodeRefused {
  return Object.freeze({ side: "decode", kind, path, detail });
}
