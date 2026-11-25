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
  | "LoneSurrogate"
  /** An object has a key that its declared type does not have. */
  | "UnknownMember"
  /** An object leaves out a member that is not `.optional()` and has no `.default()`. */
  | "MissingMember"
  /** A key that only the engine writes: it starts with `__h_` or `__horsies_`, or is `__builtin_task_code__`. */
  | "EngineKey"
  /** The value is nested deeper than the core's bound of a user value. */
  | "TooDeep"
  /** The value is not data: a `Date`, `Map`, `Set`, function, class instance, or an object with a symbol key. */
  | "NotJsonNative"
  /** A date-time outside the years 1 to 9999 (Python's `datetime` range). */
  | "YearOutOfRange"
  /** An `h.offsetDateTime()` value whose zone is named, not a fixed offset. */
  | "NamedTimeZone"
  /** A Temporal value of a calendar other than `iso8601`. */
  | "NotIsoCalendar";

/** Why the reader refuses a stored text under the declared kind. */
export type DecodeRefusalKind =
  /** The stored value is not of the declared kind. */
  | "WrongType"
  /** An integer kind holds a float text (`7.0`). */
  | "FloatForInteger"
  /** An `h.int()` value is outside the safe integer range. */
  | "IntegerOutOfRange"
  /** An object has a key that its declared type does not have. */
  | "UnknownMember"
  /** An object leaves out a member that is not `.optional()` and has no `.default()`. */
  | "MissingMember"
  /** A key that only the engine writes. */
  | "EngineKey"
  /** The value is nested deeper than the core's bound of a user value. */
  | "TooDeep"
  /** A string that is not the stored text of its date-time kind. */
  | "BadTemporalText";

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

/** Written in place of a part of a path that comes from the value: a record key (as the Python and Rust clients). */
export const INPUT_PLACEHOLDER = "<input>";

/** The path of a declared member: its name is part of the type, not of the value. Pure. */
export function memberPath(path: string, name: string): string {
  return `${path}.${name}`;
}

/** The path of an array or tuple item. Pure. */
export function itemPath(path: string, index: number): string {
  return `${path}[${index}]`;
}

/** The path of a record entry or of an undeclared key: the key is part of the value, so it is not written. Pure. */
export function entryPath(path: string): string {
  return `${path}.${INPUT_PLACEHOLDER}`;
}

/** A refusal of the writer. Pure. */
export function encodeRefused(kind: EncodeRefusalKind, path: string, detail: string): EncodeRefused {
  return Object.freeze({ side: "encode", kind, path, detail });
}

/** A refusal of the reader. Pure. */
export function decodeRefused(kind: DecodeRefusalKind, path: string, detail: string): DecodeRefused {
  return Object.freeze({ side: "decode", kind, path, detail });
}
