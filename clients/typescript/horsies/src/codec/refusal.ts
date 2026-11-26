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
  | "NotIsoCalendar"
  /** An `h.uuid()` value that is not the lower-case 8-4-4-4-12 form. */
  | "NotCanonicalUuid"
  /** A value that is not a member of a literal, an enum or a union, or a tag that names no union member. */
  | "NotAMember"
  /** A user-side conversion or check refused the value; `detail` names the issues (paths and codes). */
  | "CheckFailed";

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
  | "BadTemporalText"
  /** A string that is not a finite decimal text. */
  | "BadDecimalText"
  /** A string that is not a UUID in the lower-case 8-4-4-4-12 form. */
  | "BadUuidText"
  /** A stored value that is not a member of a literal, an enum or a union, or a tag that names no union member. */
  | "NotAMember"
  /** A user-side conversion or check refused the read value; `detail` names the issues (paths and codes). */
  | "CheckFailed";

/** The writer refuses a value. */
export type EncodeRefused = {
  readonly side: "encode";
  readonly kind: EncodeRefusalKind;
  readonly path: string;
  readonly detail: string;
};

/** The reader refuses a stored value under its declared kind. */
export type ValueRefused = {
  readonly side: "decode";
  readonly kind: DecodeRefusalKind;
  readonly path: string;
  readonly detail: string;
};

/**
 * The rules of a stored result envelope and of a stored task error (the core's `read_task_result_text` and
 * `TaskError::from_stored_text`).
 */
export type EnvelopeRule =
  /** The envelope is not a JSON object. */
  | "NotAnObject"
  /** The marker `__h_task_result__` is missing or is not `true`. */
  | "MarkerMissing"
  /** The envelope has a key other than the marker, `ok` and `err`. */
  | "ExtraKey"
  /** The `ok` or the `err` slot is missing. */
  | "MissingSlot"
  /** Both slots are set. */
  | "BothSlotsSet"
  /** The stored error is not a JSON object. */
  | "ErrorNotAnObject"
  /** The stored error has a key that its class does not have. */
  | "ErrorUnknownKey"
  /** The stored error leaves out a required key (`error_code`, or a sub-workflow field). */
  | "ErrorMissingField"
  /** A field of the stored error does not read as its type. */
  | "ErrorFieldInvalid"
  /** The tagged code names no built-in code. */
  | "ErrorUnknownBuiltInCode"
  /** The `data`, or the output of a sub-workflow summary, breaks the rules of a user value. */
  | "ErrorDataRefused";

/** A stored result envelope or task error breaks a rule of its form. */
export type EnvelopeRefused = {
  readonly side: "decode";
  readonly kind: "EnvelopeRefused";
  readonly rule: EnvelopeRule;
  readonly path: string;
  readonly detail: string;
};

/** The reader refuses a stored text: a value under its kind, or the form of a result envelope. */
export type DecodeRefused = ValueRefused | EnvelopeRefused;

/** The worker could not read the text that the sender wrote: `cause` is the reader's refusal. */
export type ReadBackRefused = {
  readonly side: "readBack";
  readonly kind: "ReadBackRefused";
  readonly path: string;
  readonly detail: string;
  readonly cause: DecodeRefused;
};

/**
 * The value that a reader gives writes another text than the sender's (a conversion whose encode and decode do not
 * invert). `detail` names the offset of the first difference; it holds no value.
 */
export type ReadBackChanged = {
  readonly side: "readBack";
  readonly kind: "ReadBackChanged";
  readonly path: string;
  readonly detail: string;
};

/** A refusal of the codec. */
export type CodecRefused = EncodeRefused | DecodeRefused | ReadBackRefused | ReadBackChanged;

/**
 * The text of a refusal, as the Python client writes a validation summary: `<path>: <detail> [<kind>]`. A part of
 * the path that comes from the value is `<input>`; the text holds no value. Pure.
 */
export function refusalText(refused: CodecRefused): string {
  return `${refused.path}: ${refused.detail} [${refused.kind}]`;
}

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
export function decodeRefused(kind: DecodeRefusalKind, path: string, detail: string): ValueRefused {
  return Object.freeze({ side: "decode", kind, path, detail });
}

/** A refusal of the form of a result envelope or a stored task error. Pure. */
export function envelopeRefused(rule: EnvelopeRule, path: string, detail: string): EnvelopeRefused {
  return Object.freeze({ side: "decode", kind: "EnvelopeRefused", rule, path, detail });
}
