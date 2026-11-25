//! The fixed text of `monitoring_models.ts`: the refusal types that the
//! module exports, and the module-private decode helpers that every
//! rendered decoder calls. The renderer writes this text as it is.
//!
//! The helpers narrow `WireJson` values with `typeof`, so the decoder module
//! needs the lint override of `anti-slop/no-runtime-typeof` that the codec
//! folder has.

/// The exported refusal types: the closed union `MonitoringDecodeError`,
/// told apart by `kind`, and its variants. They name the type
/// `MonitoringModelName`, which the renderer writes before this text.
pub const REFUSAL_TYPES: &str = r#"/** The JSON kind of a field, as `kindMismatch` names it. */
export type MonitoringWireKind =
  | "object"
  | "list"
  | "string"
  | "integer"
  | "number"
  | "boolean";

/** A field of the model is not in the input. */
export interface MonitoringFieldMissing {
  readonly kind: "fieldMissing";
  readonly model: MonitoringModelName;
  readonly path: string;
}

/** The input has a field that the model does not have. */
export interface MonitoringFieldUnknown {
  readonly kind: "fieldUnknown";
  readonly model: MonitoringModelName;
  readonly path: string;
}

/** A value is not of the JSON kind of its field. */
export interface MonitoringKindMismatch {
  readonly kind: "kindMismatch";
  readonly model: MonitoringModelName;
  readonly path: string;
  readonly expected: MonitoringWireKind;
}

/** An integer is outside the range that a `number` holds exactly. */
export interface MonitoringIntegerUnsafe {
  readonly kind: "integerUnsafe";
  readonly model: MonitoringModelName;
  readonly path: string;
  readonly value: bigint;
}

/** A text is not the date-time form that the core writes. */
export interface MonitoringDateTimeMalformed {
  readonly kind: "dateTimeMalformed";
  readonly model: MonitoringModelName;
  readonly path: string;
  readonly text: string;
}

/** A text is not the UUID form that the core writes. */
export interface MonitoringUuidMalformed {
  readonly kind: "uuidMalformed";
  readonly model: MonitoringModelName;
  readonly path: string;
  readonly text: string;
}

/** A text is not a value of the enum of its field. */
export interface MonitoringEnumValueUnknown {
  readonly kind: "enumValueUnknown";
  readonly model: MonitoringModelName;
  readonly path: string;
  readonly text: string;
}

/** The tag of a union value names no variant of the union. */
export interface MonitoringTagUnknown {
  readonly kind: "tagUnknown";
  readonly model: MonitoringModelName;
  readonly path: string;
  readonly tag: string;
}

/**
 * Why a monitoring value of the core does not decode. `path` is the place of
 * the value in the input: `$` is the input, `.key` a field, `[n]` a list item
 * and `["key"]` a map entry.
 */
export type MonitoringDecodeError =
  | MonitoringFieldMissing
  | MonitoringFieldUnknown
  | MonitoringKindMismatch
  | MonitoringIntegerUnsafe
  | MonitoringDateTimeMalformed
  | MonitoringUuidMalformed
  | MonitoringEnumValueUnknown
  | MonitoringTagUnknown;"#;

/// The module-private decode helpers: the place of a value in the input,
/// the field reader, the leaf decoders and the decoders of nullable values,
/// lists and maps.
pub const DECODE_HELPERS: &str = r#"/** Where a value is in the input: the model that the decode started from, and the path. */
interface DecodePlace {
  readonly model: MonitoringModelName;
  readonly path: string;
}

/** A decoder of one value of the input. */
type Decoder<T> = (value: WireJson, place: DecodePlace) => DecodeResult<T, MonitoringDecodeError>;

/** A decode outcome without its value. */
type Outcome =
  | { readonly ok: true }
  | { readonly ok: false; readonly error: MonitoringDecodeError };

/** `R` with each outcome narrowed to its decoded side. */
type AllDecoded<R> = { readonly [K in keyof R]: Extract<R[K], { readonly ok: true }> };

/** The largest integer that a `number` holds exactly. */
const MAX_SAFE_INTEGER = 9_007_199_254_740_991n;

/** The date-time text that the core writes: whole seconds, or six fraction digits, then `Z`. */
const DATE_TIME_TEXT =
  /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d{6})?Z$/;

/** The end of the year and month in a date-time text. */
const YEAR_MONTH_END = 7;

/** The start of the day in a date-time text. */
const DAY_START = 8;

/** The end of the day in a date-time text. */
const DAY_END = 10;

/** A UUID as the core writes it: lower-case hex digits in groups of 8, 4, 4, 4 and 12. */
const UUID_TEXT = /^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/;

/** The fields of one object of the input, and the keys that the decode read. */
class FieldReader {
  readonly place: DecodePlace;
  readonly #fields: ReadonlyMap<string, WireJson>;
  readonly #readKeys = new Set<string>();

  constructor(fields: ReadonlyMap<string, WireJson>, place: DecodePlace) {
    this.#fields = fields;
    this.place = place;
  }

  /** Decode the field `key`. A key that is not in the object is `fieldMissing`. */
  read<T>(key: string, decode: Decoder<T>): DecodeResult<T, MonitoringDecodeError> {
    const place = fieldPlace(this.place, key);
    const value = this.#fields.get(key);

    this.#readKeys.add(key);

    if (value === undefined) {
      return refuse({ kind: "fieldMissing", model: place.model, path: place.path });
    }

    return decode(value, place);
  }

  /** `fieldUnknown` for the first key of the object that no `read` took. */
  unread(): Outcome {
    for (const key of this.#fields.keys()) {
      if (!this.#readKeys.has(key)) {
        const place = fieldPlace(this.place, key);

        return refuse({ kind: "fieldUnknown", model: place.model, path: place.path });
      }
    }

    return { ok: true };
  }
}

/** The place of the field `key` of the object at `place`. */
function fieldPlace(place: DecodePlace, key: string): DecodePlace {
  return { model: place.model, path: `${place.path}.${key}` };
}

/** The place of the item `index` of the list at `place`. */
function itemPlace(place: DecodePlace, index: number): DecodePlace {
  return { model: place.model, path: `${place.path}[${index}]` };
}

/** The place of the entry `key` of the map at `place`. */
function entryPlace(place: DecodePlace, key: string): DecodePlace {
  return { model: place.model, path: `${place.path}[${JSON.stringify(key)}]` };
}

/** A decoded value. */
function ok<T>(value: T): DecodeResult<T, MonitoringDecodeError> {
  return { ok: true, value };
}

/** A refusal, with the error frozen. */
function refuse(error: MonitoringDecodeError): DecodeResult<never, MonitoringDecodeError> {
  return { ok: false, error: Object.freeze(error) };
}

/** `kindMismatch` at `place`. */
function kindMismatch(
  place: DecodePlace,
  expected: MonitoringWireKind,
): DecodeResult<never, MonitoringDecodeError> {
  return refuse({ kind: "kindMismatch", model: place.model, path: place.path, expected });
}

/** `tagUnknown` for the tag field `key` of `fields`. */
function tagUnknown(
  fields: FieldReader,
  key: string,
  tag: string,
): DecodeResult<never, MonitoringDecodeError> {
  const place = fieldPlace(fields.place, key);

  return refuse({ kind: "tagUnknown", model: place.model, path: place.path, tag });
}

/** `kindMismatch` for a value that is not a text, else `enumValueUnknown`. */
function enumValueUnknown(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<never, MonitoringDecodeError> {
  if (typeof value !== "string") {
    return kindMismatch(place, "string");
  }

  return refuse({ kind: "enumValueUnknown", model: place.model, path: place.path, text: value });
}

/** Whether every outcome of `results` decoded. */
function allDecoded<R extends { readonly [key: string]: Outcome }>(
  results: R,
): results is R & AllDecoded<R> {
  return Object.values(results).every((outcome) => outcome.ok);
}

/** The first failure of `results`, in field order. A call with no failure is a defect. */
function firstFailure(results: {
  readonly [key: string]: Outcome;
}): DecodeResult<never, MonitoringDecodeError> {
  for (const outcome of Object.values(results)) {
    if (!outcome.ok) {
      return outcome;
    }
  }

  throw new Error("firstFailure: every field decoded");
}

/** Whether a JSON container is a list. */
function isList(
  value: ReadonlyArray<WireJson> | ReadonlyMap<string, WireJson>,
): value is ReadonlyArray<WireJson> {
  return Array.isArray(value);
}

/** The fields of an object value. Another value is `kindMismatch`. */
function readObject(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<FieldReader, MonitoringDecodeError> {
  if (value === null || typeof value !== "object" || isList(value)) {
    return kindMismatch(place, "object");
  }

  return ok(new FieldReader(value, place));
}

/** A text. */
function decodeString(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<string, MonitoringDecodeError> {
  if (typeof value !== "string") {
    return kindMismatch(place, "string");
  }

  return ok(value);
}

/** `true` or `false`. */
function decodeBoolean(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<boolean, MonitoringDecodeError> {
  if (typeof value !== "boolean") {
    return kindMismatch(place, "boolean");
  }

  return ok(value);
}

/** A float. The core writes a float with a fraction or an exponent; an integer is refused. */
function decodeNumber(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<number, MonitoringDecodeError> {
  if (typeof value !== "number") {
    return kindMismatch(place, "number");
  }

  return ok(value);
}

/** An integer as a `number`. An integer outside the safe range of a `number` is `integerUnsafe`. */
function decodeInteger(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<number, MonitoringDecodeError> {
  if (typeof value !== "bigint") {
    return kindMismatch(place, "integer");
  }

  if (value > MAX_SAFE_INTEGER || value < -MAX_SAFE_INTEGER) {
    return refuse({ kind: "integerUnsafe", model: place.model, path: place.path, value });
  }

  return ok(Number(value));
}

/** A date-time text of the core as a `Temporal.Instant`, with its microseconds. */
function decodeDateTime(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<Temporal.Instant, MonitoringDecodeError> {
  if (typeof value !== "string") {
    return kindMismatch(place, "string");
  }

  if (!DATE_TIME_TEXT.test(value) || !dayExists(value)) {
    return refuse({ kind: "dateTimeMalformed", model: place.model, path: place.path, text: value });
  }

  return ok(Temporal.Instant.from(value));
}

/** Whether the day of a text that `DATE_TIME_TEXT` matches is in its month: `2026-02-30` is not. */
function dayExists(text: string): boolean {
  const month = Temporal.PlainYearMonth.from(text.slice(0, YEAR_MONTH_END));

  return Number(text.slice(DAY_START, DAY_END)) <= month.daysInMonth;
}

/** A UUID text in the form that the core writes. */
function decodeUuid(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<string, MonitoringDecodeError> {
  if (typeof value !== "string") {
    return kindMismatch(place, "string");
  }

  if (!UUID_TEXT.test(value)) {
    return refuse({ kind: "uuidMalformed", model: place.model, path: place.path, text: value });
  }

  return ok(value);
}

/** A decoder of `null` or a value that `decode` decodes. */
function nullableOf<T>(decode: Decoder<T>): Decoder<T | null> {
  return (value, place) => (value === null ? ok(null) : decode(value, place));
}

/** A decoder of a list whose items `decode` decodes. The list is frozen. */
function listOf<T>(decode: Decoder<T>): Decoder<ReadonlyArray<T>> {
  return (value, place) => {
    if (value === null || typeof value !== "object" || !isList(value)) {
      return kindMismatch(place, "list");
    }

    const items: Array<T> = [];

    for (const [index, item] of value.entries()) {
      const result = decode(item, itemPlace(place, index));

      if (!result.ok) {
        return result;
      }

      items.push(result.value);
    }

    return ok(Object.freeze(items));
  };
}

/** A decoder of an object whose values `decode` decodes. The map keeps the order of the keys. */
function mapOf<T>(decode: Decoder<T>): Decoder<ReadonlyMap<string, T>> {
  return (value, place) => {
    if (value === null || typeof value !== "object" || isList(value)) {
      return kindMismatch(place, "object");
    }

    const entries = new Map<string, T>();

    for (const [key, item] of value) {
      const result = decode(item, entryPlace(place, key));

      if (!result.ok) {
        return result;
      }

      entries.set(key, result.value);
    }

    return ok(entries);
  };
}"#;
