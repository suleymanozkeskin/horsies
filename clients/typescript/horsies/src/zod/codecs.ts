// The `hz.*` builders: zod codecs for the kinds that zod cannot express exactly, and `hz.wire()`, which puts an
// `h.*` wire type inside a zod schema.
//
// Each codec stores Python's text form of its kind: the decode and the encode call the same text functions as the
// `h.*` kind, so a zod schema and an `h.*` schema store the same text. The adapter maps each codec to its `h.*` kind
// (it finds the codec by the brand on its decode function, `brand.ts`), so the codec's reader and writer rules apply,
// not zod's.

import { z } from "zod";
import type * as core from "zod/v4/core";

import type { DecimalRefused } from "../codec/decimal-text.ts";
import {
  instantText,
  isInstant,
  isPlainDate,
  isPlainDateTime,
  isPlainTime,
  isZonedDateTime,
  offsetDateTimeText,
  plainDateText,
  plainDateTimeText,
  plainTimeText,
  readInstant,
  readOffsetDateTime,
  readPlainDate,
  readPlainDateTime,
  readPlainTime,
  type TemporalWriteRefused,
} from "../codec/temporal-text.ts";
import { isObjectLike } from "../codec/values.ts";
import { encodeValue } from "../codec/writer.ts";
import { Decimal } from "../wire/decimal.ts";
import type { RequiredWire, SentOf, ValueOf } from "../wire/infer.ts";
import {
  DECIMAL_KIND,
  INSTANT_KIND,
  type Kind,
  OFFSET_DATE_TIME_KIND,
  PLAIN_DATE_KIND,
  PLAIN_DATE_TIME_KIND,
  PLAIN_TIME_KIND,
} from "../wire/kinds.ts";
import { isResultOfKind } from "../wire/result-of.ts";
import { ok, type Result } from "../wire/result.ts";
import { kindOf, presenceOf } from "../wire/wire-type.ts";
import { brandHz } from "./brand.ts";
import type { zodWireSent } from "./infer.ts";

/** A zod codec of horsies: stored as Python's text form of `T`, held as `T`. */
export type TextCodec<T> = z.ZodCodec<z.ZodString, z.ZodCustom<T, T>>;

/** A zod schema of an `h.*` wire type: zod's value type is the reader's value; the sender's value is `SentOf<W>`. */
export type WireSchema<W extends RequiredWire> = z.ZodCustom<ValueOf<W>, ValueOf<W>> & {
  readonly [zodWireSent]: SentOf<W>;
};

/** The `hz.*` builders. */
export interface ZodBuilders {
  /** A `Temporal.Instant`, stored in UTC: `2026-08-12T09:10:12.654321Z`. */
  instant(): TextCodec<Temporal.Instant>;
  /** A `Temporal.ZonedDateTime` with a fixed-offset zone, stored with the offset (`Z` for offset 0). */
  offsetDateTime(): TextCodec<Temporal.ZonedDateTime>;
  /** A `Temporal.PlainDateTime`, stored without an offset: `2026-08-12T09:10:12.654321`. */
  plainDateTime(): TextCodec<Temporal.PlainDateTime>;
  /** A `Temporal.PlainDate`, stored as `2026-08-12`. */
  plainDate(): TextCodec<Temporal.PlainDate>;
  /** A `Temporal.PlainTime`, stored as `09:10:12.654321`. */
  plainTime(): TextCodec<Temporal.PlainTime>;
  /** An exact `Decimal`, stored as Python's `str(Decimal)` text in a JSON string. */
  decimal(): TextCodec<Decimal>;
  /**
   * An `h.*` wire type inside a zod schema, stored as the wire type stores it. zod's own parse of it accepts the
   * values that the wire type writes. Use zod's `.optional()` and `.default()` on the result, not the wire type's.
   */
  wire<W extends RequiredWire>(wire: W): WireSchema<W>;
}

/** Why a text form refuses a value or a text; the detail holds no value. */
type TextRefused = { readonly detail: string };

/** A kind's text form: the read of a stored text, the write of a value, and the test of a value's type. */
type TextForm<T> = {
  readonly read: (text: string) => Result<T, TextRefused>;
  readonly write: (value: T) => Result<string, TextRefused>;
  readonly isValue: (value: unknown) => value is T;
};

type DecodeText<T> = (text: string, payload: core.ParsePayload<string>) => T;

type EncodeText<T> = (value: T, payload: core.ParsePayload<T>) => string;

/** One codec per kind: its decode and encode functions are built once; the decode function carries the kind's brand. */
type CodecFunctions<T> = { readonly decode: DecodeText<T>; readonly encode: EncodeText<T>; readonly form: TextForm<T> };

/** The functions of the codec of `kind`. Effects: brands the decode function with `kind`. */
function codecFunctions<T>(form: TextForm<T>, kind: Kind): CodecFunctions<T> {
  const decode: DecodeText<T> = (text, payload) => {
    const value = form.read(text);

    if (value.ok) {
      return value.value;
    }

    payload.issues.push({ code: "custom", message: value.error.detail, input: text });

    return z.NEVER;
  };

  const encode: EncodeText<T> = (value, payload) => {
    const text = form.write(value);

    if (text.ok) {
      return text.value;
    }

    payload.issues.push({ code: "custom", message: text.error.detail, input: value });

    return z.NEVER;
  };

  brandHz(decode, kind);

  return Object.freeze({ decode, encode, form });
}

function textCodec<T>(functions: CodecFunctions<T>): TextCodec<T> {
  return z.codec(z.string(), z.custom<T>(functions.form.isValue), {
    decode: functions.decode,
    encode: functions.encode,
  });
}

function readDecimal(text: string): Result<Decimal, DecimalRefused> {
  return Decimal.parse(text);
}

function decimalText(value: Decimal): Result<string, TextRefused> {
  return ok(value.toString());
}

function isDecimal(value: unknown): value is Decimal {
  return isObjectLike(value) && Decimal.isDecimal(value);
}

function timeText(value: Temporal.PlainTime): Result<string, TemporalWriteRefused> {
  return ok(plainTimeText(value));
}

const INSTANT = codecFunctions<Temporal.Instant>(
  { read: readInstant, write: instantText, isValue: isInstant },
  INSTANT_KIND,
);

const OFFSET_DATE_TIME = codecFunctions<Temporal.ZonedDateTime>(
  { read: readOffsetDateTime, write: offsetDateTimeText, isValue: isZonedDateTime },
  OFFSET_DATE_TIME_KIND,
);

const PLAIN_DATE_TIME = codecFunctions<Temporal.PlainDateTime>(
  { read: readPlainDateTime, write: plainDateTimeText, isValue: isPlainDateTime },
  PLAIN_DATE_TIME_KIND,
);

const PLAIN_DATE = codecFunctions<Temporal.PlainDate>(
  { read: readPlainDate, write: plainDateText, isValue: isPlainDate },
  PLAIN_DATE_KIND,
);

const PLAIN_TIME = codecFunctions<Temporal.PlainTime>(
  { read: readPlainTime, write: timeText, isValue: isPlainTime },
  PLAIN_TIME_KIND,
);

const DECIMAL = codecFunctions<Decimal>({ read: readDecimal, write: decimalText, isValue: isDecimal }, DECIMAL_KIND);

/**
 * `hz.wire(w)`: a `z.custom()` whose test function carries the brand of the wire type's kind. A wire type that may be
 * left out, or an upstream outcome, is a defect that the static types refuse.
 */
function wireSchema<W extends RequiredWire>(wire: W): WireSchema<W> {
  const kind = kindOf(wire);

  if (presenceOf(wire).mode !== "required" || isResultOfKind(kind)) {
    throw new TypeError("hz.wire: give a required h.* value type; use zod's .optional() and .default() on the result");
  }

  const schema = z.custom<ValueOf<W>>((value) => encodeValue(kind, value).ok);

  brandHz(schema._zod.def.fn, kind);

  // SAFETY: the sent type is type-only (`zodWireSent` does not exist at run time); the value is the zod schema.
  return schema as WireSchema<W>;
}

export const hz: ZodBuilders = Object.freeze({
  instant: (): TextCodec<Temporal.Instant> => textCodec(INSTANT),
  offsetDateTime: (): TextCodec<Temporal.ZonedDateTime> => textCodec(OFFSET_DATE_TIME),
  plainDateTime: (): TextCodec<Temporal.PlainDateTime> => textCodec(PLAIN_DATE_TIME),
  plainDate: (): TextCodec<Temporal.PlainDate> => textCodec(PLAIN_DATE),
  plainTime: (): TextCodec<Temporal.PlainTime> => textCodec(PLAIN_TIME),
  decimal: (): TextCodec<Decimal> => textCodec(DECIMAL),
  wire: <W extends RequiredWire>(wire: W): WireSchema<W> => wireSchema(wire),
});
