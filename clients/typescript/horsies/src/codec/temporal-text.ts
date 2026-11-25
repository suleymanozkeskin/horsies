// Python's date-time texts for the Temporal types (Pydantic `mode="json"`; the core's `python_text.rs`; the Rust
// client's `StoredDateTime`), and their readers.
//
// The fraction of a second: none for a whole second, 6 digits for a whole microsecond, 9 digits for a part below the
// microsecond (the Rust rule). A value that Python can hold gets Python's text.

import { err, ok, type Result } from "../wire/result.ts";

/** Python's `datetime` year range. */
const YEAR_MIN = 1;
const YEAR_MAX = 9999;

const ISO_CALENDAR = "iso8601";

/** A fixed-offset time zone id as Temporal writes it: `±HH:MM`. */
const OFFSET_ZONE_ID = /^[+-]\d{2}:\d{2}$/;

const UTC_OFFSET = "+00:00";
const UTC_DESIGNATOR = "Z";

const DATE_PART = String.raw`(\d{4})-(\d{2})-(\d{2})`;
const TIME_PART = String.raw`(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?`;
const DATE_TEXT = new RegExp(`^${DATE_PART}$`);
const NAIVE_TEXT = new RegExp(`^${DATE_PART}T${TIME_PART}$`);
const OFFSET_TEXT = new RegExp(`^${DATE_PART}T${TIME_PART}(Z|[+-]\\d{2}:\\d{2})$`);

/** The match group of the first time field in `NAIVE_TEXT` and `OFFSET_TEXT`. */
const TIME_GROUP = 4;
const OFFSET_GROUP = 8;

const NANOSECONDS_PER_SECOND_DIGITS = 9;
const MICROSECOND_DIGITS = 6;

/** Why a Temporal value has no stored text. */
export type TemporalWriteRefused = {
  readonly kind: "YearOutOfRange" | "NamedTimeZone" | "NotIsoCalendar";
  readonly detail: string;
};

/** A stored text that is not the text of its Temporal kind. */
export type TemporalReadRefused = { readonly kind: "BadTemporalText"; readonly detail: string };

type ClockFields = {
  readonly hour: number;
  readonly minute: number;
  readonly second: number;
  readonly millisecond: number;
  readonly microsecond: number;
  readonly nanosecond: number;
};

type DateFields = { readonly year: number; readonly month: number; readonly day: number };

const YEAR_REFUSED: TemporalWriteRefused = Object.freeze({
  kind: "YearOutOfRange",
  detail: `a year outside ${YEAR_MIN} to ${YEAR_MAX}, Python's datetime range`,
});

const CALENDAR_REFUSED: TemporalWriteRefused = Object.freeze({
  kind: "NotIsoCalendar",
  detail: "a calendar other than iso8601",
});

const ZONE_REFUSED: TemporalWriteRefused = Object.freeze({
  kind: "NamedTimeZone",
  detail: "a named time zone; use a fixed offset (withTimeZone(\"+00:00\"))",
});

const READ_REFUSED: TemporalReadRefused = Object.freeze({
  kind: "BadTemporalText",
  detail: "not the stored text of this date-time kind",
});

function pad(value: number, width: number): string {
  return String(value).padStart(width, "0");
}

/** The fraction of a second: none, 6 digits, or 9 digits. Pure. */
function fractionText(clock: ClockFields): string {
  if (clock.nanosecond !== 0) {
    const nanoseconds = (clock.millisecond * 1000 + clock.microsecond) * 1000 + clock.nanosecond;

    return `.${pad(nanoseconds, NANOSECONDS_PER_SECOND_DIGITS)}`;
  }

  const microseconds = clock.millisecond * 1000 + clock.microsecond;

  return microseconds === 0 ? "" : `.${pad(microseconds, MICROSECOND_DIGITS)}`;
}

/** `HH:MM:SS` and the fraction. Pure. */
export function clockText(clock: ClockFields): string {
  return `${pad(clock.hour, 2)}:${pad(clock.minute, 2)}:${pad(clock.second, 2)}${fractionText(clock)}`;
}

function dateText(date: DateFields): Result<string, TemporalWriteRefused> {
  if (date.year < YEAR_MIN || date.year > YEAR_MAX) {
    return err(YEAR_REFUSED);
  }

  return ok(`${pad(date.year, 4)}-${pad(date.month, 2)}-${pad(date.day, 2)}`);
}

/** `YYYY-MM-DD`. Pure. */
export function plainDateText(value: Temporal.PlainDate): Result<string, TemporalWriteRefused> {
  return value.calendarId === ISO_CALENDAR ? dateText(value) : err(CALENDAR_REFUSED);
}

/** `YYYY-MM-DDTHH:MM:SS[.f]`. Pure. */
export function plainDateTimeText(value: Temporal.PlainDateTime): Result<string, TemporalWriteRefused> {
  if (value.calendarId !== ISO_CALENDAR) {
    return err(CALENDAR_REFUSED);
  }

  const date = dateText(value);

  return date.ok ? ok(`${date.value}T${clockText(value)}`) : date;
}

/** An instant as Pydantic writes an aware UTC `datetime`: `YYYY-MM-DDTHH:MM:SS[.f]Z`. Pure. */
export function instantText(value: Temporal.Instant): Result<string, TemporalWriteRefused> {
  const utc = value.toZonedDateTimeISO(UTC_OFFSET);
  const date = dateText(utc);

  return date.ok ? ok(`${date.value}T${clockText(utc)}${UTC_DESIGNATOR}`) : date;
}

/** A date-time with a fixed offset: `±HH:MM`, and `Z` for offset 0. A named time zone is refused. Pure. */
export function offsetDateTimeText(value: Temporal.ZonedDateTime): Result<string, TemporalWriteRefused> {
  if (!OFFSET_ZONE_ID.test(value.timeZoneId)) {
    return err(ZONE_REFUSED);
  }

  if (value.calendarId !== ISO_CALENDAR) {
    return err(CALENDAR_REFUSED);
  }

  const date = dateText(value);
  const offset = value.offset === UTC_OFFSET ? UTC_DESIGNATOR : value.offset;

  return date.ok ? ok(`${date.value}T${clockText(value)}${offset}`) : date;
}

// Readers. Each takes the stored text forms only: the pattern checks the form, and the Temporal constructor with
// `overflow: "reject"` refuses a day 31 of a 30-day month or a second 60.

function fractionFields(fraction: string | undefined): Pick<ClockFields, "millisecond" | "microsecond" | "nanosecond"> {
  const nine = (fraction ?? "").padEnd(NANOSECONDS_PER_SECOND_DIGITS, "0");

  return {
    millisecond: Number(nine.slice(0, 3)),
    microsecond: Number(nine.slice(3, 6)),
    nanosecond: Number(nine.slice(6, 9)),
  };
}

function dateFields(match: RegExpExecArray): DateFields | null {
  const year = Number(match[1]);

  return year < YEAR_MIN ? null : { year, month: Number(match[2]), day: Number(match[3]) };
}

function clockFields(match: RegExpExecArray, first: number): ClockFields {
  return {
    hour: Number(match[first]),
    minute: Number(match[first + 1]),
    second: Number(match[first + 2]),
    ...fractionFields(match[first + 3]),
  };
}

/** Runs a Temporal constructor: its `RangeError` (a field out of range) is a text refusal. */
function built<T>(construct: () => T): Result<T, TemporalReadRefused> {
  try {
    return ok(construct());
  } catch (cause) {
    if (cause instanceof RangeError) {
      return err(READ_REFUSED);
    }

    throw cause;
  }
}

/** A `YYYY-MM-DD` text. Pure. */
export function readPlainDate(text: string): Result<Temporal.PlainDate, TemporalReadRefused> {
  const match = DATE_TEXT.exec(text);
  const date = match === null ? null : dateFields(match);

  return date === null ? err(READ_REFUSED) : built(() => Temporal.PlainDate.from(date, { overflow: "reject" }));
}

/** A date-time text without an offset. Pure. */
export function readPlainDateTime(text: string): Result<Temporal.PlainDateTime, TemporalReadRefused> {
  const match = NAIVE_TEXT.exec(text);
  const date = match === null ? null : dateFields(match);

  if (match === null || date === null) {
    return err(READ_REFUSED);
  }

  const fields = { ...date, ...clockFields(match, TIME_GROUP) };

  return built(() => Temporal.PlainDateTime.from(fields, { overflow: "reject" }));
}

/**
 * A date-time text with `Z` or `±HH:MM`; the zone of the value is that fixed offset. An offset with seconds is
 * refused. Pure.
 */
export function readOffsetDateTime(text: string): Result<Temporal.ZonedDateTime, TemporalReadRefused> {
  const match = OFFSET_TEXT.exec(text);
  const date = match === null ? null : dateFields(match);

  if (match === null || date === null) {
    return err(READ_REFUSED);
  }

  const designator = match[OFFSET_GROUP];
  const offset = designator === undefined || designator === UTC_DESIGNATOR ? UTC_OFFSET : designator;
  const fields = { ...date, ...clockFields(match, TIME_GROUP), timeZone: offset, offset };

  return built(() => Temporal.ZonedDateTime.from(fields, { overflow: "reject", offset: "reject" }));
}

/** A date-time text with an offset, as the instant that it names. Pure. */
export function readInstant(text: string): Result<Temporal.Instant, TemporalReadRefused> {
  const zoned = readOffsetDateTime(text);

  return zoned.ok ? ok(zoned.value.toInstant()) : zoned;
}

/** Whether `value` is a `Temporal.Instant`. Pure. */
export function isInstant(value: unknown): value is Temporal.Instant {
  return value instanceof Temporal.Instant;
}

/** Whether `value` is a `Temporal.ZonedDateTime`. Pure. */
export function isZonedDateTime(value: unknown): value is Temporal.ZonedDateTime {
  return value instanceof Temporal.ZonedDateTime;
}

/** Whether `value` is a `Temporal.PlainDateTime`. Pure. */
export function isPlainDateTime(value: unknown): value is Temporal.PlainDateTime {
  return value instanceof Temporal.PlainDateTime;
}

/** Whether `value` is a `Temporal.PlainDate`. Pure. */
export function isPlainDate(value: unknown): value is Temporal.PlainDate {
  return value instanceof Temporal.PlainDate;
}
