// Python's `str(Decimal(text))` of a finite decimal text, with the accept set of the Python client: Pydantic reads
// a decimal text through `decimal.Decimal`, so a text that `Decimal` refuses, or an infinity or a NaN (Pydantic's
// `finite_number` rule), is refused here too.
//
// What `Decimal(text)` takes (CPython 3.13, the C `decimal` module with libmpdec, 64-bit):
// - whitespace (`str.isspace`) at the start and at the end is removed; whitespace inside is refused;
// - every `_` is removed, at any place;
// - each Unicode decimal digit counts as its ASCII digit; any other character outside ASCII is refused;
// - then the grammar: a sign, digits with an optional point, an optional exponent (`e` or `E`, a sign, digits);
// - the exponent is bounded: the exponent of the last digit is at least `MIN_EXPONENT`, and the exponent of the
//   first digit (the adjusted exponent) is at most `MAX_ADJUSTED_EXPONENT`.
// The text form is to-scientific-string of the General Decimal Arithmetic specification.

import { err, ok, type Result } from "../wire/result.ts";

/** The grammar of the ASCII form: a sign, digits with an optional point, an optional exponent. */
const DECIMAL_TEXT = /^([+-])?(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:[eE]([+-])?(\d+))?$/;

/** The special values of the grammar: infinities and NaNs (with a payload), any case. */
const SPECIAL_TEXT = /^[+-]?(?:inf(?:inity)?|s?nan\d*)$/i;

/** The highest adjusted exponent (libmpdec `MPD_MAX_EMAX`, 64-bit). */
const MAX_ADJUSTED_EXPONENT = 999_999_999_999_999_999n;

/** The lowest exponent of the last digit (libmpdec `MPD_MIN_ETINY`, 64-bit). */
const MIN_EXPONENT = -1_999_999_999_999_999_997n;

/** An exponent with more digits (without leading zeros) is outside both bounds. */
const MAX_EXPONENT_DIGITS = 19;

/** Plain notation is used while the adjusted exponent is at least this (to-scientific-string). */
const PLAIN_ADJUSTED_MIN = -6n;

/** The characters for which Python's `str.isspace()` is true (Python 3.13, Unicode 15.1). */
const PYTHON_SPACES: ReadonlySet<number> = new Set([
  0x9, 0xa, 0xb, 0xc, 0xd, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x85, 0xa0, 0x1680, 0x2000, 0x2001, 0x2002, 0x2003, 0x2004,
  0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000,
]);

/**
 * The digit zero of each run of Unicode decimal digits (`unicodedata.decimal`, Python 3.13, Unicode 15.1). Each run
 * holds the digits 0 to 9 at the next 10 code points. Sorted.
 */
const PYTHON_DIGIT_ZEROS: readonly number[] = [
  0x30, 0x660, 0x6f0, 0x7c0, 0x966, 0x9e6, 0xa66, 0xae6, 0xb66, 0xbe6, 0xc66, 0xce6, 0xd66, 0xde6, 0xe50, 0xed0,
  0xf20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620,
  0xa8d0, 0xa900, 0xa9d0, 0xa9f0, 0xaa50, 0xabf0, 0xff10, 0x104a0, 0x10d30, 0x11066, 0x110f0, 0x11136, 0x111d0,
  0x112f0, 0x11450, 0x114d0, 0x11650, 0x116c0, 0x11730, 0x118e0, 0x11950, 0x11c50, 0x11d50, 0x11da0, 0x11f50, 0x16a60,
  0x16ac0, 0x16b50, 0x1d7ce, 0x1d7d8, 0x1d7e2, 0x1d7ec, 0x1d7f6, 0x1e140, 0x1e2f0, 0x1e4f0, 0x1e950, 0x1fbf0,
];

const DIGITS_PER_RUN = 10;
const UNDERSCORE = 0x5f;
const ASCII_MAX = 0x7f;
const ZERO_CODE = 48;

/** Why a text is not a finite decimal. */
export type DecimalRefused =
  /** The text is not a decimal text of Python's `Decimal`, or its exponent is outside the bounds. */
  | { readonly kind: "NotADecimal"; readonly detail: string }
  /** The text is an infinity or a NaN, which a stored decimal cannot hold (Pydantic refuses them). */
  | { readonly kind: "NotFinite"; readonly detail: string };

const NOT_A_DECIMAL: DecimalRefused = Object.freeze({
  kind: "NotADecimal",
  detail: "not a decimal text: a sign, digits with an optional point, an optional exponent",
});

const EXPONENT_OUT_OF_RANGE: DecimalRefused = Object.freeze({
  kind: "NotADecimal",
  detail: "the exponent is outside the range of Python's decimal",
});

const NOT_FINITE: DecimalRefused = Object.freeze({
  kind: "NotFinite",
  detail: "an infinity or a NaN; a stored decimal is finite",
});

/** Python's `str(Decimal(text))` of a finite decimal text: `1e3` is `1E+3`, `1_000` is `1000`. Pure. */
export function canonicalDecimalText(text: string): Result<string, DecimalRefused> {
  const ascii = asciiForm(text);

  if (!ascii.ok) {
    return ascii;
  }

  const match = DECIMAL_TEXT.exec(ascii.value);

  if (match === null) {
    return err(SPECIAL_TEXT.test(ascii.value) ? NOT_FINITE : NOT_A_DECIMAL);
  }

  const fractionPart = match[3] ?? match[4] ?? "";
  const coefficient = withoutLeadingZeros(`${match[2] ?? ""}${fractionPart}`);
  const exponent = exponentOf(match[5] ?? "+", match[6] ?? "0", fractionPart.length, coefficient.length);

  return exponent.ok ? ok(`${match[1] === "-" ? "-" : ""}${scientificText(coefficient, exponent.value)}`) : exponent;
}

/**
 * The ASCII form that Python's `Decimal` parses: whitespace removed at both ends, each `_` removed, each Unicode
 * decimal digit as its ASCII digit. A character outside ASCII that is no digit, and a NUL, are refused.
 */
function asciiForm(text: string): Result<string, DecimalRefused> {
  const codePoints = Array.from(text, (character) => character.codePointAt(0) ?? 0);
  let start = 0;
  let end = codePoints.length;

  while (start < end && PYTHON_SPACES.has(codePoints[start] ?? 0)) {
    start += 1;
  }

  while (end > start && PYTHON_SPACES.has(codePoints[end - 1] ?? 0)) {
    end -= 1;
  }

  let ascii = "";

  for (const codePoint of codePoints.slice(start, end)) {
    const character = asciiText(codePoint);

    if (!character.ok) {
      return character;
    }

    ascii += character.value;
  }

  return ok(ascii);
}

/** The ASCII text of one code point of a decimal text: empty for `_`, a space for inner whitespace. */
function asciiText(codePoint: number): Result<string, DecimalRefused> {
  if (codePoint === UNDERSCORE) {
    return ok("");
  }

  if (codePoint > 0 && codePoint <= ASCII_MAX) {
    return ok(String.fromCodePoint(codePoint));
  }

  if (PYTHON_SPACES.has(codePoint)) {
    return ok(" ");
  }

  const digit = unicodeDigit(codePoint);

  return digit.ok ? ok(String.fromCodePoint(ZERO_CODE + digit.value)) : digit;
}

/** The value of a Unicode decimal digit; another code point is not a decimal character. */
function unicodeDigit(codePoint: number): Result<number, DecimalRefused> {
  let low = 0;
  let high = PYTHON_DIGIT_ZEROS.length - 1;

  while (low <= high) {
    const middle = (low + high) >> 1;
    const zero = PYTHON_DIGIT_ZEROS[middle] ?? 0;

    if (codePoint < zero) {
      high = middle - 1;
    } else if (codePoint >= zero + DIGITS_PER_RUN) {
      low = middle + 1;
    } else {
      return ok(codePoint - zero);
    }
  }

  return err(NOT_A_DECIMAL);
}

/**
 * The exponent of the last digit, inside Python's bounds: at least `MIN_EXPONENT`, and the adjusted exponent (of the
 * first of `coefficientDigits` digits) at most `MAX_ADJUSTED_EXPONENT`.
 */
function exponentOf(
  sign: string,
  digits: string,
  fractionDigits: number,
  coefficientDigits: number,
): Result<bigint, DecimalRefused> {
  const significant = withoutLeadingZeros(digits);

  if (significant.length > MAX_EXPONENT_DIGITS) {
    return err(EXPONENT_OUT_OF_RANGE);
  }

  const exponent = BigInt(`${sign}${significant}`) - BigInt(fractionDigits);
  const adjusted = exponent + BigInt(coefficientDigits - 1);

  return exponent < MIN_EXPONENT || adjusted > MAX_ADJUSTED_EXPONENT ? err(EXPONENT_OUT_OF_RANGE) : ok(exponent);
}

function withoutLeadingZeros(digits: string): string {
  let first = 0;

  while (first < digits.length - 1 && digits.charCodeAt(first) === ZERO_CODE) {
    first += 1;
  }

  return digits.slice(first);
}

/** The to-scientific-string text of `coefficient` times ten to `exponent`, without the sign. Pure. */
function scientificText(coefficient: string, exponent: bigint): string {
  const adjusted = exponent + BigInt(coefficient.length - 1);

  if (exponent <= 0n && adjusted >= PLAIN_ADJUSTED_MIN) {
    if (exponent === 0n) {
      return coefficient;
    }

    const pointAt = coefficient.length + Number(exponent);

    return pointAt > 0
      ? `${coefficient.slice(0, pointAt)}.${coefficient.slice(pointAt)}`
      : `0.${"0".repeat(-pointAt)}${coefficient}`;
  }

  const head = coefficient.length > 1 ? `${coefficient.slice(0, 1)}.${coefficient.slice(1)}` : coefficient;
  const exponentSign = adjusted < 0n ? "-" : "+";
  const magnitude = adjusted < 0n ? -adjusted : adjusted;

  return `${head}E${exponentSign}${magnitude.toString()}`;
}

/** Python's whitespace and digit tables of this module, for the test that compares them with Python's own. */
export const PYTHON_DECIMAL_TABLES: { readonly spaces: readonly number[]; readonly digitZeros: readonly number[] } =
  Object.freeze({ spaces: Object.freeze([...PYTHON_SPACES]), digitZeros: PYTHON_DIGIT_ZEROS });
