// Python's `str(Decimal(text))` of a finite decimal text: the to-scientific-string form of the General Decimal
// Arithmetic specification, which Python's `decimal` module follows.

import { err, ok, type Result } from "../wire/result.ts";

/** Python's decimal string grammar, ASCII form: a sign, digits with an optional point, an optional exponent. */
const DECIMAL_TEXT = /^([+-])?(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:[eE]([+-]?\d+))?$/;

/** The special values of Python's grammar: infinities and NaNs (with a payload), any case. */
const SPECIAL_TEXT = /^[+-]?(?:inf(?:inity)?|s?nan\d*)$/i;

/** Plain notation is used while the adjusted exponent is at least this (to-scientific-string). */
const PLAIN_ADJUSTED_MIN = -6n;

const ZERO_CODE = 48;

/** Why a text is not a finite decimal. */
export type DecimalRefused =
  /** The text is not in Python's decimal grammar. */
  | { readonly kind: "NotADecimal"; readonly detail: string }
  /** The text is an infinity or a NaN, which a stored decimal cannot hold (Pydantic refuses them). */
  | { readonly kind: "NotFinite"; readonly detail: string };

const NOT_A_DECIMAL: DecimalRefused = Object.freeze({
  kind: "NotADecimal",
  detail: "not a decimal text: a sign, digits with an optional point, an optional exponent",
});

const NOT_FINITE: DecimalRefused = Object.freeze({
  kind: "NotFinite",
  detail: "an infinity or a NaN; a stored decimal is finite",
});

/**
 * Python's `str(Decimal(text))` of a finite decimal text: `1e3` is `1E+3`, `0.0000001` is `1E-7`, `1.10` stays
 * `1.10`, `-0` stays `-0`. Pure.
 */
export function canonicalDecimalText(text: string): Result<string, DecimalRefused> {
  const match = DECIMAL_TEXT.exec(text);

  if (match === null) {
    return err(SPECIAL_TEXT.test(text) ? NOT_FINITE : NOT_A_DECIMAL);
  }

  const sign = match[1] === "-" ? "-" : "";
  const integerPart = match[2] ?? "";
  const fractionPart = match[3] ?? match[4] ?? "";
  const coefficient = withoutLeadingZeros(integerPart + fractionPart);
  const exponent = BigInt(match[5] ?? "0") - BigInt(fractionPart.length);

  return ok(sign + scientificText(coefficient, exponent));
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
