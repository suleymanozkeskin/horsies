// The stored text of the scalar values that more than one writer writes: strings, integers of any size, floats.

import { err, ok, type Result } from "../wire/result.ts";
import { MAX_INT_DIGITS } from "./bounds.ts";
import { pythonFloatText } from "./float-text.ts";

/** Why a scalar value has no stored text. */
export type ScalarRefused = {
  readonly kind: "LoneSurrogate" | "IntegerTooLong" | "NonFiniteFloat";
  readonly detail: string;
};

const LONE_SURROGATE: ScalarRefused = Object.freeze({
  kind: "LoneSurrogate",
  detail: "a string holds a lone surrogate",
});

const INTEGER_TOO_LONG: ScalarRefused = Object.freeze({
  kind: "IntegerTooLong",
  detail: `more than ${MAX_INT_DIGITS} digits`,
});

const NON_FINITE_FLOAT: ScalarRefused = Object.freeze({
  kind: "NonFiniteFloat",
  detail: "NaN and the infinities are not stored",
});

/**
 * A well-formed string as `JSON.stringify` writes it, which is Python's text: `"`, `\` and U+0000 to U+001F are
 * escaped (`\b`, `\f`, `\n`, `\r`, `\t`, else `\u00xx`); other characters are written as they are. Pure.
 */
export function stringText(value: string): Result<string, ScalarRefused> {
  return value.isWellFormed() ? ok(JSON.stringify(value)) : err(LONE_SURROGATE);
}

/** The decimal digits of an integer of at most `MAX_INT_DIGITS` digits. Pure. */
export function bigintText(value: bigint): Result<string, ScalarRefused> {
  const text = value.toString();
  const digits = value < 0n ? text.length - 1 : text.length;

  return digits > MAX_INT_DIGITS ? err(INTEGER_TOO_LONG) : ok(text);
}

/** Python's float text of a finite number. Pure. */
export function floatText(value: number): Result<string, ScalarRefused> {
  return Number.isFinite(value) ? ok(pythonFloatText(value)) : err(NON_FINITE_FLOAT);
}
