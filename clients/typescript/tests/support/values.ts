// Comparison and display of decoded values, for the codec tests and the stored-form tests.

import {
  isInstant,
  isPlainDate,
  isPlainDateTime,
  isPlainTime,
  isZonedDateTime,
} from "../../horsies/src/codec/temporal-text.ts";
import { isArray, isBigint, isNumber, isObjectLike, isPlainObject } from "../../horsies/src/codec/values.ts";
import { Decimal } from "../../horsies/src/wire/decimal.ts";

/** Strict equality of decoded values: `-0`, `bigint`, key order, and the class of each value. */
export function sameValue<A, E>(actual: A, expected: E): boolean {
  if (isObjectLike(expected) && Decimal.isDecimal(expected)) {
    return isObjectLike(actual) && Decimal.isDecimal(actual) && actual.equals(expected);
  }

  if (isInstant(expected)) {
    return isInstant(actual) && actual.equals(expected);
  }

  if (isZonedDateTime(expected)) {
    return isZonedDateTime(actual) && actual.equals(expected);
  }

  if (isPlainDateTime(expected)) {
    return isPlainDateTime(actual) && actual.equals(expected);
  }

  if (isPlainDate(expected)) {
    return isPlainDate(actual) && actual.equals(expected);
  }

  if (isPlainTime(expected)) {
    return isPlainTime(actual) && actual.equals(expected);
  }

  if (isNumber(expected) || !isArray(expected) && !isPlainObject(expected)) {
    return Object.is(actual, expected);
  }

  if (isArray(expected)) {
    return (
      isArray(actual) &&
      actual.length === expected.length &&
      expected.every((item, index) => sameValue(actual[index], item))
    );
  }

  if (!isPlainObject(actual)) {
    return false;
  }

  const expectedKeys = Object.keys(expected);
  const actualKeys = Object.keys(actual);

  return (
    expectedKeys.length === actualKeys.length &&
    expectedKeys.every((key, index) => actualKeys[index] === key && sameValue(actual[key], expected[key]))
  );
}

/** The text of a value for a failure message; a `bigint` is written with `n`. */
export function show<V>(value: V): string {
  return JSON.stringify(value, (_key, item: bigint | object | string | number | boolean | null) =>
    isBigint(item) ? `${item}n` : item,
  );
}
