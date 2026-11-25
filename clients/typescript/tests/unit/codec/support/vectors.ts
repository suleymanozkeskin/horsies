// The stored-form vectors of `core/tests/fixtures/stored_forms/` and the declared wire type of each vector type
// (the notation is in the README of that folder).

import { readFileSync } from "node:fs";

import {
  isInstant,
  isPlainDate,
  isPlainDateTime,
  isZonedDateTime,
} from "../../../../horsies/src/codec/temporal-text.ts";
import {
  isArray,
  isBigint,
  isNumber,
  isObjectLike,
  isPlainObject,
  isString,
} from "../../../../horsies/src/codec/values.ts";
import { h } from "../../../../horsies/src/wire/builders.ts";
import { Decimal } from "../../../../horsies/src/wire/decimal.ts";
import type { AnyWire } from "../../../../horsies/src/wire/infer.ts";
import { STORED_FORMS_DIR } from "./files.ts";

/** A value in the notation of the vector files: an integer is decimal text, a float is float text. */
export type Notation = null | boolean | number | string | readonly Notation[] | { readonly [key: string]: Notation };

export type Parameter = { readonly name: string; readonly type: string; readonly value?: Notation };

export type Drift = { readonly text: string; readonly finding: string };

export type InputVector = {
  readonly name: string;
  readonly parameters: readonly Parameter[];
  readonly text: string;
  readonly typescript_cannot_express?: string;
  readonly typescript_drift?: Drift;
};

export type RefusedValue = { readonly name: string; readonly parameters: readonly Parameter[] };

export type RefusedText = { readonly name: string; readonly text: string };

export type TypedRefusedText = {
  readonly name: string;
  readonly parameters: readonly Parameter[];
  readonly text: string;
};

export type TaskInputFile = {
  readonly vectors: readonly InputVector[];
  readonly refused_values: readonly RefusedValue[];
  readonly refused_texts: readonly RefusedText[];
  readonly typed_refused_texts: readonly TypedRefusedText[];
};

export type ErrorCodeNotation = { readonly built_in: string } | { readonly user: string };

export type ErrorNotation = {
  readonly error_code: ErrorCodeNotation;
  readonly message: string | null;
  readonly data: Notation;
  readonly exception: { readonly [key: string]: string } | null;
  readonly sub_workflow?: {
    readonly id: string;
    readonly summary: { readonly [key: string]: Notation };
  };
};

export type EnvelopeVector = {
  readonly name: string;
  readonly ok?: { readonly type: string; readonly value: Notation };
  readonly err?: ErrorNotation;
  readonly text: string;
  readonly stored_data?: Notation;
  readonly typescript_cannot_express?: string;
};

export type ResultEnvelopeFile = {
  readonly vectors: readonly EnvelopeVector[];
  readonly refused_texts: readonly { readonly name: string; readonly text: string; readonly refusal: string }[];
};

export type ErrorDataFile = {
  readonly vectors: readonly {
    readonly name: string;
    readonly stored_as: "envelope" | "error";
    readonly error: ErrorNotation;
    readonly text: string;
  }[];
};

/** `result_envelope.json`. */
export function resultEnvelopeFile(): ResultEnvelopeFile {
  return JSON.parse(readFileSync(`${STORED_FORMS_DIR}result_envelope.json`, "utf8"));
}

/** `error_data.json`. */
export function errorDataFile(): ErrorDataFile {
  return JSON.parse(readFileSync(`${STORED_FORMS_DIR}error_data.json`, "utf8"));
}

/** `task_input.json`. */
export function taskInputFile(): TaskInputFile {
  return JSON.parse(readFileSync(`${STORED_FORMS_DIR}task_input.json`, "utf8"));
}

/** A wire type that a TypeScript user declares for a vector type, with a label for the test name. */
export type Declared = { readonly label: string; readonly wire: AnyWire };

const SAFE_INTEGER_MAX = BigInt(Number.MAX_SAFE_INTEGER);

function isSafeIntegerText(notation: Notation | undefined): boolean {
  if (!isString(notation)) {
    return true;
  }

  const value = BigInt(notation);

  return value <= SAFE_INTEGER_MAX && value >= -SAFE_INTEGER_MAX;
}

/**
 * The wire types that a TypeScript user declares for a vector type (the wire-type table of the task model). An
 * integer in the safe range has two: `h.int()` and `h.bigint()` store the same text. An empty list: the type has no
 * wire type in this package yet.
 */
export function declaredWires(type: string, notation: Notation | undefined): readonly Declared[] {
  switch (type) {
    case "none":
      return [{ label: "null", wire: h.null() }];
    case "bool":
      return [{ label: "boolean", wire: h.boolean() }];
    case "i64":
      return isSafeIntegerText(notation)
        ? [
            { label: "int", wire: h.int() },
            { label: "bigint", wire: h.bigint() },
          ]
        : [{ label: "bigint", wire: h.bigint() }];
    case "u64":
    case "i128":
    case "u128":
    case "big_int":
      return [{ label: "bigint", wire: h.bigint() }];
    case "f64":
      return [{ label: "float", wire: h.float() }];
    case "str":
      return [{ label: "string", wire: h.string() }];
    case "uuid":
      return [{ label: "uuid", wire: h.uuid() }];
    case "decimal":
      return [{ label: "decimal", wire: h.decimal() }];
    case "datetime_utc":
      return [{ label: "instant", wire: h.instant() }];
    case "datetime_offset":
      return [{ label: "offsetDateTime", wire: h.offsetDateTime() }];
    case "datetime_naive":
      return [{ label: "plainDateTime", wire: h.plainDateTime() }];
    case "date":
      return [{ label: "plainDate", wire: h.plainDate() }];
    case "str_enum":
      return [{ label: "enum", wire: h.enum(["red", "green"]) }];
    case "int_enum":
      return [{ label: "literal", wire: h.literal(1, 3) }];
    case "optional_i64":
      return [{ label: "int.nullable", wire: h.int().nullable() }];
    case "list_i64":
      return [{ label: "array(int)", wire: h.array(h.int()) }];
    case "map_str_i64":
      return [{ label: "record(int)", wire: h.record(h.int()) }];
    case "record_order":
      return [{ label: "object", wire: h.object({ id: h.int(), amount: h.float() }) }];
    case "json":
      return [{ label: "json", wire: h.json() }];
    default:
      return [];
  }
}

function notationText(notation: Notation | undefined): string {
  if (!isString(notation)) {
    throw new TypeError(`expected a notation text, got ${JSON.stringify(notation)}`);
  }

  return notation;
}

function floatOf(notation: Notation | undefined): number {
  const text = notationText(notation);

  switch (text) {
    case "nan":
      return Number.NaN;
    case "inf":
      return Number.POSITIVE_INFINITY;
    case "-inf":
      return Number.NEGATIVE_INFINITY;
    default:
      return Number(text);
  }
}

/** The TypeScript value of a vector notation under the declared wire type. */
export function valueOf(type: string, declared: Declared, notation: Notation | undefined): unknown {
  switch (type) {
    case "none":
      return null;
    case "bool":
    case "str":
    case "uuid":
    case "str_enum":
    case "json":
      return notation;
    case "int_enum":
      return Number(notationText(notation));
    case "decimal":
      return decimalOf(notationText(notation));
    case "i64":
    case "u64":
    case "i128":
    case "u128":
    case "big_int":
      return declared.label === "int" ? Number(notationText(notation)) : BigInt(notationText(notation));
    case "f64":
      return floatOf(notation);
    case "datetime_utc":
      return Temporal.Instant.from(notationText(notation));
    case "datetime_offset":
      return zonedOf(notationText(notation));
    case "datetime_naive":
      return Temporal.PlainDateTime.from(notationText(notation));
    case "date":
      return Temporal.PlainDate.from(notationText(notation));
    case "optional_i64":
      return notation === null ? null : Number(notationText(notation));
    case "list_i64":
      return notationList(notation).map((item) => Number(notationText(item)));
    case "map_str_i64":
      return mapOf(notationObject(notation), (item) => Number(notationText(item)));
    case "record_order": {
      const order = notationObject(notation);

      return { id: Number(notationText(order["id"])), amount: Number(notationText(order["amount"])) };
    }

    default:
      throw new TypeError(`no value for the vector type ${type}`);
  }
}

function decimalOf(text: string): Decimal {
  const parsed = Decimal.parse(text);

  if (!parsed.ok) {
    throw new TypeError(`the vector decimal ${text} does not parse: ${parsed.error.kind}`);
  }

  return parsed.value;
}

/** A date-time text with a `±HH:MM` offset, as a value whose zone is that fixed offset. */
function zonedOf(text: string): Temporal.ZonedDateTime {
  return Temporal.ZonedDateTime.from(`${text}[${text.slice(-6)}]`);
}

function notationList(notation: Notation | undefined): readonly Notation[] {
  if (!isArray(notation)) {
    throw new TypeError(`expected a notation list, got ${JSON.stringify(notation)}`);
  }

  return notation;
}

function notationObject(notation: Notation | undefined): { readonly [key: string]: Notation } {
  if (!isPlainObject(notation)) {
    throw new TypeError(`expected a notation object, got ${JSON.stringify(notation)}`);
  }

  return notation;
}

/** A new object with the same keys, in the same order, and each value mapped. */
function mapOf<T>(entries: { readonly [key: string]: Notation }, map: (item: Notation) => T): Record<string, T> {
  const out: Record<string, T> = {};

  for (const [key, item] of Object.entries(entries)) {
    out[key] = map(item);
  }

  return out;
}

/** One case per choice of declared wire types of a parameter list; empty when a type has no wire type yet. */
export type ParameterCase = {
  readonly label: string;
  readonly parameters: readonly { readonly name: string; readonly wire: AnyWire }[];
  readonly value: Readonly<Record<string, unknown>>;
};

export function parameterCases(parameters: readonly Parameter[]): readonly ParameterCase[] {
  const choices = parameters.map((parameter) => declaredWires(parameter.type, parameter.value));

  if (choices.some((choice) => choice.length === 0)) {
    return [];
  }

  const caseCount = Math.max(1, ...choices.map((choice) => choice.length));
  const cases: ParameterCase[] = [];

  for (let index = 0; index < caseCount; index += 1) {
    const value: Record<string, unknown> = {};

    const declaredList = parameters.map((parameter, at) => {
      const choice = choices[at] ?? [];
      const declared = choice[Math.min(index, choice.length - 1)];

      if (declared === undefined) {
        throw new TypeError(`parameter ${parameter.name} has no declared wire type`);
      }

      if (Object.hasOwn(parameter, "value")) {
        value[parameter.name] = valueOf(parameter.type, declared, parameter.value);
      }

      return { name: parameter.name, declared };
    });

    cases.push({
      label: declaredList.map((entry) => entry.declared.label).join(",") || "no parameters",
      parameters: declaredList.map((entry) => ({ name: entry.name, wire: entry.declared.wire })),
      value,
    });
  }

  return cases;
}

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
