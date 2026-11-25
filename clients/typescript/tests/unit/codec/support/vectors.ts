// The stored-form vectors of `core/tests/fixtures/stored_forms/` and the declared wire type of each vector type
// (the notation is in the README of that folder).

import { readFileSync } from "node:fs";

import { isBigint, isString } from "../../../../horsies/src/codec/values.ts";
import { h } from "../../../../horsies/src/wire/builders.ts";
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
 * The wire types that a TypeScript user declares for a vector type. An
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
      return notation;
    case "i64":
    case "u64":
    case "i128":
    case "u128":
    case "big_int":
      return declared.label === "int" ? Number(notationText(notation)) : BigInt(notationText(notation));
    case "f64":
      return floatOf(notation);
    default:
      throw new TypeError(`no value for the vector type ${type}`);
  }
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

/** Strict equality of decoded values: `-0`, `bigint`, and the class of each value. */
export function sameValue<A, E>(actual: A, expected: E): boolean {
  return Object.is(actual, expected);
}

/** The text of a value for a failure message; a `bigint` is written with `n`. */
export function show<V>(value: V): string {
  return JSON.stringify(value, (_key, item: bigint | object | string | number | boolean | null) =>
    isBigint(item) ? `${item}n` : item,
  );
}
