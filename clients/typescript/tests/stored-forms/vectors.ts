// The loader of the core's stored-form vectors (`core/tests/fixtures/stored_forms/`; the notation is in the README of
// that folder): it reads a file, checks that every section and row holds only the keys that these tests read, and
// maps each vector type to the wire type that a TypeScript user declares.
//
// The stored-form vectors are read in this folder only: each vector rule of this client has one test.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { BUILT_IN_TASK_CODES } from "../../horsies/src/_catalog/task_code.ts";
import { WORKFLOW_STATUSES } from "../../horsies/src/wire/task-error.ts";
import { isArray, isPlainObject, isString } from "../../horsies/src/codec/values.ts";
import { h } from "../../horsies/src/wire/builders.ts";
import { Decimal } from "../../horsies/src/wire/decimal.ts";
import type { RequiredWire } from "../../horsies/src/wire/infer.ts";
import type {
  ExceptionRecord,
  BuiltInTaskCode,
  TaskError,
  WorkflowStatus,
} from "../../horsies/src/wire/task-error.ts";
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

/** A text that the core's strict check refuses: `core_refusal` names its variant; Python's words, if any. */
export type RefusedText = {
  readonly name: string;
  readonly text: string;
  readonly core_refusal: string;
  readonly refusal_text?: string;
};

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

/** How a reader refuses a refused envelope text: the envelope's form, or the decode of its `ok` slot. */
export type EnvelopeRefusal = "envelope" | "ok value";

export type ResultEnvelopeFile = {
  readonly vectors: readonly EnvelopeVector[];
  readonly refused_texts: readonly {
    readonly name: string;
    readonly text: string;
    readonly refusal: EnvelopeRefusal;
  }[];
};

export type ErrorDataFile = {
  readonly vectors: readonly {
    readonly name: string;
    readonly stored_as: "envelope" | "error";
    readonly error: ErrorNotation;
    readonly text: string;
  }[];
};

export type SpecDataFile = {
  readonly vectors: readonly { readonly name: string; readonly document: Notation; readonly text: string }[];
  readonly refused_texts: readonly { readonly name: string; readonly text: string; readonly core_field_path: string }[];
};

/** The vector files of the forms that this client encodes or decodes itself. */
export type VectorFileName = "task_input.json" | "result_envelope.json" | "error_data.json" | "workflow_spec_data.json";

/** The vector files of the forms that only the core writes: the task options and the trace carrier. */
export const CORE_ONLY_FILES: readonly string[] = ["task_options.json", "trace_carrier.json"];

/**
 * The keys that a row may hold, and the keys of each member that holds an object or a list of objects. A member that
 * holds a value notation, or facts that these tests do not read (`inputs` of the error data writers), is not listed
 * in `members`. The marks of the Rust client (`rust_cannot_express`, `rust_drift`) are keys that these tests skip.
 */
type RowKeys = {
  readonly keys: readonly string[];
  readonly members: Readonly<Record<string, RowKeys>>;
};

type FileLayout = {
  readonly form: string;
  readonly sections: Readonly<Record<string, RowKeys>>;
};

function rowKeys(keys: readonly string[], members: Readonly<Record<string, RowKeys>> = {}): RowKeys {
  return { keys, members };
}

const PARAMETER = rowKeys(["name", "type", "value"]);
const DRIFT = rowKeys(["text", "finding"]);
const ERROR_CODE = rowKeys(["built_in", "user"]);

const ERROR = rowKeys(["error_code", "message", "data", "exception", "sub_workflow"], {
  error_code: ERROR_CODE,
  sub_workflow: rowKeys(["id", "summary"]),
});

const FILE_LAYOUTS: Readonly<Record<VectorFileName, FileLayout>> = {
  "task_input.json": {
    form: "task input",
    sections: {
      vectors: rowKeys(
        ["name", "parameters", "text", "rust_cannot_express", "rust_drift", "typescript_cannot_express", "typescript_drift"],
        { parameters: PARAMETER, rust_drift: DRIFT, typescript_drift: DRIFT },
      ),
      refused_values: rowKeys(["name", "parameters"], { parameters: PARAMETER }),
      refused_texts: rowKeys(["name", "text", "core_refusal", "refusal_text"]),
      typed_refused_texts: rowKeys(["name", "parameters", "text"], { parameters: PARAMETER }),
    },
  },
  "result_envelope.json": {
    form: "result envelope",
    sections: {
      vectors: rowKeys(
        ["name", "ok", "err", "text", "stored_data", "rust_cannot_express", "rust_drift", "typescript_cannot_express"],
        { ok: rowKeys(["type", "value"]), err: ERROR, rust_drift: DRIFT },
      ),
      refused_texts: rowKeys(["name", "text", "refusal"]),
    },
  },
  "error_data.json": {
    form: "error data",
    sections: {
      vectors: rowKeys(["name", "stored_as", "error", "text", "python_writer", "core_writer", "inputs"], {
        error: ERROR,
      }),
    },
  },
  "workflow_spec_data.json": {
    form: "workflow spec data",
    sections: {
      vectors: rowKeys(["name", "document", "text"]),
      refused_texts: rowKeys(["name", "text", "core_field_path"]),
    },
  },
};

/** The problems of one object against its keys: each key that the tests do not read, and each nested problem. */
function objectProblems(where: string, value: Notation, keys: RowKeys): readonly string[] {
  if (!isPlainObject(value)) {
    return [`${where}: not an object`];
  }

  const unknownKeys = Object.keys(value)
    .filter((key) => !keys.keys.includes(key))
    .map((key) => `${where}: the key ${key} is not read by these tests`);

  const nested = Object.entries(keys.members).flatMap(([member, memberKeys]) => {
    const item = value[member];

    if (item === undefined) {
      return [];
    }

    if (isArray(item)) {
      return item.flatMap((entry, index) => objectProblems(`${where}.${member}[${index}]`, entry, memberKeys));
    }

    return objectProblems(`${where}.${member}`, item, memberKeys);
  });

  return [...unknownKeys, ...nested];
}

function rowName(row: Notation, index: number): string {
  return isPlainObject(row) && isString(row["name"]) ? row["name"] : `row ${index}`;
}

/**
 * The problems of a vector file against the keys that these tests read: an unknown or missing section, another form
 * name, a row without a name, a key that these tests do not read. An empty list means every row is read.
 */
export function layoutProblems(fileName: VectorFileName, document: Notation): readonly string[] {
  const layout = FILE_LAYOUTS[fileName];

  if (!isPlainObject(document)) {
    return [`${fileName}: not an object`];
  }

  const form = document["form"] === layout.form ? [] : [`${fileName}: the form is not "${layout.form}"`];

  const unknownSections = Object.keys(document)
    .filter((key) => key !== "form" && !Object.hasOwn(layout.sections, key))
    .map((key) => `${fileName}: the section ${key} is not read by these tests`);

  const rows = Object.entries(layout.sections).flatMap(([section, keys]) => {
    const list = document[section];

    if (!isArray(list)) {
      return [`${fileName}: the section ${section} is missing or not a list`];
    }

    return list.flatMap((row, index) => {
      const where = `${fileName} ${section} ${rowName(row, index)}`;
      const named = isPlainObject(row) && isString(row["name"]) ? [] : [`${where}: no name`];

      return [...named, ...objectProblems(where, row, keys)];
    });
  });

  return [...form, ...unknownSections, ...rows];
}

/** The parsed text of a vector file. */
export function vectorDocument(fileName: VectorFileName): Notation {
  return JSON.parse(readFileSync(`${STORED_FORMS_DIR}${fileName}`, "utf8"));
}

/** A vector file whose rows hold only the keys that these tests read; any other file is a failure of the caller. */
function checkedDocument(fileName: VectorFileName): Notation {
  const document = vectorDocument(fileName);
  const problems = layoutProblems(fileName, document);

  if (problems.length > 0) {
    throw new TypeError(`${fileName} has rows that these tests do not read: ${problems.join("; ")}`);
  }

  return document;
}

/** `task_input.json`. */
export function taskInputFile(): TaskInputFile {
  // SAFETY: checkedDocument checked each section and each row key against the keys of TaskInputFile.
  return checkedDocument("task_input.json") as TaskInputFile;
}

/** `result_envelope.json`. */
export function resultEnvelopeFile(): ResultEnvelopeFile {
  // SAFETY: checkedDocument checked each section and each row key against the keys of ResultEnvelopeFile.
  return checkedDocument("result_envelope.json") as ResultEnvelopeFile;
}

/** `error_data.json`. */
export function errorDataFile(): ErrorDataFile {
  // SAFETY: checkedDocument checked each section and each row key against the keys of ErrorDataFile.
  return checkedDocument("error_data.json") as ErrorDataFile;
}

/** `workflow_spec_data.json`. */
export function specDataFile(): SpecDataFile {
  // SAFETY: checkedDocument checked each section and each row key against the keys of SpecDataFile.
  return checkedDocument("workflow_spec_data.json") as SpecDataFile;
}

/** The row `name` of a list; a missing row fails the test that names it. */
export function rowNamed<R extends { readonly name: string }>(rows: readonly R[], name: string): R {
  const row = rows.find((item) => item.name === name);

  assert(row !== undefined, `the vector ${name} exists`);

  return row;
}

/** A wire type that a TypeScript user declares for a vector type, with a label for the test name. */
export type Declared = { readonly label: string; readonly wire: RequiredWire };

const SAFE_INTEGER_MAX = BigInt(Number.MAX_SAFE_INTEGER);

function isSafeIntegerText(notation: Notation | undefined): boolean {
  if (!isString(notation)) {
    return true;
  }

  const value = BigInt(notation);

  return value <= SAFE_INTEGER_MAX && value >= -SAFE_INTEGER_MAX;
}

/**
 * The wire types that a TypeScript user declares for a vector type. An integer in the safe range has two: `h.int()`
 * and `h.bigint()` store the same text. A type with no wire type (only `bytes`, on rows marked
 * `typescript_cannot_express`) is a failure of the test that asks for it.
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
    case "time":
      return [{ label: "plainTime", wire: h.plainTime() }];
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
      throw new TypeError(`the vector type ${type} has no declared wire type`);
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
    case "time":
      return Temporal.PlainTime.from(notationText(notation));
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

/** One case per choice of declared wire types of a parameter list. */
export type ParameterCase = {
  readonly label: string;
  readonly parameters: readonly { readonly name: string; readonly wire: RequiredWire }[];
  readonly value: Readonly<Record<string, unknown>>;
};

export function parameterCases(parameters: readonly Parameter[]): readonly ParameterCase[] {
  const choices = parameters.map((parameter) => declaredWires(parameter.type, parameter.value));

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

const BUILT_IN_CODES: ReadonlySet<string> = new Set(BUILT_IN_TASK_CODES);
const STATUSES: ReadonlySet<string> = new Set(WORKFLOW_STATUSES);

function isBuiltInCode(text: string): text is BuiltInTaskCode {
  return BUILT_IN_CODES.has(text);
}

function isWorkflowStatus(text: string): text is WorkflowStatus {
  return STATUSES.has(text);
}

function builtInCode(text: string): BuiltInTaskCode {
  assert(isBuiltInCode(text), text);

  return text;
}

function workflowStatus(notation: Notation | undefined): WorkflowStatus {
  assert(isString(notation) && isWorkflowStatus(notation), String(notation));

  return notation;
}

function textOrNull(notation: Notation | undefined): string | null {
  if (notation === null || notation === undefined) {
    return null;
  }

  assert(isString(notation));

  return notation;
}

function countOf(notation: Notation | undefined): number {
  assert(Number.isInteger(notation));

  return Number(notation);
}

function exceptionOf(record: { readonly [key: string]: string }): ExceptionRecord {
  const { type = "", module = "", message = "", repr = "", traceback = "" } = record;

  return { type, module, message, repr, traceback };
}

/** The `TaskError` that an error notation names; `storedData` is the `data` that the writer stored in its place. */
export function taskErrorOf(error: ErrorNotation, storedData?: Notation): TaskError {
  const code = error.error_code;
  const subWorkflow = error.sub_workflow;

  return {
    ...("built_in" in code
      ? { kind: "builtIn", code: builtInCode(code.built_in) }
      : { kind: "custom", code: code.user }),
    message: error.message,
    data: storedData ?? error.data,
    exception: error.exception === null ? null : exceptionOf(error.exception),
    subWorkflow:
      subWorkflow === undefined
        ? null
        : {
            workflowId: subWorkflow.id,
            summary: {
              status: workflowStatus(subWorkflow.summary["status"]),
              output: subWorkflow.summary["output"] ?? null,
              errorSummary: textOrNull(subWorkflow.summary["error_summary"]),
              totalNodes: countOf(subWorkflow.summary["total_nodes"]),
              completedNodes: countOf(subWorkflow.summary["completed_nodes"]),
              failedNodes: countOf(subWorkflow.summary["failed_nodes"]),
              skippedNodes: countOf(subWorkflow.summary["skipped_nodes"]),
            },
          },
  };
}
