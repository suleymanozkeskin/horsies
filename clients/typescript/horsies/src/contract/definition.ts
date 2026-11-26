// The facts of a contract that `defineTask` keeps, out of the contract's public members, and the refusals that the
// definition alone shows (no schema is compiled here, so no zod module is loaded).

import { isEngineKey } from "../codec/engine-keys.ts";
import { memberPath } from "../codec/refusal.ts";
import type { AnyWire } from "../wire/infer.ts";
import type { ExecutionClass } from "./define-task.ts";
import type { ParameterSchema, StandardSchemaMarker, ZodSchemaLike } from "./schema.ts";

/** The Standard Schema vendor name of zod. */
const ZOD_VENDOR = "zod";

/** The parameter names that the engine fills in a workflow: a sender and a task never declare them. */
const ENGINE_PARAMETER_NAMES: ReadonlySet<string> = new Set(["workflow_ctx", "workflow_meta"]);

/** A parameter name: a letter or `_`, then letters, digits and `_` (an identifier of every client). */
const PARAMETER_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** A schema of the definition: an `h.*` wire type, or a zod schema. */
export type SchemaSource =
  | { readonly kind: "wire"; readonly wire: AnyWire }
  | { readonly kind: "zod"; readonly schema: StandardSchemaMarker };

/** One input parameter, in declaration order. */
export type ParameterSource = { readonly name: string; readonly schema: SchemaSource };

/** A duration in milliseconds that the definition sets, or not. */
export type MillisecondsSetting = { readonly kind: "notSet" } | { readonly kind: "set"; readonly milliseconds: number };

/** The codes of a task: any code that is not built in, or the declared list. */
export type DeclaredCodes = { readonly kind: "open" } | { readonly kind: "declared"; readonly codes: readonly string[] };

/** The queue of a task: none (the default queue mode), or a named custom queue. */
export type QueueSetting = { readonly kind: "noQueue" } | { readonly kind: "named"; readonly name: string };

/** The retry policy of a task. */
export type RetrySetting =
  | { readonly kind: "noRetry" }
  | {
      readonly kind: "exponential";
      readonly baseSeconds: number;
      readonly maxRetries: number;
      readonly jitter: boolean;
      readonly maxDelay: MaxDelaySetting;
      readonly autoRetryFor: readonly string[];
    }
  | {
      readonly kind: "fixed";
      readonly intervalsSeconds: readonly number[];
      readonly jitter: boolean;
      readonly autoRetryFor: readonly string[];
    };

/** The longest wait between two retries of an exponential policy: no cap, or a number of seconds. */
export type MaxDelaySetting = { readonly kind: "noCap" } | { readonly kind: "seconds"; readonly seconds: number };

/** Why the definition of a contract cannot be a task. `path` names the place: `input.<name>` or `output`. */
export type DefinitionRefused =
  /** A Standard Schema that is not a zod schema: only `h.*` wire types and zod 4 schemas have a stored form. */
  | { readonly rule: "SchemaVendor"; readonly path: string; readonly vendor: string }
  /** A parameter name that is not an identifier, that the engine fills, or that only the engine writes. */
  | { readonly rule: "ParameterName"; readonly path: string; readonly detail: string };

/** The facts of one contract. Frozen. */
export type ContractRecord = {
  readonly name: string;
  readonly queue: QueueSetting;
  readonly parameters: readonly ParameterSource[];
  readonly output: SchemaSource;
  readonly codes: DeclaredCodes;
  readonly execution: ExecutionClass;
  readonly timeout: MillisecondsSetting;
  readonly cancelGrace: MillisecondsSetting;
  readonly retry: RetrySetting;
  readonly refusals: readonly DefinitionRefused[];
};

/** The definition fields as `defineTask` reads them, with the time rules of every class. */
export type DefinitionFields = {
  readonly name: string;
  readonly queue?: string;
  readonly input: { readonly [name: string]: ParameterSchema };
  readonly output: ParameterSchema;
  readonly errors?: readonly string[];
  readonly execution?: ExecutionClass;
  readonly retry?: RetryFields;
  readonly timeoutMs?: number;
  readonly cancelGraceMs?: number;
};

/** The retry policy fields as `defineTask` reads them. */
type RetryFields =
  | {
      readonly kind: "exponential";
      readonly baseSeconds: number;
      readonly maxRetries: number;
      readonly jitter?: boolean;
      readonly maxDelaySeconds?: number;
      readonly autoRetryFor: readonly string[];
    }
  | {
      readonly kind: "fixed";
      readonly intervalsSeconds: readonly number[];
      readonly jitter?: boolean;
      readonly autoRetryFor: readonly string[];
    };

/** `jitter` of a retry policy that leaves it out (the Python client's `RetryPolicy.jitter` default). */
const DEFAULT_JITTER = true;

const records = new WeakMap<object, ContractRecord>();

/** Keep the facts of `contract`. Called once, by `defineTask`. */
export function recordDefinition(contract: object, fields: DefinitionFields): void {
  records.set(contract, recordOf(fields));
}

/** The facts of a contract. A value that `defineTask` did not build is a defect: `TypeError`. */
export function contractRecord(contract: object): ContractRecord {
  const kept = records.get(contract);

  if (kept === undefined) {
    throw new TypeError("horsies: the value is not a task contract (build it with defineTask)");
  }

  return kept;
}

/** Whether `defineTask` built the value. */
export function isContract(value: object): boolean {
  return records.has(value);
}

function recordOf(fields: DefinitionFields): ContractRecord {
  const parameters = Object.keys(fields.input).map((name) => parameterSource(fields.input, name));
  const output = schemaSource(fields.output);

  const refusals = [
    ...parameters.flatMap((parameter) => parameterRefusals(parameter, fields.input)),
    ...schemaRefusals(fields.output, "output"),
  ];

  const record: ContractRecord = {
    name: fields.name,
    queue: fields.queue === undefined ? { kind: "noQueue" } : { kind: "named", name: fields.queue },
    parameters: Object.freeze(parameters),
    output,
    codes: fields.errors === undefined ? { kind: "open" } : { kind: "declared", codes: Object.freeze([...fields.errors]) },
    execution: fields.execution ?? "ISOLATED",
    timeout: millisecondsSetting(fields.timeoutMs),
    cancelGrace: millisecondsSetting(fields.cancelGraceMs),
    retry: retrySetting(fields.retry),
    refusals: Object.freeze(refusals),
  };

  return Object.freeze(record);
}

function parameterSource(input: DefinitionFields["input"], name: string): ParameterSource {
  const schema = input[name];

  if (schema === undefined) {
    throw new TypeError(`defineTask: the parameter ${name} has no schema`);
  }

  return Object.freeze({ name, schema: schemaSource(schema) });
}

/** The source of a schema: a zod schema by its Standard Schema marker, else an `h.*` wire type. */
function schemaSource(schema: ParameterSchema): SchemaSource {
  if (isStandardSchema(schema)) {
    return Object.freeze({ kind: "zod", schema });
  }

  // SAFETY: a schema without the Standard Schema marker is an `h.*` wire type: the static type allows only the two, and
  // `kindOf` refuses any other value with a `TypeError` when the contract compiles.
  return Object.freeze({ kind: "wire", wire: schema as AnyWire });
}

function isStandardSchema(schema: ParameterSchema): schema is ZodSchemaLike & StandardSchemaMarker {
  return "~standard" in schema;
}

function parameterRefusals(parameter: ParameterSource, input: DefinitionFields["input"]): readonly DefinitionRefused[] {
  const path = memberPath("input", parameter.name);
  const schema = input[parameter.name];
  const schemaRefused = schema === undefined ? [] : schemaRefusals(schema, path);

  return [...parameterNameRefusals(parameter.name, path), ...schemaRefused];
}

/** The refusal of a parameter name, or none. Pure. */
function parameterNameRefusals(name: string, path: string): readonly DefinitionRefused[] {
  if (!PARAMETER_NAME.test(name)) {
    return [{ rule: "ParameterName", path, detail: "a parameter name is a letter or _, then letters, digits and _" }];
  }

  if (ENGINE_PARAMETER_NAMES.has(name)) {
    return [{ rule: "ParameterName", path, detail: "the workflow engine fills a parameter of this name" }];
  }

  if (isEngineKey(name)) {
    return [{ rule: "ParameterName", path, detail: "only the engine writes a key with this name" }];
  }

  return [];
}

/**
 * The refusal of one schema: a Standard Schema of another vendor. The zod adapter checks the release of a zod schema
 * (a zod 3 schema has the same marker) when the contract compiles. Pure.
 */
function schemaRefusals(schema: ParameterSchema, path: string): readonly DefinitionRefused[] {
  if (!isStandardSchema(schema)) {
    return [];
  }

  const vendor = schema["~standard"].vendor;

  return vendor === ZOD_VENDOR ? [] : [{ rule: "SchemaVendor", path, vendor }];
}

function millisecondsSetting(value: number | undefined): MillisecondsSetting {
  return value === undefined ? { kind: "notSet" } : { kind: "set", milliseconds: value };
}

function maxDelaySetting(seconds: number | undefined): MaxDelaySetting {
  return seconds === undefined ? { kind: "noCap" } : { kind: "seconds", seconds };
}

function retrySetting(retry: RetryFields | undefined): RetrySetting {
  if (retry === undefined) {
    return { kind: "noRetry" };
  }

  switch (retry.kind) {
    case "exponential":
      return Object.freeze({
        kind: "exponential",
        baseSeconds: retry.baseSeconds,
        maxRetries: retry.maxRetries,
        jitter: retry.jitter ?? DEFAULT_JITTER,
        maxDelay: maxDelaySetting(retry.maxDelaySeconds),
        autoRetryFor: Object.freeze([...retry.autoRetryFor]),
      });
    case "fixed":
      return Object.freeze({
        kind: "fixed",
        intervalsSeconds: Object.freeze([...retry.intervalsSeconds]),
        jitter: retry.jitter ?? DEFAULT_JITTER,
        autoRetryFor: Object.freeze([...retry.autoRetryFor]),
      });
    default: {
      const unexpected: never = retry;

      throw new TypeError(`defineTask: a retry policy kind outside its type: ${String(unexpected)}`);
    }
  }
}
