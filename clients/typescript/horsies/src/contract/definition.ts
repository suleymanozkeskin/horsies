// The facts of a contract that `defineTask` keeps, out of the contract's public members, and the refusals that the
// definition alone shows (no schema is compiled here).

import { isEngineKey } from "../codec/engine-keys.ts";
import { memberPath } from "../codec/refusal.ts";
import type { Execution } from "../_catalog/monitoring_models.ts";
import type { AnyWire } from "../wire/infer.ts";
import type { ContextUse } from "./define-task.ts";
import type { ParameterSchema, StandardSchemaMarker, ZodSchemaLike } from "./schema.ts";

/** The Standard Schema vendor name of zod. */
const ZOD_VENDOR = "zod";

/** The parameter names that the engine fills in a workflow: a sender and a task never declare them. */
const ENGINE_PARAMETER_NAMES: ReadonlySet<string> = new Set(["workflow_context", "workflow_meta"]);

/** A parameter name: a letter or `_`, then letters, digits and `_` (an identifier of every client). */
const PARAMETER_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * A schema of the definition: an `h.*` wire type, a zod schema, or a Standard Schema of another vendor (only `h.*`
 * wire types and zod 4 schemas have a stored form, so the compile refuses it).
 */
export type SchemaSource =
  | { readonly kind: "wire"; readonly wire: AnyWire }
  | { readonly kind: "zod"; readonly schema: StandardSchemaMarker }
  | { readonly kind: "otherVendor"; readonly vendor: string };

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
      readonly retryFor: readonly string[];
    }
  | {
      readonly kind: "fixed";
      readonly intervalsSeconds: readonly number[];
      readonly jitter: boolean;
      readonly retryFor: readonly string[];
    };

/** The longest wait between two retries of an exponential policy: no cap, or a number of seconds. */
export type MaxDelaySetting = { readonly kind: "noCap" } | { readonly kind: "seconds"; readonly seconds: number };

/** Why a task name cannot be stored: it is empty, or it holds a lone surrogate (text that is not well formed). */
export type TaskNameFault = "empty" | "loneSurrogate";

/** Why the definition of a contract cannot be a task. `path` names the place: `name`, `input.<name>` or `output`. */
export type DefinitionRefused =
  /** A task name that cannot be stored. */
  | { readonly rule: "TaskName"; readonly path: "name"; readonly fault: TaskNameFault }
  /** A parameter name that is not an identifier, that the engine fills, or that only the engine writes. */
  | { readonly rule: "ParameterName"; readonly path: string; readonly detail: string };

/** The facts of one contract. Frozen. */
export type ContractRecord = {
  readonly name: string;
  readonly queue: QueueSetting;
  readonly parameters: readonly ParameterSource[];
  readonly output: SchemaSource;
  readonly codes: DeclaredCodes;
  readonly execution: Execution;
  readonly timeout: MillisecondsSetting;
  readonly cancelGrace: MillisecondsSetting;
  readonly retry: RetrySetting;
  /** Whether the task takes the workflow context (`workflowContext: true`). */
  readonly context: ContextUse;
  readonly refusals: readonly DefinitionRefused[];
};

/** The definition fields as `defineTask` reads them, with the time rules of every class. */
export type DefinitionFields = {
  readonly name: string;
  readonly queue?: string;
  readonly input: { readonly [name: string]: ParameterSchema };
  readonly output: ParameterSchema;
  readonly errorCodes?: readonly string[];
  readonly execution?: Execution;
  readonly retryPolicy?: RetryFields;
  readonly timeoutMs?: number;
  readonly cancelGraceMs?: number;
  readonly workflowContext?: boolean;
};

/** The retry policy fields as `defineTask` reads them. */
type RetryFields =
  | {
      readonly kind: "exponential";
      readonly baseSeconds: number;
      readonly maxRetries: number;
      readonly jitter?: boolean;
      readonly maxDelaySeconds?: number;
      readonly retryFor: readonly string[];
    }
  | {
      readonly kind: "fixed";
      readonly intervalsSeconds: readonly number[];
      readonly jitter?: boolean;
      readonly retryFor: readonly string[];
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
    ...taskNameRefusals(fields.name),
    ...parameters.flatMap((parameter) => parameterNameRefusals(parameter.name, memberPath("input", parameter.name))),
  ];

  const record: ContractRecord = {
    name: fields.name,
    queue: fields.queue === undefined ? { kind: "noQueue" } : { kind: "named", name: fields.queue },
    parameters: Object.freeze(parameters),
    output,
    codes:
      fields.errorCodes === undefined
        ? { kind: "open" }
        : { kind: "declared", codes: Object.freeze([...fields.errorCodes]) },
    execution: fields.execution ?? "ISOLATED",
    timeout: millisecondsSetting(fields.timeoutMs),
    cancelGrace: millisecondsSetting(fields.cancelGraceMs),
    retry: retrySetting(fields.retryPolicy),
    context: fields.workflowContext === true ? "takesContext" : "noContext",
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

/**
 * The source of a schema: by its Standard Schema marker a zod schema (the vendor `zod`) or a schema of another vendor,
 * else an `h.*` wire type. The zod adapter checks the release of a zod schema (a zod 3 schema has the same marker)
 * when the contract compiles.
 */
function schemaSource(schema: ParameterSchema): SchemaSource {
  if (isStandardSchema(schema)) {
    const vendor = schema["~standard"].vendor;
    const source: SchemaSource = vendor === ZOD_VENDOR ? { kind: "zod", schema } : { kind: "otherVendor", vendor };

    return Object.freeze(source);
  }

  // SAFETY: a schema without the Standard Schema marker is an `h.*` wire type: the static type allows only the two, and
  // `kindOf` refuses any other value with a `TypeError` when the contract compiles.
  return Object.freeze({ kind: "wire", wire: schema as AnyWire });
}

function isStandardSchema(schema: ParameterSchema): schema is ZodSchemaLike & StandardSchemaMarker {
  return "~standard" in schema;
}

/**
 * The refusal of a task name, or none. The core stores the name with each send and finds the implementation by it:
 * an empty name has no fingerprint, and a lone surrogate does not cross the addon boundary unchanged. Pure.
 */
function taskNameRefusals(name: string): readonly DefinitionRefused[] {
  if (name === "") {
    return [{ rule: "TaskName", path: "name", fault: "empty" }];
  }

  return name.isWellFormed() ? [] : [{ rule: "TaskName", path: "name", fault: "loneSurrogate" }];
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
        retryFor: Object.freeze([...retry.retryFor]),
      });
    case "fixed":
      return Object.freeze({
        kind: "fixed",
        intervalsSeconds: Object.freeze([...retry.intervalsSeconds]),
        jitter: retry.jitter ?? DEFAULT_JITTER,
        retryFor: Object.freeze([...retry.retryFor]),
      });
    default: {
      const unexpected: never = retry;

      throw new TypeError(`defineTask: a retry policy kind outside its type: ${String(unexpected)}`);
    }
  }
}
