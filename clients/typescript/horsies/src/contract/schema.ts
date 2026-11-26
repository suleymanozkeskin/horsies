// The static types of the schemas of a contract: an `h.*` wire type, or a zod 4 schema.
//
// The main entry point imports no zod, also as a type. The types of a zod schema come from `src/zod/infer.ts`, which
// reads a schema's type by its structure and imports no zod: a body and a reader get zod's output type (`z.json()`
// gives `JsonValue`); a sender may leave out a member that zod marks `.optional()`, `.default()` or `.prefault()`.

import type { AnyWire, EmptyObject, PresenceOf, RequiredWire, RoleOf, SentOf, ValueOf } from "../wire/infer.ts";
import type {
  ParameterSchema,
  ParameterSchemas,
  ParametersSent,
  ParametersValue,
  ZodSchemaLike,
  ZodSentOf,
  ZodValueOf,
} from "../zod/infer.ts";

export type { ParameterSchema, ParameterSchemas, ZodSchemaLike };

/** The Standard Schema marker of a schema object: a zod schema has it, an `h.*` wire type does not. */
export interface StandardSchemaMarker {
  readonly "~standard": { readonly vendor: string; readonly version: number };
}

/** The schema of a task output: an `h.*` wire type that is always present, or a zod schema. */
export type OutputSchema = RequiredWire | ZodSchemaLike;

/** The value that a body and a reader get for a schema. */
export type SchemaValue<S> = S extends AnyWire ? ValueOf<S> : ZodValueOf<S>;

/** The value that a sender gives for a schema. */
export type SchemaSent<S> = S extends AnyWire ? SentOf<S> : ZodSentOf<S>;

/** Whether a parameter is an upstream outcome that only a workflow node gives (`h.resultOf`). */
type IsUpstreamOutcome<S> = S extends AnyWire ? (RoleOf<S> extends "resultOf" ? true : false) : false;

/** Whether a sender may leave out an upstream outcome parameter (an `h.*` wire type): `.optional()`. */
type LeftOutBySender<S> = S extends AnyWire ? (PresenceOf<S> extends "required" ? false : true) : false;

/** The parameters that a sender gives: every parameter that is not an upstream outcome. */
type PlainParameters<P extends ParameterSchemas> = {
  readonly [K in keyof P as IsUpstreamOutcome<P[K]> extends true ? never : K]: P[K];
};

/** The input that a body gets: every parameter; an `.optional()` one may be absent; a defaulted one is filled. */
export type BodyInput<P extends ParameterSchemas> = ParametersValue<P>;

/** The input that a sender gives: the plain parameters; an `.optional()` or defaulted one may be left out. */
export type SendInput<P extends ParameterSchemas> = keyof PlainParameters<P> extends never
  ? EmptyObject
  : ParametersSent<PlainParameters<P>>;

/** Whether a task takes an upstream outcome that a sender cannot leave out. */
type HasRequiredUpstreamOutcome<P extends ParameterSchemas> = true extends {
  [K in keyof P]: IsUpstreamOutcome<P[K]> extends true ? (LeftOutBySender<P[K]> extends true ? false : true) : false;
}[keyof P]
  ? true
  : false;

/** The type of the input of a task that only a workflow node can run: no value has it. */
export type WorkflowNodeOnly =
  "horsies: this task takes an upstream result (h.resultOf); only a workflow node can run it";

/** The input that `app.send` takes: `SendInput`, or a refusal text for a task that only a workflow node can run. */
export type SendableInput<P extends ParameterSchemas> =
  HasRequiredUpstreamOutcome<P> extends true ? WorkflowNodeOnly : SendInput<P>;
