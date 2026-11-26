// The static types of a zod schema as a wire type: the value that a reader gives and the value that a sender gives,
// and the types of a task input that mixes `h.*` wire types and zod schemas.
//
// This module imports no zod, also as a type: the main entry's types use it, and the main entry type checks without
// zod installed. It reads a schema's type structurally, from the fields that every zod 4 schema type declares:
// `_zod.def.type` and the definition fields, `_zod.output`, `_zod.optin` and `_zod.optout`.
//
// A reader gives zod's output type, with one change that follows the codec: `z.json()` gives `JsonValue` (an integer
// outside the safe range reads as a `bigint`, as for `h.json()`), at every depth. A sender gives the same value, but
// may leave out a member with `.optional()`, `.default()` or `.prefault()`: the sender never stores a default. zod's
// input type is not the sent type: it holds a codec's stored side (a string for `hz.instant()`).

import type { JsonValue } from "../codec/json-value.ts";
import type { AnyWire, EmptyObject, Flat, PresenceOf, SentOf, ValueOf } from "../wire/infer.ts";

/** Type-only key of the sent type of `hz.wire()`. It does not exist at run time. */
export declare const zodWireSent: unique symbol;

/**
 * A zod 4 schema type, as the static types read it: the definition type that every zod 4 schema declares. The output
 * type is read from the schema's own type (`ZodOutputOf`), so the constraint names no top type.
 */
export type ZodSchemaLike = {
  readonly _zod: { readonly def: { readonly type: string } };
};

export type ZodDefOf<S> = S extends { readonly _zod: { readonly def: infer D } } ? D : never;

export type ZodOutputOf<S> = S extends { readonly _zod: { readonly output: infer O } } ? O : never;

/**
 * Whether an output type is zod's `JSONType`, the output of `z.json()`: it takes every JSON primitive and arrays of
 * itself, and it does not take `undefined`. The `undefined` test keeps out a top type (the output of `z.unknown()`,
 * which takes every value) and an optional member (`z.json().optional()`, read through its `optional` definition).
 * (`z.json()` is a lazy schema at run time and a union in its static type.)
 */
export type IsZodJson<O> = [undefined] extends [O]
  ? false
  : [string | number | boolean | null | O[]] extends [O]
    ? true
    : false;

/** A member that a reader may give without the key: zod marks `.optional()` with `optout`. */
export type ZodOptionalOut = { readonly _zod: { readonly optout: "optional" } };

/**
 * A member that a sender may leave out: zod marks `.optional()`, `.default()` and `.prefault()` with `optin`. zod 4.1
 * marks each one `"optional"`; later zod 4 releases mark a default `"defaulted"`.
 */
export type ZodOptionalIn = { readonly _zod: { readonly optin: "optional" | "defaulted" } };

export type ZodObjectValue<M> = keyof M extends never
  ? EmptyObject
  : Flat<
      {
        -readonly [K in keyof M as M[K] extends ZodOptionalOut ? never : K]: ZodValueOf<M[K]>;
      } & {
        -readonly [K in keyof M as M[K] extends ZodOptionalOut ? K : never]?: ZodValueOf<M[K]>;
      }
    >;

export type ZodObjectSent<M> = keyof M extends never
  ? EmptyObject
  : Flat<
      {
        -readonly [K in keyof M as M[K] extends ZodOptionalIn ? never : K]: ZodSentOf<M[K]>;
      } & {
        -readonly [K in keyof M as M[K] extends ZodOptionalIn ? K : never]?: ZodSentOf<M[K]>;
      }
    >;

/** The value that a reader gives for a zod schema: zod's output type, with `JsonValue` for `z.json()`. */
export type ZodValueOf<S> =
  IsZodJson<ZodOutputOf<S>> extends true
    ? JsonValue
    : ZodContainerValue<S>;

export type ZodContainerValue<S> =
  ZodDefOf<S> extends { readonly type: "object"; readonly "shape": infer M }
    ? ZodObjectValue<M>
    : ZodDefOf<S> extends { readonly type: "array"; readonly element: infer I }
      ? ZodValueOf<I>[]
      : ZodDefOf<S> extends { readonly type: "tuple"; readonly items: infer T }
        ? { -readonly [K in keyof T]: ZodValueOf<T[K]> }
        : ZodDefOf<S> extends { readonly type: "record"; readonly valueType: infer V }
          ? { [K in keyof ZodOutputOf<S>]: ZodValueOf<V> }
          : ZodDefOf<S> extends { readonly type: "nullable"; readonly innerType: infer I }
            ? ZodValueOf<I> | null
            : ZodDefOf<S> extends { readonly type: "optional"; readonly innerType: infer I }
              ? ZodValueOf<I> | undefined
              : ZodDefOf<S> extends { readonly type: "default" | "prefault" | "nonoptional"; readonly innerType: infer I }
                ? Exclude<ZodValueOf<I>, undefined>
                : ZodDefOf<S> extends { readonly type: "readonly"; readonly innerType: infer I }
                  ? Readonly<ZodValueOf<I>>
                  : ZodDefOf<S> extends { readonly type: "lazy"; readonly getter: () => infer I }
                    ? ZodValueOf<I>
                    : ZodDefOf<S> extends { readonly type: "union"; readonly options: readonly (infer O)[] }
                      ? ZodValueOf<O>
                      : ZodOutputOf<S>;

/**
 * The value that a sender gives for a zod schema. It follows the containers (object, array, tuple, record, union,
 * nullable, readonly, lazy) down to each member; any other schema (a leaf, a codec) gives zod's output type.
 */
export type ZodSentOf<S> = S extends { readonly [zodWireSent]: infer Sent }
  ? Sent
  : IsZodJson<ZodOutputOf<S>> extends true
    ? JsonValue
    : ZodContainerSent<S>;

export type ZodContainerSent<S> =
  ZodDefOf<S> extends { readonly type: "object"; readonly "shape": infer M }
    ? ZodObjectSent<M>
    : ZodDefOf<S> extends { readonly type: "array"; readonly element: infer I }
      ? ZodSentOf<I>[]
      : ZodDefOf<S> extends { readonly type: "tuple"; readonly items: infer T }
        ? { -readonly [K in keyof T]: ZodSentOf<T[K]> }
        : ZodDefOf<S> extends { readonly type: "record"; readonly valueType: infer V }
          ? { [K in keyof ZodOutputOf<S>]: ZodSentOf<V> }
          : ZodDefOf<S> extends { readonly type: "nullable"; readonly innerType: infer I }
            ? ZodSentOf<I> | null
            : ZodDefOf<S> extends {
                  readonly type: "optional" | "default" | "prefault" | "nonoptional";
                  readonly innerType: infer I;
                }
              ? ZodSentOf<I>
              : ZodDefOf<S> extends { readonly type: "readonly"; readonly innerType: infer I }
                ? Readonly<ZodSentOf<I>>
                : ZodDefOf<S> extends { readonly type: "lazy"; readonly getter: () => infer I }
                  ? ZodSentOf<I>
                  : ZodDefOf<S> extends { readonly type: "union"; readonly options: readonly (infer O)[] }
                    ? ZodSentOf<O>
                    : ZodOutputOf<S>;

/** A task input parameter: an `h.*` wire type or a zod schema. */
export type ParameterSchema = AnyWire | ZodSchemaLike;

/** The parameters of a task input: an `h.*` wire type or a zod schema per parameter name. */
export type ParameterSchemas = { readonly [name: string]: ParameterSchema };

/** The value of one parameter that a reader gives. */
export type ParameterValue<P> = P extends AnyWire ? ValueOf<P> : ZodValueOf<P>;

/** The value of one parameter that a sender gives. */
export type ParameterSent<P> = P extends AnyWire ? SentOf<P> : ZodSentOf<P>;

/** A parameter that a reader may give without the key: `h.*` `.optional()`, zod `.optional()`. */
export type ReaderMayLeaveOut<P> = P extends AnyWire
  ? PresenceOf<P> extends "optional"
    ? true
    : false
  : P extends ZodOptionalOut
    ? true
    : false;

/** A parameter that a sender may leave out: `h.*` `.optional()` and `.default()`, zod `optin`. */
export type SenderMayLeaveOut<P> = P extends AnyWire
  ? PresenceOf<P> extends "required"
    ? false
    : true
  : P extends ZodOptionalIn
    ? true
    : false;

/** The task input that a body gets: one member per parameter, `h.*` and zod alike. */
export type ParametersValue<M extends ParameterSchemas> = keyof M extends never
  ? EmptyObject
  : Flat<
      {
        -readonly [K in keyof M as ReaderMayLeaveOut<M[K]> extends true ? never : K]: ParameterValue<M[K]>;
      } & {
        -readonly [K in keyof M as ReaderMayLeaveOut<M[K]> extends true ? K : never]?: ParameterValue<M[K]>;
      }
    >;

/** The task input that a sender gives: a parameter with `.optional()` or a default may be left out. */
export type ParametersSent<M extends ParameterSchemas> = keyof M extends never
  ? EmptyObject
  : Flat<
      {
        -readonly [K in keyof M as SenderMayLeaveOut<M[K]> extends true ? never : K]: ParameterSent<M[K]>;
      } & {
        -readonly [K in keyof M as SenderMayLeaveOut<M[K]> extends true ? K : never]?: ParameterSent<M[K]>;
      }
    >;
