// The static types of a zod schema as a wire type: the value that a reader gives and the value that a sender gives.
//
// A reader gives zod's output type (`z.output`): codec outputs, defaults filled. A sender gives the same value, but
// may leave out a member with `.optional()`, `.default()` or `.prefault()`: the sender never stores a default. zod's
// input type is not the sent type: it holds a codec's stored side (a string for `hz.instant()`).

import type * as core from "zod/v4/core";

import type { EmptyObject } from "../wire/infer.ts";
import type { zodWireSent } from "./codecs.ts";

/** The value that a reader gives for a zod schema. */
export type ZodValueOf<S extends core.$ZodType> = core.output<S>;

/**
 * A member that a sender may leave out: zod marks `.optional()`, `.default()` and `.prefault()` with `optin`. zod 4.1
 * marks each one `"optional"`; later zod 4 releases mark a default `"defaulted"`.
 */
type OptionalIn = { readonly _zod: { readonly optin: "optional" | "defaulted" } };

type Flat<T> = { [K in keyof T]: T[K] } & {};

/** The value that a sender gives for an object: a member that zod marks `optin` may be left out. */
type ObjectSent<M> = keyof M extends never
  ? EmptyObject
  : Flat<
      {
        -readonly [K in keyof M as M[K] extends OptionalIn ? never : K]: ZodSentOf<M[K]>;
      } & {
        -readonly [K in keyof M as M[K] extends OptionalIn ? K : never]?: ZodSentOf<M[K]>;
      }
    >;

/** The values of a tuple that a sender gives. */
type TupleSent<T> = { -readonly [K in keyof T]: ZodSentOf<T[K]> };

/**
 * The value that a sender gives for a zod schema. It follows the containers (object, array, tuple, record, union,
 * nullable, readonly, lazy) down to each member; any other schema (a leaf, a codec) gives zod's output type.
 */
export type ZodSentOf<S> = S extends { readonly [zodWireSent]: infer Sent }
  ? Sent
  : S extends core.$ZodObject<infer Members>
    ? ObjectSent<Members>
    : S extends core.$ZodArray<infer Item>
      ? ZodSentOf<Item>[]
      : S extends core.$ZodTuple<infer Items, null>
        ? TupleSent<Items>
        : S extends core.$ZodRecord<core.$ZodRecordKey, infer Value>
          ? { [K in keyof core.output<S>]: ZodSentOf<Value> }
          : S extends core.$ZodNullable<infer Inner>
            ? ZodSentOf<Inner> | null
            : S extends
                  | core.$ZodOptional<infer Inner>
                  | core.$ZodDefault<infer Inner>
                  | core.$ZodPrefault<infer Inner>
                  | core.$ZodNonOptional<infer Inner>
                  | core.$ZodLazy<infer Inner>
              ? ZodSentOf<Inner>
              : S extends core.$ZodReadonly<infer Inner>
                ? Readonly<ZodSentOf<Inner>>
                : S extends core.$ZodUnion<infer Options>
                  ? ZodSentOf<Options[number]>
                  : core.output<S>;
