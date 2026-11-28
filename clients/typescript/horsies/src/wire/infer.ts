// The static types of a wire type: what a reader gives and what a sender gives.

import type { Presence, Role, Wire, wirePresence, wireRole, wireSent, wireValue } from "./wire-type.ts";

/** Any wire type: the constraint of a generic parameter. */
export type AnyWire = Wire<unknown, unknown, Presence, Role>;

/** A wire type of a plain value: an object member. */
export type ValueWire = Wire<unknown, unknown, Presence, "value">;

/** A wire type of a plain value that is always present: an array item, a tuple item, a record value, an output. */
export type RequiredWire = Wire<unknown, unknown, "required", "value">;

/** The value that a reader gives for a wire type. */
export type ValueOf<W extends AnyWire> = W[typeof wireValue];

/** The value that a sender gives for a wire type. */
export type SentOf<W extends AnyWire> = W[typeof wireSent];

/** How a wire type is present as an object member. */
export type PresenceOf<W extends AnyWire> = W[typeof wirePresence];

/** Whether a wire type is a plain value or an upstream outcome. */
export type RoleOf<W extends AnyWire> = W[typeof wireRole];

/** The members of an object: a wire type of a plain value per member name, in declaration order. */
export type WireMembers = { readonly [name: string]: ValueWire };

/** The parameters of a task input: a wire type per parameter, which may be an upstream outcome (`h.taskOutcome`). */
export type ParameterWires = { readonly [name: string]: AnyWire };

declare const noMembers: unique symbol;

/** The value of an object with no members: only `{}`. A key is refused by the excess property check. */
export interface EmptyObject {
  readonly [noMembers]?: never;
}

/** An intersection of object types, shown as one object type. */
export type Flat<T> = { [K in keyof T]: T[K] } & {};

/** The value that a reader gives for an object: an `.optional()` member may be absent; a defaulted one is filled. */
export type ObjectValue<M extends WireMembers> = keyof M extends never
  ? EmptyObject
  : Flat<
      {
        -readonly [K in keyof M as PresenceOf<M[K]> extends "optional" ? never : K]: ValueOf<M[K]>;
      } & {
        -readonly [K in keyof M as PresenceOf<M[K]> extends "optional" ? K : never]?: ValueOf<M[K]>;
      }
    >;

/** The value that a sender gives for an object: an `.optional()` or defaulted member may be left out. */
export type ObjectSent<M extends WireMembers> = keyof M extends never
  ? EmptyObject
  : Flat<
      {
        -readonly [K in keyof M as PresenceOf<M[K]> extends "required" ? K : never]: SentOf<M[K]>;
      } & {
        -readonly [K in keyof M as PresenceOf<M[K]> extends "required" ? never : K]?: SentOf<M[K]>;
      }
    >;

/** The values of a tuple of wire types. */
export type TupleValue<T extends readonly RequiredWire[]> = { -readonly [K in keyof T]: ValueOf<T[K]> };

/** The sent values of a tuple of wire types. */
export type TupleSent<T extends readonly RequiredWire[]> = { -readonly [K in keyof T]: SentOf<T[K]> };
