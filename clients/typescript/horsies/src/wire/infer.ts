// The static types of a wire type: what a reader gives and what a sender gives.

import type { Wire, wireSent, wireValue } from "./builders.ts";

/** Any wire type: the constraint of a generic parameter. */
export type AnyWire = Wire<unknown, unknown>;

/** The value that a reader gives for a wire type. */
export type ValueOf<W extends AnyWire> = W[typeof wireValue];

/** The value that a sender gives for a wire type. */
export type SentOf<W extends AnyWire> = W[typeof wireSent];
