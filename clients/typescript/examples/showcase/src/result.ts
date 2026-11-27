// The constructors of the horsies `Result` for the showcase's own functions. The package exports the type; an app
// builds its values. A `Result` that a caller drops is a lint finding (`horsies/no-dropped-result`).

import type { Err, Ok } from "horsies";

/** A success. Pure. */
export function ok<T>(value: T): Ok<T> {
  return { ok: true, value };
}

/** A failure. Pure. */
export function err<E>(error: E): Err<E> {
  return { ok: false, error };
}
