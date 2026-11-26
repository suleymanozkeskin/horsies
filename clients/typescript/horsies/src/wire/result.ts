// The outcome of a call that can fail: the value, or one error of a closed union.

/** The value of a call that succeeded. */
export type Ok<T> = { readonly ok: true; readonly value: T };

/** The error of a call that failed. */
export type Err<E> = { readonly ok: false; readonly error: E };

/** The outcome of a call that can fail. `E` is a closed union of named errors. */
export type Result<T, E> = Ok<T> | Err<E>;

/** A success. Pure. */
export function ok<T>(value: T): Ok<T> {
  return { ok: true, value };
}

/** A failure. Pure. */
export function err<E>(error: E): Err<E> {
  return { ok: false, error };
}
