// The outcome of a call that can fail: the value, or one error of a closed union.

/**
 * Type-only key that marks the horsies `Result`, so that the lint rule for a dropped result finds the type. It does
 * not exist at run time, and a value never has it.
 */
export declare const resultBrand: unique symbol;

/** The value of a call that succeeded. */
export type Ok<T> = { readonly ok: true; readonly value: T; readonly [resultBrand]?: "horsies.Result" };

/** The error of a call that failed. */
export type Err<E> = { readonly ok: false; readonly error: E; readonly [resultBrand]?: "horsies.Result" };

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
