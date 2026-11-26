/* The declarations of the native addon of horsies, written by `napi build`. */
/* eslint-disable */

/** The answer of every addon function: its value, or why it threw. */
export type NativeEnvelope<T> =
  | { kind: 'value'; value: T }
  | { kind: 'thrown'; thrown: NativeThrown }

/** The outcome of one operation: its record, or its expected failure. */
export type NativeOutcome<T> =
  | { kind: 'done'; record: T }
  | { kind: 'failed'; error: RawCatalogError }
