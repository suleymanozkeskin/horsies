// The kind tree: the declared wire type that drives the writer and the reader. Each `h.*` call builds one frozen
// node. The writer and the reader walk the tree and the value together, and never look at the JavaScript value to
// choose a stored form: `7` under `h.float()` is `7.0`, under `h.int()` it is `7`.

/** `null`. */
export type NullKind = { readonly node: "null" };

/** `true` or `false`. */
export type BooleanKind = { readonly node: "boolean" };

/** An integer in the safe range of a JavaScript `number`. */
export type IntKind = { readonly node: "int" };

/** An integer of any size, at most the core's digit bound, as a `bigint`. */
export type BigintKind = { readonly node: "bigint" };

/** A finite binary64 float, stored in Python's float text. */
export type FloatKind = { readonly node: "float" };

/** A well-formed string. */
export type StringKind = { readonly node: "string" };

/** A declared wire type. */
export type Kind = NullKind | BooleanKind | IntKind | BigintKind | FloatKind | StringKind;

export const NULL_KIND: NullKind = Object.freeze({ node: "null" });
export const BOOLEAN_KIND: BooleanKind = Object.freeze({ node: "boolean" });
export const INT_KIND: IntKind = Object.freeze({ node: "int" });
export const BIGINT_KIND: BigintKind = Object.freeze({ node: "bigint" });
export const FLOAT_KIND: FloatKind = Object.freeze({ node: "float" });
export const STRING_KIND: StringKind = Object.freeze({ node: "string" });
