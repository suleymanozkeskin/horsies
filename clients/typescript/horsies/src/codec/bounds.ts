// Bounds and spellings of the core's stored forms. Each constant names its source in the core. The codegen
// output takes them over (#2372).

/**
 * The deepest nesting of arrays and objects in a stored text (core `MAX_JSON_DEPTH`,
 * `core/src/core/codec/strict_read.rs`). The outermost array or object is level 1.
 */
export const MAX_JSON_DEPTH = 300;
