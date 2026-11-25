// Bounds and spellings of the core's stored forms. Each constant names its source in the core. The codegen
// output takes them over (#2372).

/**
 * The deepest nesting of arrays and objects in a stored text (core `MAX_JSON_DEPTH`,
 * `core/src/core/codec/strict_read.rs`). The outermost array or object is level 1.
 */
export const MAX_JSON_DEPTH = 300;

/** The most digits of a stored integer (core `MAX_INT_DIGITS`, `core/src/core/codec/strict_read.rs`). */
export const MAX_INT_DIGITS = 4300;

/**
 * The deepest nesting of a value that user code builds (core `MAX_VALUE_JSON_DEPTH`,
 * `core/src/core/codec/user_value.rs`). The outermost array or object is level 1.
 */
export const MAX_VALUE_JSON_DEPTH = 200;

/** Key prefixes that only the engine writes (core `ENGINE_KEY_PREFIXES`, `core/src/core/codec/user_value.rs`). */
export const ENGINE_KEY_PREFIXES: readonly string[] = Object.freeze(["__h_", "__horsies_"]);

/** The tag key of a built-in error code (core `BUILTIN_CODE_KEY`, `core/src/core/codec/user_value.rs`). */
export const BUILTIN_CODE_KEY = "__builtin_task_code__";
