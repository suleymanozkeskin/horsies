import { BUILTIN_CODE_KEY, ENGINE_KEY_PREFIXES } from "./bounds.ts";

/**
 * Whether only the engine may write this object key: a key that starts with an engine prefix, and the tag of a
 * built-in error code (core `is_engine_key`). Pure.
 */
export function isEngineKey(key: string): boolean {
  return key === BUILTIN_CODE_KEY || ENGINE_KEY_PREFIXES.some((prefix) => key.startsWith(prefix));
}
