// The bounds of the event registration: the core's rules of a handler name and of the handler count. Each constant
// names its source in the core; the codegen renderer does not write them yet.

/** The most event handlers of an app (`MAX_EVENT_HANDLERS`, `core/src/worker/executor/protocol/bounds.rs`). */
export const MAX_EVENT_HANDLERS = 32;

/** The most characters of a handler name (`MAX_HANDLER_NAME_CHARS`, `core/src/worker/executor/protocol/bounds.rs`). */
export const MAX_HANDLER_NAME_CHARS = 255;

/**
 * The `handler` label value of a name outside the declared handlers, which no handler may take
 * (`DeclaredSource::HandlerNames.fallback`, `core/src/telemetry/labels.rs`).
 */
export const UNKNOWN_HANDLER_NAME = "(unknown)";
