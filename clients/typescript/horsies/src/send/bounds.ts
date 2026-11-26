// The bounds of a send and of a result wait.
//
// - The core's bounds below have no constant in the generated catalog yet; each names its core source, and issue
//   #2425 moves them to the codegen output.
// - The resend numbers are the client's policy, the same as the Python and Rust clients' (Python `_SEND_RETRY_COUNT`,
//   `_SEND_RETRY_INITIAL_MS`, `_SEND_RETRY_MAX_MS`; Rust `send/function.rs`).

/** The most UTF-8 bytes of an idempotency key (core `IDEMPOTENCY_KEY_MAX_BYTES`, `core/src/core/history/identity/keys.rs`). */
export const IDEMPOTENCY_KEY_MAX_BYTES = 255;

/** The longest result wait, in milliseconds (core `MAX_WAIT_LIMIT_MS`, `core/src/broker/wait_limit.rs`). */
export const MAX_WAIT_LIMIT_MS = 4_294_967_295;

/**
 * The priority of the one queue of the default queue mode. The Python and Rust clients hold the same value
 * (`clients/rust/horsies/src/registry.rs`, `DEFAULT_PRIORITY`); no core constant holds it yet.
 */
export const DEFAULT_QUEUE_PRIORITY = 100;

/** Resends of a retryable `ENQUEUE_FAILED` after the first send, when `resendOnTransientErr` is on. */
export const RESEND_COUNT = 3;

/** The wait before the first resend, in milliseconds; each later wait doubles. */
export const RESEND_FIRST_WAIT_MS = 200;

/** The longest wait before a resend, in milliseconds. */
export const RESEND_LONGEST_WAIT_MS = 2_000;
