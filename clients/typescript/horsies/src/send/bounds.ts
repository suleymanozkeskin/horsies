// The resend numbers of a send. They are the client's policy, the same as the Python and Rust clients' (Python
// `_SEND_RETRY_COUNT`, `_SEND_RETRY_INITIAL_MS`, `_SEND_RETRY_MAX_MS`; Rust `send/function.rs`). The core's bounds and
// the default queue route of a send are in the generated catalog (`_catalog/send_bounds.ts`).

/** Resends of a retryable `ENQUEUE_FAILED` after the first send, when `resendOnTransientError` is on. */
export const RESEND_COUNT = 3;

/** The wait before the first resend, in milliseconds; each later wait doubles. */
export const RESEND_FIRST_WAIT_MS = 200;

/** The longest wait before a resend, in milliseconds. */
export const RESEND_LONGEST_WAIT_MS = 2_000;
