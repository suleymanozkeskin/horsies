// Retry tasks (Rust `clients/rust/tests/worker/src/tasks/retry.rs`, Python `clients/python/tests/e2e/tasks/retry.py`).
// The attempts are counted with marker files, which every child of the worker sees.

import { defineTask, h, type Horsies } from "../../../../horsies/src/index.ts";
import { markerErrorText, nextAttempt } from "../markers.ts";

/** The retry codes of this module. */
const RETRY_CODES = ["TRANSIENT", "RETRYABLE_ERROR", "MARKER_FAILED"] as const;

/** The attempt of `e2e_retry_success` that succeeds. */
export const SUCCESS_ATTEMPT = 3;

/** Always `TRANSIENT`; three retries 1 s apart. */
export const retryExhausted = defineTask({
  name: "e2e_retry_exhausted",
  input: {},
  output: h.string(),
  errors: RETRY_CODES,
  retry: { kind: "fixed", intervalsSeconds: [1, 1, 1], jitter: false, autoRetryFor: ["TRANSIENT"] },
});

/** `TRANSIENT` on the first two attempts of `token`, then `succeeded_on_attempt_3`. */
export const retrySuccess = defineTask({
  name: "e2e_retry_success",
  input: { token: h.string() },
  output: h.string(),
  errors: RETRY_CODES,
  retry: { kind: "fixed", intervalsSeconds: [1, 1, 1], jitter: false, autoRetryFor: ["TRANSIENT"] },
});

/** Always `TRANSIENT`; three retries 1 s, 2 s and 4 s apart. */
export const exponential = defineTask({
  name: "e2e_exponential",
  input: {},
  output: h.string(),
  errors: RETRY_CODES,
  retry: { kind: "exponential", baseSeconds: 1, maxRetries: 3, jitter: false, autoRetryFor: ["TRANSIENT"] },
});

/** Always `RETRYABLE_ERROR`; one retry. */
export const retryErrorCode = defineTask({
  name: "e2e_retry_error_code",
  input: {},
  output: h.string(),
  errors: RETRY_CODES,
  retry: { kind: "fixed", intervalsSeconds: [1], jitter: false, autoRetryFor: ["RETRYABLE_ERROR"] },
});

/** A policy that names `TRANSIENT`, and an error with another code: no retry. */
export const retryOtherCode = defineTask({
  name: "e2e_retry_other_code",
  input: {},
  output: h.string(),
  errors: RETRY_CODES,
  retry: { kind: "fixed", intervalsSeconds: [1, 1], jitter: false, autoRetryFor: ["TRANSIENT"] },
});

/** Always `TRANSIENT`, with 60 s between retries: a retry would start after a near `goodUntil`. */
export const retryLongInterval = defineTask({
  name: "e2e_retry_long_interval",
  input: {},
  output: h.string(),
  errors: RETRY_CODES,
  retry: { kind: "fixed", intervalsSeconds: [60, 60, 60], jitter: false, autoRetryFor: ["TRANSIENT"] },
});

/** Every contract of this module. */
export const RETRY_CONTRACTS = [retryExhausted, retrySuccess, exponential, retryErrorCode, retryOtherCode, retryLongInterval] as const;

/** The implementations of this module on `app`. */
export function implementRetry(app: Horsies): void {
  app.implement(retryExhausted, (_input, { err }) => err({ code: "TRANSIENT", message: "always fails" }));
  app.implement(retrySuccess, ({ token }, { ok, err }) => {
    const attempt = nextAttempt(token);

    if (!attempt.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(attempt.error) });
    }

    return attempt.value < SUCCESS_ATTEMPT
      ? err({ code: "TRANSIENT", message: `attempt ${String(attempt.value)}` })
      : ok(`succeeded_on_attempt_${String(attempt.value)}`);
  });
  app.implement(exponential, (_input, { err }) => err({ code: "TRANSIENT", message: "fail" }));
  app.implement(retryErrorCode, (_input, { err }) => err({ code: "RETRYABLE_ERROR", message: "Retryable error" }));
  app.implement(retryOtherCode, (_input, { err }) => err({ code: "RETRYABLE_ERROR", message: "not in autoRetryFor" }));
  app.implement(retryLongInterval, (_input, { err }) => err({ code: "TRANSIENT", message: "always fails" }));
}
