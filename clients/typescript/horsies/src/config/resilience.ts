// The resilience settings of the config: the retry of transient database errors in a worker, and the poll interval
// of the notification listener (the core's `WorkerResilienceConfig`, `core/src/core/config/resilience.rs`).

import { unreachable } from "../wire/unreachable.ts";
import {
  accepted,
  type Checked,
  checkedInteger,
  checkedObject,
  type ConfigJson,
  type IntegerBound,
  memberPath,
  optionalEntry,
  U64_FIELD,
} from "./checked.ts";

/** The most retries of a transient database error: a count, or no limit. */
export type RetryAttempts = { readonly kind: "attempts"; readonly attempts: number } | { readonly kind: "noLimit" };

/** The resilience settings. Each field left out has the core's default; the core checks each range. */
export interface ResilienceConfigInput {
  readonly dbRetryInitialMs?: number;
  readonly dbRetryMaxMs?: number;
  readonly dbRetryMaxAttempts?: RetryAttempts;
  readonly notifyPollIntervalMs?: number;
}

/** The core's value of "no limit" in `db_retry_max_attempts`. */
const NO_ATTEMPT_LIMIT = 0;

/**
 * A count of retries: at least 1, because the core reads `0` as no limit (`RetryAttempts` gives that state its own
 * variant).
 */
const ATTEMPT_COUNT: IntegerBound = Object.freeze({ min: 1, max: U64_FIELD.max, code: "FIELD_BOUND" });

/** The core's `resilience` object of `input`. Pure. */
export function resilienceJson(input: ResilienceConfigInput, path: string): Checked<ConfigJson> {
  return checkedObject([
    optionalEntry("db_retry_initial_ms", input.dbRetryInitialMs, (value) =>
      checkedInteger(value, U64_FIELD, memberPath(path, "dbRetryInitialMs")),
    ),
    optionalEntry("db_retry_max_ms", input.dbRetryMaxMs, (value) =>
      checkedInteger(value, U64_FIELD, memberPath(path, "dbRetryMaxMs")),
    ),
    optionalEntry("db_retry_max_attempts", input.dbRetryMaxAttempts, (attempts) =>
      attemptsJson(attempts, memberPath(path, "dbRetryMaxAttempts.attempts")),
    ),
    optionalEntry("notify_poll_interval_ms", input.notifyPollIntervalMs, (value) =>
      checkedInteger(value, U64_FIELD, memberPath(path, "notifyPollIntervalMs")),
    ),
  ]);
}

function attemptsJson(attempts: RetryAttempts, path: string): Checked<ConfigJson> {
  switch (attempts.kind) {
    case "attempts":
      return checkedInteger(attempts.attempts, ATTEMPT_COUNT, path);
    case "noLimit":
      return accepted(NO_ATTEMPT_LIMIT);
    default:
      return unreachable(attempts, "attemptsJson");
  }
}
