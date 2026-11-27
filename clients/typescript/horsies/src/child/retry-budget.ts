// The retry facts of a run: the retry budget of the task (`MaxRetries`) and the retries that are left after the
// attempt (`RetriesLeft`). Both are whole numbers from 0 to `MAX_TASK_RETRIES`, the bound that the task table holds.

import { MAX_TASK_RETRIES } from "../_catalog/event_bounds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { AttemptNumber } from "./attempt.ts";

declare const maxRetriesBrand: unique symbol;
declare const retriesLeftBrand: unique symbol;

/** The retry budget of a task: the retries that follow its first attempt, a whole number from 0 to `MaxRetries.MAX`. */
export type MaxRetries = number & { readonly [maxRetriesBrand]: "MaxRetries" };

/** The retries that a task has left after an attempt: a whole number from 0 to `MaxRetries.MAX`. */
export type RetriesLeft = number & { readonly [retriesLeftBrand]: "RetriesLeft" };

/** Why a number is not a retry budget. `reason` names the rule. */
export type MaxRetriesRefused = { readonly kind: "notARetryBudget"; readonly reason: string };

/** The smallest retry budget: the task is not retried. */
const MIN_RETRIES = 0;

/** The retry budget of `value`, or why it is not one. Pure. */
function parseMaxRetries(value: number): Result<MaxRetries, MaxRetriesRefused> {
  if (!Number.isSafeInteger(value) || value < MIN_RETRIES || value > MAX_TASK_RETRIES) {
    return err(
      Object.freeze({
        kind: "notARetryBudget",
        reason: `a retry budget is a whole number from ${String(MIN_RETRIES)} to ${String(MAX_TASK_RETRIES)}`,
      }),
    );
  }

  // SAFETY: the brand is the fact that the number is a whole number from 0 to MAX_TASK_RETRIES, checked above.
  return ok(value as MaxRetries);
}

/** The parser and the bounds of retry budgets. */
export const MaxRetries: {
  readonly parse: (value: number) => Result<MaxRetries, MaxRetriesRefused>;
  readonly MIN: number;
  readonly MAX: number;
  /** The budget of a task that is not retried: the default of `TaskRuntime.forTest`. */
  readonly NONE: MaxRetries;
} = Object.freeze({
  parse: parseMaxRetries,
  MIN: MIN_RETRIES,
  MAX: MAX_TASK_RETRIES,
  // SAFETY: 0 is inside the bounds of the brand.
  NONE: MIN_RETRIES as MaxRetries,
});

/** The retry budget that the core gave a run. Another value is a defect: `TypeError`. */
export function maxRetriesFromCore(value: number, subject: string): MaxRetries {
  const parsed = parseMaxRetries(value);

  if (!parsed.ok) {
    throw new TypeError(`${subject}: the core gave a retry budget outside its bounds`);
  }

  return parsed.value;
}

/** The retries left after `attempt` with the budget `maxRetries`: `maxRetries - (attempt - 1)`, and 0 past the budget. Pure. */
export function retriesLeftOf(maxRetries: MaxRetries, attempt: AttemptNumber): RetriesLeft {
  const used = attempt - AttemptNumber.MIN;

  // SAFETY: the difference of a number from 0 to MAX_TASK_RETRIES and a number from 0, floored at 0, stays in the same
  // range.
  return Math.max(MIN_RETRIES, maxRetries - used) as RetriesLeft;
}
