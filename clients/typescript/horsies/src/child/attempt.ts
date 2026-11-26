// The attempt number of a run: the value of its fencing token, from 1, greater for each later attempt of the task.
// Only `AttemptNumber.parse` (or the child, from the core's token) makes one.

import { MAX_ATTEMPT } from "../_catalog/event_bounds.ts";
import { err, ok, type Result } from "../wire/result.ts";

declare const attemptBrand: unique symbol;

/** The attempt number of a run: a whole number from 1 to `AttemptNumber.MAX`. */
export type AttemptNumber = number & { readonly [attemptBrand]: "AttemptNumber" };

/** Why a number is not an attempt number. `reason` names the rule. */
export type AttemptRefused = { readonly kind: "notAnAttempt"; readonly reason: string };

/** The smallest attempt number: the first attempt of a task. */
const MIN_ATTEMPT = 1;

/** The attempt number of `value`, or why it is not one. Pure. */
function parseAttempt(value: number): Result<AttemptNumber, AttemptRefused> {
  if (!Number.isSafeInteger(value) || value < MIN_ATTEMPT || value > MAX_ATTEMPT) {
    return err(
      Object.freeze({
        kind: "notAnAttempt",
        reason: `an attempt number is a whole number from ${String(MIN_ATTEMPT)} to ${String(MAX_ATTEMPT)}`,
      }),
    );
  }

  // SAFETY: the brand is the fact that the number is a whole number from 1 to MAX_ATTEMPT, checked above.
  return ok(value as AttemptNumber);
}

/** The parser and the bounds of attempt numbers. */
export const AttemptNumber: {
  readonly parse: (value: number) => Result<AttemptNumber, AttemptRefused>;
  readonly MIN: number;
  readonly MAX: number;
} = Object.freeze({ parse: parseAttempt, MIN: MIN_ATTEMPT, MAX: MAX_ATTEMPT });

/** The attempt number that the core gave a run. Another value is a defect: `TypeError`. */
export function attemptFromCore(value: number, subject: string): AttemptNumber {
  const parsed = parseAttempt(value);

  if (!parsed.ok) {
    throw new TypeError(`${subject}: the core gave an attempt number outside its bounds`);
  }

  return parsed.value;
}
