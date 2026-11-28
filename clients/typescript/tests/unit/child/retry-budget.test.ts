// The retry facts of a run: `MaxRetries` (0 to `MAX_TASK_RETRIES`), the budget that the core gives a run, and
// `retriesLeft` (`maxRetries - (attempt - 1)`, 0 past the budget).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { MAX_TASK_RETRIES } from "../../../horsies/src/_catalog/event_bounds.ts";
import { AttemptNumber } from "../../../horsies/src/child/attempt.ts";
import { MaxRetries, maxRetriesFromCore, retriesLeftOf } from "../../../horsies/src/child/retry-budget.ts";

function attempt(value: number): AttemptNumber {
  const parsed = AttemptNumber.parse(value);

  return parsed.ok ? parsed.value : assert.fail(parsed.error.reason);
}

function budget(value: number): MaxRetries {
  const parsed = MaxRetries.parse(value);

  return parsed.ok ? parsed.value : assert.fail(parsed.error.reason);
}

describe("MaxRetries", () => {
  test("the bounds are 0 and the core's MAX_TASK_RETRIES (20)", () => {
    assert.deepEqual([MaxRetries.MIN, MaxRetries.MAX, MAX_TASK_RETRIES], [0, 20, 20]);
  });

  test("the numbers from 0 to 20 are retry budgets", () => {
    for (const value of [0, 1, 20]) {
      assert.deepEqual(MaxRetries.parse(value), { ok: true, value });
    }
  });

  test("a number below 0, above 20, or not a whole number is refused with the rule", () => {
    for (const value of [-1, 21, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      assert.deepEqual(MaxRetries.parse(value), {
        ok: false,
        error: { kind: "notARetryBudget", reason: "a retry budget is a whole number from 0 to 20" },
      });
    }
  });

  test("NONE is the budget 0", () => {
    assert.equal(MaxRetries.NONE, 0);
  });

  test("a budget outside the bounds from the core is a TypeError that names the subject", () => {
    assert.equal(maxRetriesFromCore(20, "runBody"), 20);
    assert.throws(() => maxRetriesFromCore(21, "runBody"), {
      name: "TypeError",
      message: "runBody: the core gave a retry budget outside its bounds",
    });
  });
});

describe("retriesLeftOf", () => {
  test("the first attempt holds the whole budget", () => {
    assert.equal(retriesLeftOf(budget(3), attempt(1)), 3);
  });

  test("each retry uses one", () => {
    assert.deepEqual([2, 3, 4].map((value) => retriesLeftOf(budget(3), attempt(value))), [2, 1, 0]);
  });

  test("past the budget it stays 0", () => {
    assert.equal(retriesLeftOf(budget(3), attempt(5)), 0);
    assert.equal(retriesLeftOf(MaxRetries.NONE, attempt(2)), 0);
  });

  test("a task that is not retried has none left on its first attempt", () => {
    assert.equal(retriesLeftOf(MaxRetries.NONE, attempt(1)), 0);
  });
});
