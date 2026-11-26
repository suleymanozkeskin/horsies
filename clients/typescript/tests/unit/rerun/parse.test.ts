// The parse of a rerun command and policy: the records that the addon takes, and each refusal.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { parseRerunRequest } from "../../../horsies/src/rerun/parse.ts";
import type { RerunCommand, RerunPolicy } from "../../../horsies/src/rerun/types.ts";
import { COMMAND, POLICY, SOURCE_TASK_ID } from "./support.ts";

/** The refusal of a request: its code and its message. */
function refusalOf(command: RerunCommand, policy: RerunPolicy): { readonly code: string; readonly message: string } {
  const parsed = parseRerunRequest(command, policy);

  assert.ok(!parsed.ok, "the request is refused");

  return { code: parsed.error.code, message: parsed.error.message };
}

/** The policy with the window `window`. */
function withWindow(window: Temporal.DurationLike): RerunPolicy {
  return { ...POLICY, reservationWindow: Temporal.Duration.from(window) };
}

describe("parseRerunRequest accepts", () => {
  test("an unkeyed command with no deadline and a policy of days", () => {
    assert.deepEqual(parseRerunRequest(COMMAND, POLICY), {
      ok: true,
      value: {
        command: { sourceTaskId: SOURCE_TASK_ID, deadline: { kind: "noDeadline" }, callerKey: { kind: "unkeyed" } },
        policy: {
          retention: { kind: "days", days: 7 },
          rerunInput: "keep",
          reservationWindowMicros: 86_400_000_000n,
        },
      },
    });
  });

  test("a keyed command with a deadline in epoch microseconds", () => {
    const command: RerunCommand = {
      sourceTaskId: SOURCE_TASK_ID,
      deadline: { kind: "until", at: Temporal.Instant.from("2026-11-26T09:10:12.654321Z") },
      callerKey: "order-7",
    };

    const parsed = parseRerunRequest(command, POLICY);

    assert.ok(parsed.ok);
    assert.deepEqual(parsed.value.command.deadline, { kind: "until", epochMicros: 1_795_684_212_654_321n });
    assert.deepEqual(parsed.value.command.callerKey, { kind: "keyed", key: "order-7" });
  });

  test("a deadline with a part below the microsecond, cut toward the past", () => {
    const after = parseRerunRequest(
      { ...COMMAND, deadline: { kind: "until", at: Temporal.Instant.fromEpochNanoseconds(1_500n) } },
      POLICY,
    );

    const before = parseRerunRequest(
      { ...COMMAND, deadline: { kind: "until", at: Temporal.Instant.fromEpochNanoseconds(-1_500n) } },
      POLICY,
    );

    assert.ok(after.ok && before.ok);
    assert.deepEqual(after.value.command.deadline, { kind: "until", epochMicros: 1n });
    assert.deepEqual(before.value.command.deadline, { kind: "until", epochMicros: -2n });
  });

  test("the first and the last deadline that the bounds hold", () => {
    for (const at of ["0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999999Z"]) {
      const parsed = parseRerunRequest({ ...COMMAND, deadline: { kind: "until", at: Temporal.Instant.from(at) } }, POLICY);

      assert.ok(parsed.ok, at);
    }
  });

  test("retention forever, the input dropped, and the window bounds", () => {
    const policy: RerunPolicy = { retention: { kind: "forever" }, rerunInput: "drop", reservationWindow: Temporal.Duration.from({ microseconds: 1 }) };
    const shortest = parseRerunRequest(COMMAND, policy);
    const longest = parseRerunRequest(COMMAND, withWindow({ days: 30 }));

    assert.deepEqual(shortest.ok && shortest.value.policy, {
      retention: { kind: "forever" },
      rerunInput: "drop",
      reservationWindowMicros: 1n,
    });
    assert.deepEqual(longest.ok && longest.value.policy.reservationWindowMicros, 2_592_000_000_000n);
  });

  test("a window of mixed units, as their sum", () => {
    const parsed = parseRerunRequest(COMMAND, withWindow({ hours: 1, minutes: 2, seconds: 3, milliseconds: 4, microseconds: 5, nanoseconds: 6000 }));

    assert.deepEqual(parsed.ok && parsed.value.policy.reservationWindowMicros, 3_723_004_011n);
  });

  test("the shortest and the longest retention period", () => {
    for (const days of [1, 1825]) {
      const parsed = parseRerunRequest(COMMAND, { ...POLICY, retention: { kind: "days", days } });

      assert.ok(parsed.ok, `${days}`);
    }
  });
});

describe("parseRerunRequest refuses with INVALID_COMMAND", () => {
  test("a deadline before the year 1 or after the year 9999", () => {
    for (const at of ["0000-12-31T23:59:59.999999Z", "+010000-01-01T00:00:00Z"]) {
      const refusal = refusalOf({ ...COMMAND, deadline: { kind: "until", at: Temporal.Instant.from(at) } }, POLICY);

      assert.deepEqual(refusal, {
        code: "INVALID_COMMAND",
        message: "deadline.at must be from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999Z",
      });
    }
  });

  test("a caller key with a lone surrogate, which would not reach the core as written", () => {
    assert.deepEqual(refusalOf({ ...COMMAND, callerKey: "key-\uD800" }, POLICY), {
      code: "INVALID_COMMAND",
      message: "callerKey must be well-formed text: it holds a lone UTF-16 surrogate",
    });
  });
});

describe("parseRerunRequest refuses with INVALID_POLICY", () => {
  test("a retention period that is not a whole number of days from 1 to 1825", () => {
    for (const days of [0, 1826, 7.5, -1, Number.NaN]) {
      assert.deepEqual(refusalOf(COMMAND, { ...POLICY, retention: { kind: "days", days } }), {
        code: "INVALID_POLICY",
        message: "retention.days must be a whole number from 1 to 1825",
      });
    }
  });

  test("a window of 0 and a window of 31 days", () => {
    for (const window of [{ seconds: 0 }, { days: 31 }, { days: 30, microseconds: 1 }, { hours: -1 }]) {
      assert.deepEqual(refusalOf(COMMAND, withWindow(window)), {
        code: "INVALID_POLICY",
        message: "reservationWindow must be more than 0 and at most 30 days (2592000000000 microseconds)",
      });
    }
  });

  test("a window with a part below the microsecond", () => {
    assert.deepEqual(refusalOf(COMMAND, withWindow({ seconds: 1, nanoseconds: 1 })), {
      code: "INVALID_POLICY",
      message: "reservationWindow must be a whole number of microseconds",
    });
  });

  test("a window in years, months or weeks", () => {
    for (const window of [{ years: 1 }, { months: 1 }, { weeks: 1 }]) {
      assert.equal(refusalOf(COMMAND, withWindow(window)).message, "reservationWindow must use days and smaller units: a year, a month or a week has no fixed length");
    }
  });

  test("the command first: a refused command hides a refused policy", () => {
    const lateDeadline = { kind: "until", at: Temporal.Instant.from("+010000-01-01T00:00:00Z") } as const;

    assert.equal(refusalOf({ ...COMMAND, deadline: lateDeadline }, withWindow({ seconds: 0 })).code, "INVALID_COMMAND");
  });
});
