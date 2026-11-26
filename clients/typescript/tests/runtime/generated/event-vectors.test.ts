// `decodeEvent` on the core's event wire vectors: each vector decodes to the event that the core
// encoded, with exact `Temporal` values on Node and on Bun.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { decodeEvent, EVENT_CONTRACT_VERSION, EVENT_TYPES } from "../../../horsies/src/_catalog/events.ts";
import type {
  AnyEvent,
  AttemptEnd,
  AttemptEnded,
  AttemptRecord,
  AttemptRecorded,
  Execution,
} from "../../../horsies/src/_catalog/events.ts";
import type { WireJson } from "../../../horsies/src/_catalog/wire_json.ts";
import { refusalText, withItem } from "./support/wire-items.ts";

interface ReviverContext {
  readonly source?: string;
}

/** One vector: the core's wire form of one event. */
interface EventVector {
  readonly name: string;
  readonly wire: ReadonlyArray<WireJson>;
}

/** The vector file. */
interface EventVectorFile {
  readonly contract_version: bigint;
  readonly vectors: ReadonlyArray<EventVector>;
}

const VECTORS_URL = new URL("../../../../../core/tests/fixtures/event_wire/vectors.json", import.meta.url);

/** The source text of a JSON integer: a JSON number with no fraction and no exponent. */
const INTEGER_SOURCE = /^-?\d+$/u;

/** The vectors' time, 1700000000123456789 ns since the epoch. */
const AT = Temporal.Instant.fromEpochNanoseconds(1_700_000_000_123_456_789n);
const STARTED_AT = Temporal.Instant.fromEpochNanoseconds(1_700_000_000_100_000_000n);
const NEXT_RETRY_AT = Temporal.Instant.fromEpochNanoseconds(1_700_000_060_000_000_000n);
const TASK_ID = "0192f6a1-7b3c-7d4e-8f00-0123456789ab";
const LARGEST_DURATION_NANOSECONDS = 18_446_744_073_709_551_615n;
const NANOSECONDS_PER_SECOND = 1_000_000_000n;
const NANOSECONDS_PER_MILLISECOND = 1_000_000n;
const NANOSECONDS_PER_MICROSECOND = 1000n;

function ended(
  execution: Execution,
  attempt: number,
  end: AttemptEnd,
  runDuration: Temporal.Duration,
): AttemptEnded {
  return {
    kind: "AttemptEnded",
    at: AT,
    taskId: TASK_ID,
    taskName: "send_email",
    queue: "default",
    execution,
    attempt,
    end,
    runDuration,
  };
}

function recorded(attempt: number, record: AttemptRecord, recordDuration: Temporal.Duration): AttemptRecorded {
  return {
    kind: "AttemptRecorded",
    at: AT,
    taskId: TASK_ID,
    taskName: "send_email",
    queue: "default",
    attempt,
    record,
    recordDuration,
  };
}

/** The event of each vector, by the vector's name. */
const EXPECTED: ReadonlyMap<string, AnyEvent> = new Map<string, AnyEvent>([
  [
    "task_claimed",
    {
      kind: "TaskClaimed",
      at: AT,
      taskId: TASK_ID,
      taskName: "send_email",
      queue: "default",
      execution: "SHARED_ASYNC",
      attempt: 2,
      priority: 50,
      queueWait: Temporal.Duration.from({ milliseconds: 1, microseconds: 500 }),
    },
  ],
  [
    "claim_released",
    {
      kind: "ClaimReleased",
      at: AT,
      taskId: TASK_ID,
      taskName: "send_email",
      queue: "default",
      reason: "QUEUE_FULL",
    },
  ],
  [
    "attempt_placed",
    {
      kind: "AttemptPlaced",
      at: AT,
      taskId: TASK_ID,
      taskName: "send_email",
      queue: "default",
      execution: "SHARED_ASYNC",
      attempt: 1,
      childKind: "ISOLATED",
      childPid: 4242,
      slotWait: Temporal.Duration.from({ microseconds: 250 }),
    },
  ],
  [
    "attempt_not_started",
    {
      kind: "AttemptNotStarted",
      at: AT,
      taskId: TASK_ID,
      taskName: "send_email",
      queue: "default",
      attempt: 1,
      reason: "WORKFLOW_STOPPED",
    },
  ],
  [
    "attempt_ended_completed",
    ended(
      "SHARED_THREAD",
      1,
      {
        kind: "reportedByChild",
        result: { kind: "completed" },
        start: { kind: "reported", startedAt: STARTED_AT },
      },
      Temporal.Duration.from({ seconds: 2 }),
    ),
  ],
  [
    "attempt_ended_user_code",
    ended(
      "SHARED_ASYNC",
      2,
      {
        kind: "reportedByChild",
        result: { kind: "failed", errorCode: { kind: "user", code: "RATE_LIMITED" } },
        start: { kind: "reported", startedAt: STARTED_AT },
      },
      Temporal.Duration.from({ seconds: 2 }),
    ),
  ],
  [
    "attempt_ended_built_in_code",
    ended(
      "ISOLATED",
      1,
      {
        kind: "reportedByChild",
        result: { kind: "failed", errorCode: { kind: "library", code: "TASK_EXCEPTION" } },
        start: { kind: "notReported" },
      },
      Temporal.Duration.from({ milliseconds: 5 }),
    ),
  ],
  [
    "attempt_ended_worker_failure",
    ended(
      "ISOLATED",
      1,
      {
        kind: "reportedByChild",
        result: { kind: "workerFailure" },
        start: { kind: "notReported" },
      },
      Temporal.Duration.from({ milliseconds: 5 }),
    ),
  ],
  [
    "attempt_ended_timed_out_in_child",
    ended(
      "SHARED_ASYNC",
      3,
      {
        kind: "reportedByChild",
        result: { kind: "timedOut" },
        start: { kind: "reported", startedAt: STARTED_AT },
      },
      Temporal.Duration.from({ seconds: 30 }),
    ),
  ],
  [
    "attempt_ended_by_worker",
    ended(
      "SHARED_ASYNC",
      3,
      { kind: "decidedByWorker", reason: "TIMED_OUT" },
      Temporal.Duration.from({ seconds: 30 }),
    ),
  ],
  [
    "attempt_ended_by_worker_without_carrier",
    ended(
      "SHARED_ASYNC",
      1,
      { kind: "decidedByWorker", reason: "NEIGHBOUR_BLOCKED_LOOP" },
      Temporal.Duration.from({ seconds: 30 }),
    ),
  ],
  [
    "attempt_ended_by_worker_in_workflow",
    ended(
      "SHARED_THREAD",
      2,
      { kind: "decidedByWorker", reason: "CHILD_LOST" },
      Temporal.Duration.from({ seconds: 30 }),
    ),
  ],
  [
    "attempt_ended_not_known",
    ended("ISOLATED", 1, { kind: "notKnown", childPid: 4242 }, Temporal.Duration.from({ seconds: 60 })),
  ],
  [
    "attempt_recorded_retry",
    recorded(
      2,
      { kind: "retryScheduled", retryCount: 2, nextRetryAt: NEXT_RETRY_AT },
      Temporal.Duration.from({ milliseconds: 3 }),
    ),
  ],
  ["attempt_recorded_failed", recorded(3, { kind: "failed" }, Temporal.Duration.from({ milliseconds: 1 }))],
  [
    "task_recovered",
    {
      kind: "TaskRecovered",
      at: AT,
      taskId: TASK_ID,
      taskName: "send_email",
      queue: "default",
      action: "RUNNING_FAILED",
    },
  ],
  [
    "child_started",
    {
      kind: "ChildStarted",
      at: AT,
      childKind: "SHARED_THREAD",
      slot: 3,
      childPid: 4243,
      bootDuration: Temporal.Duration.from({ milliseconds: 850 }),
    },
  ],
  [
    "child_replaced",
    {
      kind: "ChildReplaced",
      at: AT,
      childKind: "ISOLATED",
      slot: 0,
      childPid: 4242,
      reason: "MEMORY_LIMIT",
    },
  ],
  [
    "slot_down",
    {
      kind: "SlotDown",
      at: AT,
      childKind: "SHARED_ASYNC",
      slot: 1,
      cause: { kind: "resourceShortage", errno: "EMFILE" },
    },
  ],
  ["slot_recovered", { kind: "SlotRecovered", at: AT, childKind: "SHARED_ASYNC", slot: 1 }],
]);

/**
 * A `JSON.parse` reviver: a JSON integer becomes the `bigint` of its source text, so a value above
 * 2^53 stays exact. `JSON.parse` gives a reviver untyped values; every other value stays as it is.
 */
function integerAsBigInt(_key: string, value: any, context?: ReviverContext): any {
  const source = context?.source;

  if (source !== undefined && INTEGER_SOURCE.test(source)) {
    return BigInt(source);
  }

  return value;
}

const vectorFile: EventVectorFile = JSON.parse(readFileSync(VECTORS_URL, "utf8"), integerAsBigInt);

/** The wire items of the vector `name`. */
function vectorWire(name: string): ReadonlyArray<WireJson> {
  const vector = vectorFile.vectors.find((candidate) => candidate.name === name);

  assert(vector !== undefined, `the vector file has the vector ${name}`);

  return vector.wire;
}

/**
 * The text of a decoded event. `Temporal` values write their exact ISO text, so two events with
 * the same text hold the same instants and durations.
 */
function eventText(event: AnyEvent): string {
  return JSON.stringify(event);
}

/** The event of `items`; the test fails when the items do not decode. */
function decoded(items: ReadonlyArray<WireJson>): AnyEvent {
  const result = decodeEvent(items);

  assert(result.ok, result.ok ? "" : refusalText(result.error));

  return result.value;
}

/** The nanoseconds of a duration whose largest unit is the second. */
function durationNanoseconds(duration: Temporal.Duration): bigint {
  return (
    BigInt(duration.seconds) * NANOSECONDS_PER_SECOND +
    BigInt(duration.milliseconds) * NANOSECONDS_PER_MILLISECOND +
    BigInt(duration.microseconds) * NANOSECONDS_PER_MICROSECOND +
    BigInt(duration.nanoseconds)
  );
}

/** The `queue_wait` duration of the `task_claimed` vector with `nanoseconds` in its place. */
function queueWait(nanoseconds: bigint): Temporal.Duration {
  const event = decoded(withItem(vectorWire("task_claimed"), 8, nanoseconds));

  assert(event.kind === "TaskClaimed");

  return event.queueWait;
}

void describe("decodeEvent on the event wire vectors", () => {
  void test("the vector file is of this contract version and covers every event type", () => {
    assert.equal(vectorFile.contract_version, BigInt(EVENT_CONTRACT_VERSION));
    assert.deepEqual(
      vectorFile.vectors.map((vector) => vector.name).toSorted(),
      [...EXPECTED.keys()].toSorted(),
    );
    assert.deepEqual(
      new Set(vectorFile.vectors.map((vector) => decoded(vector.wire).kind)),
      new Set(EVENT_TYPES),
    );
  });

  for (const vector of vectorFile.vectors) {
    void test(`${vector.name} decodes to the event that the core encoded`, () => {
      const expected = EXPECTED.get(vector.name);

      assert(expected !== undefined, `an expected event for ${vector.name}`);

      const event = decoded(vector.wire);

      assert.equal(eventText(event), eventText(expected));
      assert.deepEqual(Object.keys(event), Object.keys(expected));
      assert(Object.isFrozen(event));
    });
  }
});

void describe("the Temporal values of an event", () => {
  void test("a time keeps every nanosecond", () => {
    const event = decoded(vectorWire("task_claimed"));

    assert.equal(event.at.epochNanoseconds, 1_700_000_000_123_456_789n);
    assert.equal(event.at.toString(), "2023-11-14T22:13:20.123456789Z");
  });

  void test("the earliest and the latest time of the wire decode", () => {
    const items = vectorWire("slot_recovered");
    const earliest = decoded(withItem(items, 1, -9_223_372_036_854_775_808n));
    const latest = decoded(withItem(items, 1, 9_223_372_036_854_775_807n));

    assert.equal(earliest.at.toString(), "1677-09-21T00:12:43.145224192Z");
    assert.equal(latest.at.toString(), "2262-04-11T23:47:16.854775807Z");
  });

  void test("a duration has the second as its largest unit and keeps every nanosecond", () => {
    const wait = queueWait(1_500_000n);

    assert.deepEqual(
      [wait.hours, wait.minutes, wait.seconds, wait.milliseconds, wait.microseconds, wait.nanoseconds],
      [0, 0, 0, 1, 500, 0],
    );
    assert.equal(durationNanoseconds(queueWait(3_600_000_000_001n)), 3_600_000_000_001n);
  });

  void test("a duration above 2^53 nanoseconds keeps every nanosecond", () => {
    const aboveSafe = 9_007_199_254_740_993n;

    assert.equal(durationNanoseconds(queueWait(aboveSafe)), aboveSafe);

    const largest = queueWait(LARGEST_DURATION_NANOSECONDS);

    assert.deepEqual(
      [largest.seconds, largest.milliseconds, largest.microseconds, largest.nanoseconds],
      [18_446_744_073, 709, 551, 615],
    );
    assert.equal(durationNanoseconds(largest), LARGEST_DURATION_NANOSECONDS);
    assert.equal(largest.toString(), "PT18446744073.709551615S");
  });
});

void describe("the closed types of an event", () => {
  void test("a built-in error code has the kind library", () => {
    const event = decoded(vectorWire("attempt_ended_built_in_code"));

    assert(event.kind === "AttemptEnded");
    assert(event.end.kind === "reportedByChild");
    assert(event.end.result.kind === "failed");
    assert.deepEqual(event.end.result.errorCode, { kind: "library", code: "TASK_EXCEPTION" });
  });

  void test("an internal field is checked and is not a member", () => {
    const event = decoded(vectorWire("attempt_ended_by_worker"));

    assert(event.kind === "AttemptEnded");
    assert.deepEqual(Object.keys(event.end), ["kind", "reason"]);
  });

  void test("every record of an event is frozen", () => {
    const event = decoded(vectorWire("attempt_ended_user_code"));

    assert(event.kind === "AttemptEnded");
    assert(event.end.kind === "reportedByChild");
    assert(event.end.result.kind === "failed");
    assert(Object.isFrozen(event.end));
    assert(Object.isFrozen(event.end.result));
    assert(Object.isFrozen(event.end.result.errorCode));
    assert(Object.isFrozen(event.end.start));
  });
});
