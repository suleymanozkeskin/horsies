// The refusals of `decodeEvent`: one test per refusal kind and per cause, each with the path to the
// refused item, as the core's decoder gives them.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { decodeEvent } from "../../../horsies/src/_catalog/events.ts";
import type { EventDecodeCause, EventDecodeError } from "../../../horsies/src/_catalog/events.ts";
import type { WireJson } from "../../../horsies/src/_catalog/wire_json.ts";
import { withItem } from "./support/wire-items.ts";

/** The items of the `task_claimed` vector of `core/tests/fixtures/event_wire/vectors.json`. */
const claimed: ReadonlyArray<WireJson> = [
  "TaskClaimed",
  1_700_000_000_123_456_789n,
  "0192f6a1-7b3c-7d4e-8f00-0123456789ab",
  "send_email",
  "default",
  "SHARED_ASYNC",
  2n,
  50n,
  1_500_000n,
];

/** The items of the `attempt_ended_not_known` vector. */
const endedNotKnown: ReadonlyArray<WireJson> = [
  "AttemptEnded",
  1_700_000_000_123_456_789n,
  "0192f6a1-7b3c-7d4e-8f00-0123456789ab",
  "send_email",
  "default",
  "ISOLATED",
  1n,
  ["NotKnown", 4242n],
  60_000_000_000n,
];

/** The item positions of the `task_claimed` vector. */
const AT = 1;
const TASK_ID = 2;
const TASK_NAME = 3;
const EXECUTION = 5;
const ATTEMPT = 6;
const PRIORITY = 7;
const QUEUE_WAIT = 8;

/** The item position of `end` in an `AttemptEnded` vector. */
const END = 7;

/** The refusal of `items`; the test fails when the items decode. */
function refusal(items: ReadonlyArray<WireJson>): EventDecodeError {
  const result = decodeEvent(items);

  assert(!result.ok, "the items are refused");

  return result.error;
}

/** The refusal of a `TaskClaimed` event with `item` at `index`. */
function claimedRefusal(index: number, item: WireJson): EventDecodeError {
  return refusal(withItem(claimed, index, item));
}

/** The refusal of an `AttemptEnded` event whose `end` item is `end`. */
function endRefusal(end: WireJson): EventDecodeError {
  return refusal(withItem(endedNotKnown, END, end));
}

function malformed(
  event: "TaskClaimed" | "AttemptEnded",
  path: ReadonlyArray<string>,
  cause: EventDecodeCause,
): EventDecodeError {
  return { kind: "malformedEvent", event, path, cause };
}

void describe("decodeEvent refuses items that are not an event", () => {
  for (const items of [[], [1n, 2n], [null], [["TaskClaimed"]], [1.5]]) {
    void test(`${items.length} items whose first item is no string`, () => {
      assert.deepEqual(refusal(items), { kind: "notAnEvent" });
    });
  }
});

void describe("decodeEvent refuses an unknown event type", () => {
  void test("the tag is named", () => {
    assert.deepEqual(refusal(["TaskStarted", 1n]), { kind: "unknownEventType", tag: "TaskStarted" });
  });

  void test("a long tag is cut to 64 characters, counted as Unicode scalar values", () => {
    assert.deepEqual(refusal(["X".repeat(80)]), { kind: "unknownEventType", tag: "X".repeat(64) });
    assert.deepEqual(refusal(["😀".repeat(70)]), { kind: "unknownEventType", tag: "😀".repeat(64) });
  });
});

void describe("decodeEvent refuses a malformed event with the path to the item", () => {
  void test("a missing or an extra item is an item count refusal", () => {
    assert.deepEqual(
      refusal(claimed.slice(0, -1)),
      malformed("TaskClaimed", [], { kind: "itemCount", expected: 8, found: 7 }),
    );
    assert.deepEqual(
      refusal([...claimed, 1n]),
      malformed("TaskClaimed", [], { kind: "itemCount", expected: 8, found: 9 }),
    );
  });

  void test("an item of another JSON kind is refused with the kind it needs", () => {
    const cases: ReadonlyArray<readonly [number, WireJson, string, EventDecodeCause]> = [
      [ATTEMPT, "2", "attempt", { kind: "wrongJsonKind", expected: "wholeNumber" }],
      [ATTEMPT, true, "attempt", { kind: "wrongJsonKind", expected: "wholeNumber" }],
      [ATTEMPT, 2, "attempt", { kind: "wrongJsonKind", expected: "wholeNumber" }],
      [QUEUE_WAIT, 1.5, "queue_wait", { kind: "wrongJsonKind", expected: "wholeNumber" }],
      [TASK_NAME, 7n, "task_name", { kind: "wrongJsonKind", expected: "text" }],
      [EXECUTION, null, "execution", { kind: "wrongJsonKind", expected: "text" }],
      [TASK_ID, new Map(), "task_id", { kind: "wrongJsonKind", expected: "text" }],
    ];

    for (const [index, item, field, cause] of cases) {
      assert.deepEqual(claimedRefusal(index, item), malformed("TaskClaimed", [field], cause));
    }

    assert.deepEqual(
      endRefusal("NotKnown"),
      malformed("AttemptEnded", ["end"], { kind: "wrongJsonKind", expected: "array" }),
    );
  });

  void test("a number outside its bound is refused with the bound", () => {
    const cases: ReadonlyArray<readonly [number, bigint, string, bigint, bigint]> = [
      [ATTEMPT, 0n, "attempt", 1n, 2_147_483_648n],
      [PRIORITY, 101n, "priority", 1n, 100n],
      [QUEUE_WAIT, -1n, "queue_wait", 0n, 18_446_744_073_709_551_615n],
      [QUEUE_WAIT, 2n ** 64n, "queue_wait", 0n, 18_446_744_073_709_551_615n],
      [AT, 2n ** 63n, "at", -(2n ** 63n), 2n ** 63n - 1n],
    ];

    for (const [index, item, field, min, max] of cases) {
      assert.deepEqual(
        claimedRefusal(index, item),
        malformed("TaskClaimed", [field], { kind: "outOfRange", min, max }),
      );
    }
  });

  void test("a text over its bound is refused; the bound counts Unicode scalar values", () => {
    assert.deepEqual(
      claimedRefusal(TASK_NAME, "é".repeat(256)),
      malformed("TaskClaimed", ["task_name"], { kind: "textOverBound", chars: 256, bound: 255 }),
    );
    assert.deepEqual(
      claimedRefusal(TASK_NAME, "😀".repeat(256)),
      malformed("TaskClaimed", ["task_name"], { kind: "textOverBound", chars: 256, bound: 255 }),
    );

    const atBound = decodeEvent(withItem(claimed, TASK_NAME, "😀".repeat(255)));

    assert(atBound.ok && atBound.value.kind === "TaskClaimed");
    assert.equal(atBound.value.taskName, "😀".repeat(255));
  });

  void test("a UUID in another form is refused", () => {
    for (const text of [
      "0192F6A1-7B3C-7D4E-8F00-0123456789AB",
      "{0192f6a1-7b3c-7d4e-8f00-0123456789ab}",
      "0192f6a17b3c7d4e8f000123456789ab",
      "0192f6a1-7b3c-7d4e-8f00-0123456789ab\n",
      "not a uuid",
    ]) {
      assert.deepEqual(
        claimedRefusal(TASK_ID, text),
        malformed("TaskClaimed", ["task_id"], { kind: "notCanonicalUuid" }),
      );
    }
  });

  void test("a name outside a closed type is refused with the type and the cut name", () => {
    assert.deepEqual(
      claimedRefusal(EXECUTION, "SHARED"),
      malformed("TaskClaimed", ["execution"], { kind: "unknownName", typeName: "Execution", found: "SHARED" }),
    );
    assert.deepEqual(
      claimedRefusal(EXECUTION, "Y".repeat(70)),
      malformed("TaskClaimed", ["execution"], {
        kind: "unknownName",
        typeName: "Execution",
        found: "Y".repeat(64),
      }),
    );
    assert.deepEqual(
      endRefusal(["EndedSomehow"]),
      malformed("AttemptEnded", ["end"], { kind: "unknownName", typeName: "AttemptEnd", found: "EndedSomehow" }),
    );
    assert.deepEqual(
      endRefusal(["ReportedByChild", ["Failed", ["BuiltIn", "NOT_A_CODE"]], ["NotReported"]]),
      malformed(
        "AttemptEnded",
        ["end", "ReportedByChild", "result", "Failed", "error_code", "BuiltIn", "code"],
        { kind: "unknownName", typeName: "BuiltInTaskCode", found: "NOT_A_CODE" },
      ),
    );
  });

  void test("a variant value without its tag is refused", () => {
    for (const end of [[], [42n]]) {
      assert.deepEqual(
        endRefusal(end),
        malformed("AttemptEnded", ["end"], { kind: "noVariantTag", typeName: "AttemptEnd" }),
      );
    }
  });

  void test("a variant with a missing or an extra item is refused inside its tag", () => {
    assert.deepEqual(
      endRefusal(["NotKnown"]),
      malformed("AttemptEnded", ["end", "NotKnown"], { kind: "itemCount", expected: 1, found: 0 }),
    );
    assert.deepEqual(
      endRefusal(["ReportedByChild", ["TimedOut", "extra"], ["NotReported"]]),
      malformed("AttemptEnded", ["end", "ReportedByChild", "result", "TimedOut"], {
        kind: "itemCount",
        expected: 0,
        found: 1,
      }),
    );
  });

  void test("an internal field is checked: a bad internal item refuses the event", () => {
    const end: ReadonlyArray<WireJson> = [
      "DecidedByWorker",
      "CHILD_LOST",
      "WORKER_CRASHED",
      ["NoCarrier"],
      1_700_000_000_000_000_000n,
      "ISOLATED",
      "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      "WORKFLOW_NOT_KNOWN",
    ];

    assert(decodeEvent(withItem(endedNotKnown, END, end)).ok);
    assert.deepEqual(
      endRefusal(withItem(end, 7, "IN_WORKFLOW")),
      malformed("AttemptEnded", ["end", "DecidedByWorker", "workflow"], {
        kind: "unknownName",
        typeName: "WorkflowPlace",
        found: "IN_WORKFLOW",
      }),
    );
  });

  void test("a refusal is frozen", () => {
    const refused = endRefusal(["NotKnown"]);

    assert(refused.kind === "malformedEvent");
    assert(Object.isFrozen(refused));
    assert(Object.isFrozen(refused.path));
    assert(Object.isFrozen(refused.cause));
  });
});
