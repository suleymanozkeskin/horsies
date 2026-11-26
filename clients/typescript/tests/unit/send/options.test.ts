// The check of the send options: each refusal, the accepted forms, and the deadline cut toward the past.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { checkSendOptions, epochMicrosTowardPast, type SendOptions } from "../../../horsies/src/send/options.ts";

function refusalOf(options: SendOptions): readonly [string, string] | "accepted" {
  const checked = checkSendOptions(options);

  return checked.ok ? "accepted" : [checked.error.option, checked.error.detail];
}

describe("checkSendOptions: refusals", () => {
  test("an empty idempotency key", () => {
    assert.equal(refusalOf({ idempotencyKey: "" })[0], "idempotencyKey");
  });

  test("an idempotency key of 256 UTF-8 bytes, also when it has fewer characters", () => {
    assert.equal(refusalOf({ idempotencyKey: "k".repeat(256) })[0], "idempotencyKey");
    assert.equal(refusalOf({ idempotencyKey: "é".repeat(128) })[0], "idempotencyKey");
  });

  test("an idempotency key with a lone surrogate", () => {
    assert.deepEqual(refusalOf({ idempotencyKey: "key-\uD800" }), ["idempotencyKey", "the key holds a lone surrogate"]);
  });

  test("a goodUntil outside the years 1 to 9999 is a refusal, not a defect of the bridge", () => {
    for (const text of ["+275000-01-01T00:00:00Z", "0000-12-31T23:59:59Z", "+010000-01-01T00:00:00Z"]) {
      assert.equal(refusalOf({ goodUntil: Temporal.Instant.from(text) })[0], "goodUntil");
    }

    assert.equal(checkSendOptions({ goodUntil: Temporal.Instant.from("9999-12-31T23:59:59Z") }).ok, true);
  });

  test("a goodUntil that is not a Temporal.Instant", () => {
    // SAFETY: a JavaScript caller can pass a value that the static type refuses.
    const goodUntil = new Date() as unknown as Temporal.Instant;

    assert.equal(refusalOf({ goodUntil })[0], "goodUntil");
  });

  test("a delay that is not a whole number of seconds", () => {
    assert.equal(refusalOf({ delaySeconds: 1.5 })[0], "delaySeconds");
    assert.equal(refusalOf({ delaySeconds: Number.NaN })[0], "delaySeconds");
  });

  test("a delay outside 0 to 4,294,967,295 seconds", () => {
    assert.equal(refusalOf({ delaySeconds: -1 })[0], "delaySeconds");
    assert.equal(refusalOf({ delaySeconds: 4_294_967_296 })[0], "delaySeconds");
  });

  test("a retention of 0 days and of 1.5 days", () => {
    assert.equal(refusalOf({ retention: { kind: "days", days: 0 } })[0], "retention");
    assert.equal(refusalOf({ retention: { kind: "days", days: 1.5 } })[0], "retention");
  });

  test("the first refusal in field order is given", () => {
    assert.equal(refusalOf({ idempotencyKey: "", delaySeconds: -1 })[0], "idempotencyKey");
  });

  test("a refusal detail holds no option value", () => {
    const secret = "secret-key-value";
    const refused = refusalOf({ idempotencyKey: `${secret}-\uD800` });

    assert.equal(refused === "accepted" ? true : refused[1].includes(secret), false);
  });
});

describe("checkSendOptions: accepted forms", () => {
  test("no option: no key, no deadline, no delay, the queue's retention", () => {
    assert.deepEqual(checkSendOptions({}), {
      ok: true,
      value: {
        idempotencyKey: { kind: "none" },
        deadline: { kind: "noDeadline" },
        delay: { kind: "noDelay" },
        retention: { kind: "queueOrDefault" },
      },
    });
  });

  test("every option at its bound", () => {
    const goodUntil = Temporal.Instant.from("2026-08-12T09:10:12.654321Z");

    assert.deepEqual(
      checkSendOptions({
        idempotencyKey: "k".repeat(255),
        goodUntil,
        delaySeconds: 4_294_967_295,
        retention: { kind: "days", days: 1 },
      }),
      {
        ok: true,
        value: {
          idempotencyKey: { kind: "text", text: "k".repeat(255) },
          deadline: { kind: "until", epochMicros: 1_786_525_812_654_321n },
          delay: { kind: "seconds", seconds: 4_294_967_295 },
          retention: { kind: "days", days: 1 },
        },
      },
    );
  });

  test("a delay of 0 and a retention forever", () => {
    const checked = checkSendOptions({ delaySeconds: 0, retention: { kind: "forever" } });

    assert.deepEqual(checked.ok ? [checked.value.delay, checked.value.retention] : [], [
      { kind: "seconds", seconds: 0 },
      { kind: "forever" },
    ]);
  });
});

describe("epochMicrosTowardPast", () => {
  test("a part below the microsecond is cut toward the past", () => {
    assert.equal(epochMicrosTowardPast(Temporal.Instant.fromEpochNanoseconds(1_000_999n)), 1_000n);
    assert.equal(epochMicrosTowardPast(Temporal.Instant.fromEpochNanoseconds(1_000_000n)), 1_000n);
  });

  test("before 1970 the cut is toward the past too", () => {
    assert.equal(epochMicrosTowardPast(Temporal.Instant.fromEpochNanoseconds(-1n)), -1n);
    assert.equal(epochMicrosTowardPast(Temporal.Instant.fromEpochNanoseconds(-1_001n)), -2n);
    assert.equal(epochMicrosTowardPast(Temporal.Instant.fromEpochNanoseconds(-1_000n)), -1n);
  });
});
