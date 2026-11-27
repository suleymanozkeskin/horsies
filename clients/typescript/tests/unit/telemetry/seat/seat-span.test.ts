// The attempt span of a body in a seat isolate: the span of a Node child (name, kind, attributes, the parent from the
// stored carrier), active where the body runs; its start at the isolate's clock and its end at the main thread's
// `endedAt`; the status of each verdict; the cancel signal ends nothing; a thrown value is recorded; no span for facts
// that are not recorded; a throw of the SDK changes no run and is one report of a span not made per process (at the
// open, the end or the record of a thrown value); the flush of the isolate's provider.

import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, beforeEach, describe, test } from "node:test";

import * as otel from "@opentelemetry/api";

import type { BodyEnd } from "../../../../horsies/src/child/body-observer.ts";
import type { BodyCarrierRecord, BodyFactsRecord } from "../../../../horsies/src/native/child/addon.ts";
import type { SeatBodyObserver } from "../../../../horsies/src/seats/body-observer.ts";
import type { OtelApi } from "../../../../horsies/src/telemetry/otel-api.ts";
import { highResolutionTimeOfInstant, otelSeatBodyObserver } from "../../../../horsies/src/telemetry/seat-span.ts";
import { installSdk, loadedApi, onlySpan, recordedNotMade, recordedWarnings, type TestSdk } from "../support.ts";

const TASK_ID = "0192f6a1-7b3c-7d4e-8f00-0123456789ab";

/** The isolate's clock at the open of each span in the tests. */
const OPENED_AT = Temporal.Instant.from("2026-11-26T10:00:00.000001234Z");

/** The main thread's time of the settle in the tests: 2.5 s after the open. */
const ENDED_AT = Temporal.Instant.from("2026-11-26T10:00:02.500001234Z");

function factsWith(carrier: BodyCarrierRecord): BodyFactsRecord {
  return {
    kind: "recorded",
    taskId: TASK_ID,
    taskName: "crunch",
    queueName: "default",
    attempt: 1,
    workerId: "worker-1",
    execution: "SHARED_THREAD",
    workflow: { kind: "notInWorkflow" },
    carrier,
  };
}

const NO_CARRIER: BodyCarrierRecord = { kind: "noCarrier" };

/** The observer of a test, the warnings it wrote and the reports of spans not made that it wrote. */
type TestObserver = { readonly observer: SeatBodyObserver; readonly lines: string[]; readonly written: string[] };

function observerOf(api: OtelApi = loadedApi()): TestObserver {
  const { warnings, lines } = recordedWarnings();
  const { notMade, written } = recordedNotMade();

  return { observer: otelSeatBodyObserver({ api, parentage: "childOfSend", warnings, now: () => OPENED_AT, notMade }), lines, written };
}

/** The API whose spans throw at `member`. */
function apiWithBrokenSpans(member: "end" | "recordException"): OtelApi {
  const api = loadedApi();

  return {
    ...api,
    trace: {
      ...api.trace,
      getTracer: (name) => ({
        startSpan: (spanName, options, context) => {
          const span = api.trace.getTracer(name).startSpan(spanName, options, context);

          const broken = (): never => {
            throw new Error(`the SDK is broken at ${member}`);
          };

          return {
            setAttribute: (key, value) => span.setAttribute(key, value),
            setStatus: (status) => span.setStatus(status),
            recordException: member === "recordException" ? broken : (exception) => {
              span.recordException(exception);
            },
            end: member === "end" ? broken : (endTime) => {
              span.end(endTime);
            },
            spanContext: () => span.spanContext(),
          };
        },
      }),
    },
  };
}

/** The carrier of a send span that the test opens and ends. */
function sendCarrier(): { readonly carrier: BodyCarrierRecord; readonly send: otel.SpanContext } {
  const span = otel.trace.getTracer("sender").startSpan("send crunch");
  const entries: { key: string; value: string }[] = [];

  otel.propagation.inject(otel.trace.setSpan(otel.ROOT_CONTEXT, span), entries, {
    set: (carrier, key, value) => {
      carrier.push({ key, value });
    },
  });
  span.end();

  return { carrier: { kind: "carrier", entries }, send: span.spanContext() };
}

/** Run one body through `observer`, then end it with the verdict `end` at `ENDED_AT`. */
async function runSeatBody(observer: SeatBodyObserver, facts: BodyFactsRecord, end: BodyEnd, body: () => Promise<void> = async () => undefined): Promise<void> {
  const observation = observer.enter(facts);

  await observation.run(body);
  observation.end(end, ENDED_AT);
}

describe("the attempt span of a seat body", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("one CONSUMER span `run <task_name>` with the attributes of a span of a Node child, a child of the send span", async () => {
    const { carrier, send } = sendCarrier();

    await runSeatBody(observerOf().observer, factsWith(carrier), { kind: "returned" });

    const span = onlySpan(sdk.spans(), "run crunch");

    assert.equal(span.kind, otel.SpanKind.CONSUMER);
    assert.equal(span.parentSpanContext?.spanId, send.spanId);
    assert.equal(span.spanContext().traceId, send.traceId);
    assert.deepEqual(span.attributes, {
      "horsies.task.id": TASK_ID,
      "horsies.task.name": "crunch",
      "horsies.task.attempt": 1,
      "horsies.task.execution": "SHARED_THREAD",
      "horsies.queue": "default",
      "horsies.worker.id": "worker-1",
      "messaging.system": "horsies",
      "messaging.operation.type": "process",
      "messaging.operation.name": "run",
      "messaging.destination.name": "default",
      "messaging.message.id": TASK_ID,
    });
  });

  test("the span starts at the isolate's clock at the open and ends at the main thread's endedAt", async () => {
    await runSeatBody(observerOf().observer, factsWith(NO_CARRIER), { kind: "returned" });

    const span = onlySpan(sdk.spans(), "run crunch");

    assert.deepEqual(span.startTime, highResolutionTimeOfInstant(OPENED_AT));
    assert.deepEqual(span.endTime, highResolutionTimeOfInstant(ENDED_AT));
    assert.deepEqual(span.endTime, [1_795_687_202, 500_001_234]);
  });

  test("a user span in the body nests under the attempt span, also after an await", async () => {
    const tracer = otel.trace.getTracer("user");

    await runSeatBody(observerOf().observer, factsWith(NO_CARRIER), { kind: "returned" }, async () => {
      tracer.startSpan("first").end();
      await sleep(1);
      tracer.startSpan("after await").end();
    });

    const attempt = onlySpan(sdk.spans(), "run crunch").spanContext().spanId;

    for (const name of ["first", "after await"]) {
      assert.equal(onlySpan(sdk.spans(), name).parentSpanContext?.spanId, attempt, name);
    }
  });

  test("each verdict sets its status: unset for a returned value, the error code, TASK_TIMEOUT, TASK_CANCELLED", async () => {
    const ends: readonly [BodyEnd, number, string | undefined][] = [
      [{ kind: "returned" }, otel.SpanStatusCode.UNSET, undefined],
      [{ kind: "failed", code: "RATE_LIMITED" }, otel.SpanStatusCode.ERROR, "RATE_LIMITED"],
      [{ kind: "timedOut" }, otel.SpanStatusCode.ERROR, "TASK_TIMEOUT"],
      [{ kind: "cancelled" }, otel.SpanStatusCode.ERROR, "TASK_CANCELLED"],
    ];

    for (const [end] of ends) {
      await runSeatBody(observerOf().observer, factsWith(NO_CARRIER), end);
    }

    assert.deepEqual(
      sdk.spans().map((span) => [span.status.code, span.attributes["error.type"]]),
      ends.map(([, code, errorType]) => [code, errorType]),
    );
  });

  test("the cancel signal ends nothing: a sync body past its deadline ends its span at the verdict's endedAt", () => {
    const observation = observerOf().observer.enter(factsWith(NO_CARRIER));

    observation.run(() => undefined);
    observation.cancelled("DEADLINE");
    assert.deepEqual(sdk.spans(), []);
    observation.end({ kind: "timedOut" }, ENDED_AT);

    const span = onlySpan(sdk.spans(), "run crunch");

    assert.deepEqual([span.status.code, span.attributes["error.type"]], [otel.SpanStatusCode.ERROR, "TASK_TIMEOUT"]);
    assert.deepEqual(span.endTime, highResolutionTimeOfInstant(ENDED_AT));
  });

  test("a thrown value is recorded on the span", () => {
    const observation = observerOf().observer.enter(factsWith(NO_CARRIER));

    observation.thrown(new RangeError("out of range"));
    observation.end({ kind: "failed", code: "UNHANDLED_EXCEPTION" }, ENDED_AT);

    const [event] = onlySpan(sdk.spans(), "run crunch").events;

    assert.equal(event?.name, "exception");
    assert.equal(event?.attributes?.["exception.type"], "RangeError");
  });

  test("facts that are not recorded give no span", async () => {
    await runSeatBody(observerOf().observer, { kind: "notRecorded" }, { kind: "returned" });

    assert.deepEqual(sdk.spans(), []);
  });

  test("the flush calls forceFlush of the provider that the isolate set", async () => {
    let flushes = 0;
    const original = sdk.provider.forceFlush.bind(sdk.provider);

    sdk.provider.forceFlush = async (options) => {
      flushes += 1;

      return original(options);
    };

    await observerOf().observer.flush();
    assert.equal(flushes, 1);
  });
});

describe("the attempt span of a seat body with a broken SDK", () => {
  test("a tracer that throws: the body runs with no span, and one report of a span not made per process", async () => {
    const api = loadedApi();

    const broken: OtelApi = {
      ...api,
      trace: {
        ...api.trace,
        getTracer: () => {
          throw new Error("the SDK is broken");
        },
      },
    };

    const { observer, lines, written } = observerOf(broken);
    let runs = 0;

    for (let run = 0; run < 2; run += 1) {
      await runSeatBody(observer, factsWith(NO_CARRIER), { kind: "returned" }, async () => {
        runs += 1;
      });
    }

    assert.equal(runs, 2);
    assert.deepEqual(written, [`seatAttempt ${TASK_ID} Error: the SDK is broken`]);
    assert.deepEqual(lines, []);
  });

  test("a span whose end throws is a span not made: one report per process", async () => {
    const { observer, written } = observerOf(apiWithBrokenSpans("end"));

    await runSeatBody(observer, factsWith(NO_CARRIER), { kind: "returned" });
    await runSeatBody(observer, factsWith(NO_CARRIER), { kind: "timedOut" });
    assert.deepEqual(written, [`seatAttempt ${TASK_ID} Error: the SDK is broken at end`]);
  });

  test("a throw when the span records a thrown value is a span that is not complete: one report per process", () => {
    const { observer, lines, written } = observerOf(apiWithBrokenSpans("recordException"));
    const observation = observer.enter(factsWith(NO_CARRIER));

    observation.thrown(new RangeError("out of range"));
    observation.end({ kind: "failed", code: "UNHANDLED_EXCEPTION" }, ENDED_AT);
    assert.deepEqual(written, [`seatAttempt ${TASK_ID} Error: the SDK is broken at recordException`]);
    assert.deepEqual(lines, []);
  });
});

describe("the time of an instant", () => {
  test("seconds and nanoseconds since the epoch, also before 1970", () => {
    assert.deepEqual(highResolutionTimeOfInstant(Temporal.Instant.fromEpochNanoseconds(1_500_000_001n)), [1, 500_000_001]);
    assert.deepEqual(highResolutionTimeOfInstant(Temporal.Instant.fromEpochNanoseconds(-1n)), [-1, 999_999_999]);
  });
});

describe("the span of a run whose seat isolate ended", () => {
  const STARTED_AT = Temporal.Instant.from("2026-11-26T10:00:00.000001234Z");
  const EXITED_AT = Temporal.Instant.from("2026-11-26T10:00:03.250001234Z");

  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("one CONSUMER span `run <task_name>`, ERROR with WORKER_CRASHED, source worker, the parent from the carrier", () => {
    const { carrier, send } = sendCarrier();

    observerOf().observer.isolateEnded({ facts: factsWith(carrier), startedAt: STARTED_AT, endedAt: EXITED_AT });

    const span = onlySpan(sdk.spans(), "run crunch");

    assert.equal(span.kind, otel.SpanKind.CONSUMER);
    assert.equal(span.parentSpanContext?.spanId, send.spanId);
    assert.deepEqual([span.status.code, span.attributes["error.type"]], [otel.SpanStatusCode.ERROR, "WORKER_CRASHED"]);
    assert.equal(span.attributes["horsies.span.source"], "worker");
    assert.equal(span.attributes["horsies.task.id"], TASK_ID);
    assert.equal(span.attributes["horsies.task.execution"], "SHARED_THREAD");
  });

  test("it starts at the time of the run post and ends at the time of the isolate's exit", () => {
    observerOf().observer.isolateEnded({ facts: factsWith(NO_CARRIER), startedAt: STARTED_AT, endedAt: EXITED_AT });

    const span = onlySpan(sdk.spans(), "run crunch");

    assert.deepEqual(span.startTime, highResolutionTimeOfInstant(STARTED_AT));
    assert.deepEqual(span.endTime, highResolutionTimeOfInstant(EXITED_AT));
  });

  test("facts that are not recorded give no span", () => {
    observerOf().observer.isolateEnded({ facts: { kind: "notRecorded" }, startedAt: STARTED_AT, endedAt: EXITED_AT });
    assert.deepEqual(sdk.spans(), []);
  });

  test("a throw of the SDK at the end is one report of a span not made, and no throw out of the observer", () => {
    const { observer, written } = observerOf(apiWithBrokenSpans("end"));

    observer.isolateEnded({ facts: factsWith(NO_CARRIER), startedAt: STARTED_AT, endedAt: EXITED_AT });
    assert.equal(written.length, 1);
  });
});
