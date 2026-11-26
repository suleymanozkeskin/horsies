// The attempt span of a body in a child: the parent by the parentage; a user span in the body nests under it, also
// after an `await` and in a timer; the status of each end; the attributes; no span for facts that are not recorded;
// a throw of the SDK changes no run; the flush of the provider.

import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, beforeEach, describe, test } from "node:test";

import * as otel from "@opentelemetry/api";

import type { BodyEnd, BodyObserver } from "../../../../horsies/src/child/body-observer.ts";
import type { BodyFactsRecord, BodyCarrierRecord } from "../../../../horsies/src/native/child/addon.ts";
import { otelBodyObserver } from "../../../../horsies/src/telemetry/attempt-span.ts";
import type { OtelApi } from "../../../../horsies/src/telemetry/otel-api.ts";
import type { AttemptParentage } from "../../../../horsies/src/telemetry/tracing-config.ts";
import { installSdk, loadedApi, onlySpan, recordedNotMade, recordedWarnings, type TestSdk } from "../support.ts";

const TASK_ID = "0192f6a1-7b3c-7d4e-8f00-0123456789ab";

function factsWith(carrier: BodyCarrierRecord, workflow: Extract<BodyFactsRecord, { kind: "recorded" }>["workflow"]): BodyFactsRecord {
  return {
    kind: "recorded",
    taskId: TASK_ID,
    taskName: "resize",
    queueName: "images",
    attempt: 2,
    workerId: "worker-1",
    execution: "SHARED_ASYNC",
    workflow,
    carrier,
  };
}

const NOT_IN_WORKFLOW = { kind: "notInWorkflow" } as const;

function observerOf(parentage: AttemptParentage, api: OtelApi = loadedApi()): { observer: BodyObserver; written: string[] } {
  const { notMade, written } = recordedNotMade();

  return { observer: otelBodyObserver({ api, parentage, warnings: recordedWarnings().warnings, notMade }), written };
}

/** The carrier of a send span that the test opens and ends. */
function sendCarrier(): { readonly carrier: BodyCarrierRecord; readonly send: otel.SpanContext } {
  const span = otel.trace.getTracer("sender").startSpan("send resize");
  const entries: { key: string; value: string }[] = [];

  otel.propagation.inject(otel.trace.setSpan(otel.ROOT_CONTEXT, span), entries, {
    set: (carrier, key, value) => {
      carrier.push({ key, value });
    },
  });
  span.end();

  return { carrier: { kind: "carrier", entries }, send: span.spanContext() };
}

/** Run one body through `observer` and end it with `end`. */
async function runBody(observer: BodyObserver, facts: BodyFactsRecord, end: BodyEnd, body: () => Promise<void> = async () => undefined): Promise<void> {
  const observation = observer.enter(facts);

  await observation.run(body);
  observation.end(end);
}

describe("the attempt span", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("one CONSUMER span `run <task_name>` with the attributes of the Python client", async () => {
    await runBody(observerOf("childOfSend").observer, factsWith({ kind: "noCarrier" }, NOT_IN_WORKFLOW), { kind: "returned" });

    const span = onlySpan(sdk.spans(), "run resize");

    assert.equal(span.kind, otel.SpanKind.CONSUMER);
    assert.deepEqual(span.attributes, {
      "horsies.task.id": TASK_ID,
      "horsies.task.name": "resize",
      "horsies.task.attempt": 2,
      "horsies.task.execution": "SHARED_ASYNC",
      "horsies.queue": "images",
      "horsies.worker.id": "worker-1",
      "messaging.system": "horsies",
      "messaging.operation.type": "process",
      "messaging.operation.name": "run",
      "messaging.destination.name": "images",
      "messaging.message.id": TASK_ID,
    });
  });

  test("a workflow task has the workflow id, the node index and the root id", async () => {
    const workflow = { kind: "inWorkflow", workflowId: "wf-1", nodeIndex: 3, rootWorkflowId: "wf-0" } as const;

    await runBody(observerOf("childOfSend").observer, factsWith({ kind: "noCarrier" }, workflow), { kind: "returned" });

    const { attributes } = onlySpan(sdk.spans(), "run resize");

    assert.deepEqual(
      [attributes["horsies.workflow.id"], attributes["horsies.workflow.node"], attributes["horsies.workflow.root_id"]],
      ["wf-1", 3, "wf-0"],
    );
  });

  test("childOfSend: the send span is the parent", async () => {
    const { carrier, send } = sendCarrier();

    await runBody(observerOf("childOfSend").observer, factsWith(carrier, NOT_IN_WORKFLOW), { kind: "returned" });

    const span = onlySpan(sdk.spans(), "run resize");

    assert.equal(span.parentSpanContext?.spanId, send.spanId);
    assert.equal(span.spanContext().traceId, send.traceId);
    assert.deepEqual(span.links, []);
  });

  test("linkedToSend: a root span with a link to the send span", async () => {
    const { carrier, send } = sendCarrier();

    await runBody(observerOf("linkedToSend").observer, factsWith(carrier, NOT_IN_WORKFLOW), { kind: "returned" });

    const span = onlySpan(sdk.spans(), "run resize");

    assert.equal(span.parentSpanContext, undefined);
    assert.notEqual(span.spanContext().traceId, send.traceId);
    assert.deepEqual(
      span.links.map((link) => link.context.spanId),
      [send.spanId],
    );
  });

  test("no carrier: a root span with no link, in both parentages", async () => {
    for (const parentage of ["childOfSend", "linkedToSend"] as const) {
      await runBody(observerOf(parentage).observer, factsWith({ kind: "noCarrier" }, NOT_IN_WORKFLOW), { kind: "returned" });
    }

    for (const span of sdk.spans()) {
      assert.deepEqual([span.parentSpanContext, span.links], [undefined, []]);
    }
  });

  test("a user span in the body nests under the attempt span, after an await and in a timer", async () => {
    const tracer = otel.trace.getTracer("user");

    await runBody(observerOf("childOfSend").observer, factsWith({ kind: "noCarrier" }, NOT_IN_WORKFLOW), { kind: "returned" }, async () => {
      tracer.startSpan("first").end();
      await sleep(1);
      tracer.startSpan("after await").end();
      await new Promise<void>((resolve) => {
        setTimeout(() => {
          tracer.startSpan("in timer").end();
          resolve();
        }, 1);
      });
    });

    const attempt = onlySpan(sdk.spans(), "run resize").spanContext().spanId;

    for (const name of ["first", "after await", "in timer"]) {
      assert.equal(onlySpan(sdk.spans(), name).parentSpanContext?.spanId, attempt, name);
    }
  });

  test("each end sets its status: unset for a returned value, the error code, TASK_TIMEOUT, TASK_CANCELLED", async () => {
    const ends: readonly [BodyEnd, number, string | undefined][] = [
      [{ kind: "returned" }, otel.SpanStatusCode.UNSET, undefined],
      [{ kind: "failed", code: "RATE_LIMITED" }, otel.SpanStatusCode.ERROR, "RATE_LIMITED"],
      [{ kind: "timedOut" }, otel.SpanStatusCode.ERROR, "TASK_TIMEOUT"],
      [{ kind: "cancelled" }, otel.SpanStatusCode.ERROR, "TASK_CANCELLED"],
    ];

    for (const [end] of ends) {
      await runBody(observerOf("childOfSend").observer, factsWith({ kind: "noCarrier" }, NOT_IN_WORKFLOW), end);
    }

    assert.deepEqual(
      sdk.spans().map((span) => [span.status.code, span.attributes["error.type"]]),
      ends.map(([, code, errorType]) => [code, errorType]),
    );
  });

  test("SHARED_ASYNC: a body that never settles gets its span at the core's deadline decision", () => {
    const observation = observerOf("childOfSend").observer.enter(factsWith({ kind: "noCarrier" }, NOT_IN_WORKFLOW));

    void observation.run(async () => new Promise<never>(() => undefined));
    observation.cancelled("DEADLINE");

    const span = onlySpan(sdk.spans(), "run resize");

    assert.deepEqual([span.status.code, span.attributes["error.type"]], [otel.SpanStatusCode.ERROR, "TASK_TIMEOUT"]);
  });

  test("SHARED_ASYNC: an operator cancel ends the span with TASK_CANCELLED; a later end and throw change nothing", () => {
    const observation = observerOf("childOfSend").observer.enter(factsWith({ kind: "noCarrier" }, NOT_IN_WORKFLOW));

    observation.cancelled("OPERATOR");
    observation.thrown(new Error("late"));
    observation.end({ kind: "returned" });

    const span = onlySpan(sdk.spans(), "run resize");

    assert.equal(span.attributes["error.type"], "TASK_CANCELLED");
    assert.deepEqual(span.events, []);
  });

  test("ISOLATED: the span ends with the body, not at the cancel signal", () => {
    const facts = { ...factsWith({ kind: "noCarrier" }, NOT_IN_WORKFLOW), execution: "ISOLATED" } as const;
    const observation = observerOf("childOfSend").observer.enter(facts);

    observation.cancelled("OPERATOR");
    assert.deepEqual(sdk.spans(), []);
    observation.end({ kind: "cancelled" });
    assert.equal(onlySpan(sdk.spans(), "run resize").attributes["error.type"], "TASK_CANCELLED");
  });

  test("a thrown value is recorded on the span", async () => {
    const observation = observerOf("childOfSend").observer.enter(factsWith({ kind: "noCarrier" }, NOT_IN_WORKFLOW));

    observation.thrown(new RangeError("out of range"));
    observation.end({ kind: "failed", code: "UNHANDLED_EXCEPTION" });

    const [event] = onlySpan(sdk.spans(), "run resize").events;

    assert.equal(event?.name, "exception");
    assert.equal(event?.attributes?.["exception.type"], "RangeError");
  });

  test("facts that are not recorded give no span", async () => {
    await runBody(observerOf("childOfSend").observer, { kind: "notRecorded" }, { kind: "returned" });

    assert.deepEqual(sdk.spans(), []);
  });

  test("the flush calls forceFlush of the provider that the user set", async () => {
    let flushes = 0;
    const original = sdk.provider.forceFlush.bind(sdk.provider);

    sdk.provider.forceFlush = async (options) => {
      flushes += 1;

      return original(options);
    };

    await observerOf("childOfSend").observer.flush();
    assert.equal(flushes, 1);
  });
});

describe("the attempt span with no SDK or a broken SDK", () => {
  test("no SDK: the body runs, and the flush resolves", async () => {
    const { observer } = observerOf("childOfSend");
    let ran = false;

    await runBody(observer, factsWith({ kind: "noCarrier" }, NOT_IN_WORKFLOW), { kind: "returned" }, async () => {
      ran = true;
    });
    await observer.flush();
    assert.equal(ran, true);
  });

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

    const { observer, written } = observerOf("childOfSend", broken);
    let runs = 0;

    for (let body = 0; body < 2; body += 1) {
      await runBody(observer, factsWith({ kind: "noCarrier" }, NOT_IN_WORKFLOW), { kind: "returned" }, async () => {
        runs += 1;
      });
    }

    assert.equal(runs, 2);
    assert.deepEqual(written, [`attempt ${TASK_ID} Error: the SDK is broken`]);
  });
});
