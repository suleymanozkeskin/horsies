// The span of an attempt that the worker ended: name, kind, parent from the carrier, the start and end times in
// microseconds, ERROR with `error.type`, the attributes with `horsies.span.source = worker`, and the workflow
// attributes of a known node only; the switch of the tracing setting; a broken SDK changes nothing.

import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";

import * as otel from "@opentelemetry/api";

import type { BodyCarrierRecord } from "../../../../horsies/src/native/child/addon.ts";
import type { EndedAttemptRecord, EndedWorkflowRecord } from "../../../../horsies/src/native/handler-child/addon.ts";
import { tracingPartsOf } from "../../../../horsies/src/telemetry/create-hook.ts";
import { highResolutionTimeOf, otelEndedSpans } from "../../../../horsies/src/telemetry/ended-span.ts";
import type { OtelApi } from "../../../../horsies/src/telemetry/otel-api.ts";
import { tracingSettingOf } from "../../../../horsies/src/telemetry/tracing-config.ts";
import { installSdk, loadedApi, onlySpan, recordedWarnings, type TestSdk } from "../support.ts";

const TASK_ID = "0192f6a1-7b3c-7d4e-8f00-0123456789ab";

/** 2023-11-14T22:13:20.123456Z and 2023-11-14T22:13:50.654321Z in epoch microseconds. */
const STARTED_AT_MICROS = 1_700_000_000_123_456n;
const ENDED_AT_MICROS = 1_700_000_030_654_321n;

function endedAttempt(carrier: BodyCarrierRecord, workflow: EndedWorkflowRecord): EndedAttemptRecord {
  return {
    taskId: TASK_ID,
    taskName: "resize",
    queueName: "images",
    attempt: 1,
    workerId: "worker-1",
    execution: "ISOLATED",
    workflow,
    carrier,
    errorCode: "TASK_TIMEOUT",
    startedAtMicros: STARTED_AT_MICROS,
    endedAtMicros: ENDED_AT_MICROS,
  };
}

const NOT_IN_WORKFLOW: EndedWorkflowRecord = { kind: "known", workflow: { kind: "notInWorkflow" } };

function makerOf(api: OtelApi = loadedApi()): { readonly make: (attempt: EndedAttemptRecord) => void; readonly lines: string[] } {
  const { warnings, lines } = recordedWarnings();
  const spans = otelEndedSpans({ api, parentage: "childOfSend", warnings });

  return { make: spans.kind === "makeSpans" ? spans.make : assert.fail("no maker"), lines };
}

describe("the high-resolution time of epoch microseconds", () => {
  test("seconds and nanoseconds, with no loss", () => {
    assert.deepEqual(highResolutionTimeOf(STARTED_AT_MICROS), [1_700_000_000, 123_456_000]);
    assert.deepEqual(highResolutionTimeOf(-1n), [-1, 999_999_000]);
  });
});

describe("the worker-ended span", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("a CONSUMER span `run <task_name>` from the placement to the end, ERROR with the code of the attempt row", () => {
    makerOf().make(endedAttempt({ kind: "noCarrier" }, NOT_IN_WORKFLOW));

    const span = onlySpan(sdk.spans(), "run resize");

    assert.equal(span.kind, otel.SpanKind.CONSUMER);
    assert.deepEqual(span.startTime, [1_700_000_000, 123_456_000]);
    assert.deepEqual(span.endTime, [1_700_000_030, 654_321_000]);
    assert.equal(span.status.code, otel.SpanStatusCode.ERROR);
    assert.deepEqual(span.attributes, {
      "horsies.task.id": TASK_ID,
      "horsies.task.name": "resize",
      "horsies.task.attempt": 1,
      "horsies.task.execution": "ISOLATED",
      "horsies.queue": "images",
      "horsies.worker.id": "worker-1",
      "messaging.system": "horsies",
      "messaging.operation.type": "process",
      "messaging.operation.name": "run",
      "messaging.destination.name": "images",
      "messaging.message.id": TASK_ID,
      "horsies.span.source": "worker",
      "error.type": "TASK_TIMEOUT",
    });
  });

  test("the send span is the parent", () => {
    const send = otel.trace.getTracer("sender").startSpan("send resize");
    const entries: { key: string; value: string }[] = [];

    otel.propagation.inject(otel.trace.setSpan(otel.ROOT_CONTEXT, send), entries, {
      set: (carrier, key, value) => {
        carrier.push({ key, value });
      },
    });
    send.end();
    makerOf().make(endedAttempt({ kind: "carrier", entries }, NOT_IN_WORKFLOW));

    assert.equal(onlySpan(sdk.spans(), "run resize").parentSpanContext?.spanId, send.spanContext().spanId);
  });

  test("a known workflow node gives the workflow attributes; a node not known gives none", () => {
    const { make } = makerOf();

    make(endedAttempt({ kind: "noCarrier" }, { kind: "known", workflow: { kind: "inWorkflow", workflowId: "wf-1", nodeIndex: 2, rootWorkflowId: "wf-0" } }));
    make(endedAttempt({ kind: "noCarrier" }, { kind: "workflowNotKnown" }));

    assert.deepEqual(
      sdk.spans().map((span) => [span.attributes["horsies.workflow.id"], span.attributes["horsies.workflow.node"]]),
      [
        ["wf-1", 2],
        [undefined, undefined],
      ],
    );
  });

  test("the tracing setting: makeSpans gives the maker; noSpans and none give no spans", () => {
    const environment = { loadApi: () => ({ kind: "loaded", api: loadedApi() }) as const, warnings: recordedWarnings().warnings, clock: () => 0, now: () => Temporal.Now.instant() };

    const kinds = [
      tracingSettingOf({ kind: "otel" }),
      tracingSettingOf({ kind: "otel", workerEndedSpans: "noSpans" }),
      tracingSettingOf({ kind: "none" }),
    ].map((setting) => {
      const parts = tracingPartsOf(setting, environment);

      return parts.ok ? parts.value.endedSpans.kind : "refused";
    });

    assert.deepEqual(kinds, ["makeSpans", "noSpans", "noSpans"]);
  });
});

describe("the worker-ended span with a broken SDK", () => {
  test("a tracer that throws makes no span, writes one warning, and never throws", () => {
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

    const { make, lines } = makerOf(broken);

    assert.doesNotThrow(() => {
      make(endedAttempt({ kind: "noCarrier" }, NOT_IN_WORKFLOW));
      make(endedAttempt({ kind: "noCarrier" }, NOT_IN_WORKFLOW));
    });
    assert.equal(lines.length, 1);
  });
});
