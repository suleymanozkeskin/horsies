// The send span and the carrier: name, kind, attributes and parent; an error sets the status and `error.type`; the
// carrier that the core gets has the span's trace id; no SDK gives no carrier; a throw of the SDK changes no send;
// the rerun carrier is the active context's.

import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";

import * as otel from "@opentelemetry/api";

import { PACKAGE_VERSION } from "../../../../horsies/src/_catalog/package_version.ts";
import type { SendCallResult, SendObserver } from "../../../../horsies/src/app/observers.ts";
import { taskIdFromCore } from "../../../../horsies/src/contract/ids.ts";
import type { TraceArgument } from "../../../../horsies/src/native/values/records.ts";
import { sendErrorOfRaw } from "../../../../horsies/src/send/errors.ts";
import type { OtelApi } from "../../../../horsies/src/telemetry/otel-api.ts";
import { otelSendObserver } from "../../../../horsies/src/telemetry/send-span.ts";
import { err, ok } from "../../../../horsies/src/wire/result.ts";
import { rawEnqueueFailed, TASK_ID } from "../../app/support.ts";
import { apiWithBrokenSpanEnd, installSdk, loadedApi, onlySpan, recordedNotMade, recordedWarnings, SPAN_END_BROKEN, type TestSdk } from "../support.ts";

const FACTS = { taskName: "report", queueName: "default", taskId: TASK_ID };
const SENT: SendCallResult = ok(taskIdFromCore(TASK_ID, "the send span test"));
const FAILED: SendCallResult = err(sendErrorOfRaw(rawEnqueueFailed("report", false)));

function observerOf(api: OtelApi = loadedApi()): { readonly observer: SendObserver; readonly written: string[] } {
  const { notMade, written } = recordedNotMade();

  return { observer: otelSendObserver({ api, warnings: recordedWarnings().warnings, notMade, clock: () => performance.now() }), written };
}

/** Run one send through `observer`, keeping the trace that the core would get. */
async function sendThrough(observer: SendObserver, result: SendCallResult): Promise<TraceArgument[]> {
  const traces: TraceArgument[] = [];

  const returned = await observer.observeSend(FACTS, async (trace) => {
    traces.push(trace);

    return result;
  });

  assert.equal(returned, result, "the observer gives the result of the send call unchanged");

  return traces;
}

describe("the send span", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("one PRODUCER span `send <task_name>` with the attributes of the Python and Rust clients", async () => {
    await sendThrough(observerOf().observer, SENT);

    const span = onlySpan(sdk.spans(), "send report");

    assert.equal(span.kind, otel.SpanKind.PRODUCER);
    assert.equal(span.instrumentationScope.name, "horsies");
    assert.equal(span.instrumentationScope.version, PACKAGE_VERSION);
    assert.deepEqual(span.attributes, {
      "messaging.system": "horsies",
      "messaging.operation.type": "send",
      "messaging.operation.name": "send",
      "messaging.destination.name": "default",
      "messaging.message.id": TASK_ID,
      "horsies.task.id": TASK_ID,
      "horsies.task.name": "report",
      "horsies.queue": "default",
    });
    assert.equal(span.status.code, otel.SpanStatusCode.UNSET);
  });

  test("the parent is the caller's active span", async () => {
    const tracer = otel.trace.getTracer("caller");
    const caller = tracer.startSpan("caller");

    await otel.context.with(otel.trace.setSpan(otel.context.active(), caller), async () =>
      sendThrough(observerOf().observer, SENT),
    );
    caller.end();

    const span = onlySpan(sdk.spans(), "send report");

    assert.equal(span.parentSpanContext?.spanId, caller.spanContext().spanId);
    assert.equal(span.spanContext().traceId, caller.spanContext().traceId);
  });

  test("a failed send sets the status ERROR and error.type to the code", async () => {
    await sendThrough(observerOf().observer, FAILED);

    const span = onlySpan(sdk.spans(), "send report");

    assert.equal(span.status.code, otel.SpanStatusCode.ERROR);
    assert.equal(span.attributes["error.type"], "ENQUEUE_FAILED");
  });

  test("the carrier that the core gets is the span's, with its trace id", async () => {
    const [trace] = await sendThrough(observerOf().observer, SENT);
    const span = onlySpan(sdk.spans(), "send report");
    const { traceId, spanId } = span.spanContext();

    assert.deepEqual(trace, { kind: "carrier", carrier: { traceparent: `00-${traceId}-${spanId}-01` }, traceIdHex: traceId });
  });

  test("a throw of the send path ends the span with ERROR and passes on", async () => {
    const { observer } = observerOf();

    await assert.rejects(
      observer.observeSend(FACTS, async () => {
        throw new RangeError("a defect of the send path");
      }),
      RangeError,
    );

    const span = onlySpan(sdk.spans(), "send report");

    assert.equal(span.status.code, otel.SpanStatusCode.ERROR);
    assert.equal(span.attributes["error.type"], "RangeError");
  });
});

describe("an SDK that gives upper-case hex ids", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk("upperCaseIds");
  });

  afterEach(() => {
    sdk.remove();
  });

  /** The core reads a trace id of 32 lower-case hex digits. */
  const LOWER_CASE_TRACE_ID = /^[0-9a-f]{32}$/u;

  test("the send gives the core the trace id in lower case", async () => {
    const [trace] = await sendThrough(observerOf().observer, SENT);
    const { traceId } = onlySpan(sdk.spans(), "send report").spanContext();

    assert.match(traceId, /[A-F]/u, "the SDK gives upper-case digits");
    assert.equal(trace?.kind === "carrier" ? trace.traceIdHex : "", traceId.toLowerCase());
    assert.match(trace?.kind === "carrier" ? trace.traceIdHex : "", LOWER_CASE_TRACE_ID);
  });

  test("the rerun carrier gives the core the trace id in lower case", () => {
    const caller = otel.trace.getTracer("caller").startSpan("caller");

    const carrier = otel.context.with(otel.trace.setSpan(otel.context.active(), caller), () =>
      observerOf().observer.activeCarrier(),
    );

    caller.end();
    assert.equal(carrier.kind === "carrier" ? carrier.traceIdHex : "", caller.spanContext().traceId.toLowerCase());
    assert.match(carrier.kind === "carrier" ? carrier.traceIdHex : "", LOWER_CASE_TRACE_ID);
  });
});

describe("the send span with no SDK or a broken SDK", () => {
  test("no SDK gives no carrier, and the send goes on", async () => {
    const traces = await sendThrough(observerOf().observer, SENT);

    assert.deepEqual(traces, [{ kind: "noCarrier" }]);
  });

  test("a tracer that throws gives no carrier and one report of a span not made per process; the send result is unchanged", async () => {
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

    const { observer, written } = observerOf(broken);
    const traces: TraceArgument[] = [];

    for (let send = 0; send < 2; send += 1) {
      const result = await observer.observeSend(FACTS, async (trace) => {
        traces.push(trace);

        return SENT;
      });

      assert.equal(result, SENT);
    }

    assert.deepEqual(traces, [{ kind: "noCarrier" }, { kind: "noCarrier" }]);
    assert.deepEqual(written, [`send ${TASK_ID} Error: the SDK is broken`]);
  });
});

describe("a send span that cannot end", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("a throw at the end of the span is one report of a span not made per process; each send result is unchanged", async () => {
    const { observer, written } = observerOf(apiWithBrokenSpanEnd());

    await sendThrough(observer, SENT);
    await sendThrough(observer, FAILED);
    assert.deepEqual(written, [`send ${TASK_ID} Error: ${SPAN_END_BROKEN}`]);
  });
});

describe("the rerun carrier", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("the carrier of the caller's active span, and no span of its own", () => {
    const caller = otel.trace.getTracer("caller").startSpan("caller");

    const carrier = otel.context.with(otel.trace.setSpan(otel.context.active(), caller), () =>
      observerOf().observer.activeCarrier(),
    );

    const { traceId, spanId } = caller.spanContext();

    caller.end();
    assert.deepEqual(carrier, { kind: "carrier", carrier: { traceparent: `00-${traceId}-${spanId}-01` }, traceIdHex: traceId });
    assert.deepEqual(
      sdk.spans().map((span) => span.name),
      ["caller"],
    );
  });

  test("no active span gives no carrier", () => {
    assert.deepEqual(observerOf().observer.activeCarrier(), { kind: "noCarrier" });
  });
});
