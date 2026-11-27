// The start span of a workflow start and its carrier: name, kind, attributes and parent; a start error sets the status
// and `error.type`; the carrier that the core gets has the span's trace id; a throw of the start path ends the span
// with ERROR; no SDK gives no carrier; a throw of the SDK changes no start and is one report of a span not made per
// process. Through the app: each start and each retry of a start has one span, and the core gets its carrier; a spec
// that the core's parse refuses opens no span; an app with no tracing gives the core no carrier.

import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";

import * as otel from "@opentelemetry/api";

import { createHorsies, type Horsies } from "../../../../horsies/src/app.ts";
import { WorkflowId } from "../../../../horsies/src/contract/ids.ts";
import { defineTask } from "../../../../horsies/src/contract/public.ts";
import type { TraceArgument } from "../../../../horsies/src/native/values/records.ts";
import type { OtelApi } from "../../../../horsies/src/telemetry/otel-api.ts";
import { otelStartObserver } from "../../../../horsies/src/telemetry/start-span.ts";
import { h } from "../../../../horsies/src/wire/public.ts";
import { err, ok } from "../../../../horsies/src/wire/result.ts";
import { defineWorkflow } from "../../../../horsies/src/workflow/public.ts";
import type { StartCallResult, StartObserver } from "../../../../horsies/src/workflow/start.ts";
import { DATABASE_URL, fakeEnvironment, fakeProducer } from "../../app/support.ts";
import { fakeCore, type FakeCore, rawStartEnqueueFailed, WORKFLOW_ID } from "../../workflow/core-support.ts";
import { API, installSdk, loadedApi, onlySpan, recordedNotMade, type TestSdk } from "../support.ts";

const FACTS = { workflowName: "nightly", workflowId: WORKFLOW_ID };
const STARTED: StartCallResult = ok("the handle");
const FAILED: StartCallResult = err({ code: "WORKFLOW_ID_CONFLICT" });

function observerOf(api: OtelApi = loadedApi()): { readonly observer: StartObserver; readonly written: string[] } {
  const { notMade, written } = recordedNotMade();

  return { observer: otelStartObserver({ api, notMade }), written };
}

/** Run one start through `observer`, keeping the trace that the core would get. */
async function startThrough(observer: StartObserver, result: StartCallResult): Promise<TraceArgument[]> {
  const traces: TraceArgument[] = [];

  const returned = await observer.observeStart(FACTS, async (trace) => {
    traces.push(trace);

    return result;
  });

  assert.equal(returned, result, "the observer gives the outcome of the start unchanged");

  return traces;
}

/** The API whose tracer gives spans whose `end` throws. */
function apiWithBrokenEnd(): OtelApi {
  const api = loadedApi();

  return {
    ...api,
    trace: {
      ...api.trace,
      getTracer: (name) => ({
        startSpan: (spanName, options, context) => {
          const span = api.trace.getTracer(name).startSpan(spanName, options, context);

          return {
            setAttribute: (key, value) => span.setAttribute(key, value),
            setStatus: (status) => span.setStatus(status),
            recordException: (exception) => {
              span.recordException(exception);
            },
            end: () => {
              throw new Error("the exporter is broken");
            },
            spanContext: () => span.spanContext(),
          };
        },
      }),
    },
  };
}

describe("the start span", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("one PRODUCER span `start <workflow_name>` with the attributes of the Python and Rust clients", async () => {
    await startThrough(observerOf().observer, STARTED);

    const span = onlySpan(sdk.spans(), "start nightly");

    assert.equal(span.kind, otel.SpanKind.PRODUCER);
    assert.equal(span.instrumentationScope.name, "horsies");
    assert.deepEqual(span.attributes, {
      "messaging.system": "horsies",
      "messaging.operation.type": "send",
      "messaging.operation.name": "start",
      "horsies.workflow.id": WORKFLOW_ID,
      "horsies.workflow.name": "nightly",
    });
    assert.equal(span.status.code, otel.SpanStatusCode.UNSET);
  });

  test("the parent is the caller's active span", async () => {
    const caller = otel.trace.getTracer("caller").startSpan("caller");

    await otel.context.with(otel.trace.setSpan(otel.context.active(), caller), async () => startThrough(observerOf().observer, STARTED));
    caller.end();

    const span = onlySpan(sdk.spans(), "start nightly");

    assert.equal(span.parentSpanContext?.spanId, caller.spanContext().spanId);
    assert.equal(span.spanContext().traceId, caller.spanContext().traceId);
  });

  test("a start error sets the status ERROR and error.type to its code", async () => {
    await startThrough(observerOf().observer, FAILED);

    const span = onlySpan(sdk.spans(), "start nightly");

    assert.equal(span.status.code, otel.SpanStatusCode.ERROR);
    assert.equal(span.attributes["error.type"], "WORKFLOW_ID_CONFLICT");
  });

  test("the carrier that the core gets is the span's, with its trace id", async () => {
    const [trace] = await startThrough(observerOf().observer, STARTED);
    const { traceId, spanId } = onlySpan(sdk.spans(), "start nightly").spanContext();

    assert.deepEqual(trace, { kind: "carrier", carrier: { traceparent: `00-${traceId}-${spanId}-01` }, traceIdHex: traceId });
  });

  test("a throw of the start path ends the span with ERROR and passes on", async () => {
    await assert.rejects(
      observerOf().observer.observeStart(FACTS, async () => {
        throw new RangeError("a defect of the start path");
      }),
      RangeError,
    );

    const span = onlySpan(sdk.spans(), "start nightly");

    assert.equal(span.status.code, otel.SpanStatusCode.ERROR);
    assert.equal(span.attributes["error.type"], "RangeError");
  });

  test("a span whose end throws is a span not made: one report per process, and the outcome of the start is unchanged", async () => {
    const { observer, written } = observerOf(apiWithBrokenEnd());

    await startThrough(observer, STARTED);
    await startThrough(observer, FAILED);
    assert.deepEqual(written, [`workflowStart ${WORKFLOW_ID} Error: the exporter is broken`]);
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

  test("the start gives the core the trace id in lower case", async () => {
    const [trace] = await startThrough(observerOf().observer, STARTED);
    const { traceId } = onlySpan(sdk.spans(), "start nightly").spanContext();

    assert.match(traceId, /[A-F]/u, "the SDK gives upper-case digits");
    assert.equal(trace?.kind === "carrier" ? trace.traceIdHex : "", traceId.toLowerCase());
  });
});

describe("the start span with no SDK or a broken SDK", () => {
  test("no SDK gives no carrier, and the start goes on", async () => {
    assert.deepEqual(await startThrough(observerOf().observer, STARTED), [{ kind: "noCarrier" }]);
  });

  test("a tracer that throws gives no carrier and one report of a span not made; the outcome of the start is unchanged", async () => {
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
    const traces = [...(await startThrough(observer, STARTED)), ...(await startThrough(observer, FAILED))];

    assert.deepEqual(traces, [{ kind: "noCarrier" }, { kind: "noCarrier" }]);
    assert.deepEqual(written, [`workflowStart ${WORKFLOW_ID} Error: the SDK is broken`]);
  });
});

const greet = defineTask({ name: "greet", input: { who: h.string() }, output: h.string() });

const hello = defineWorkflow(
  { name: "hello", definitionKey: "tests.hello.v1", outputType: h.string(), parameters: { who: h.string() }, checkCases: [{ who: "a" }] },
  (w, { who }) => ({ output: w.task(greet, { who }, { nodeId: "greet" }) }),
);

/** The app of `hello` on `core`, with the tracing `tracing`. A refused create fails the test. */
function workflowApp(core: FakeCore, tracing: { readonly kind: "otelTracing" } | { readonly kind: "noTracing" }): Horsies {
  const created = createHorsies(
    { database: { databaseUrl: DATABASE_URL }, tracing, tasks: [greet], workflows: [hello] },
    fakeEnvironment({ otelApi: () => ({ kind: "loaded", api: API }), producer: { ...fakeProducer(), workflows: core } }),
  );

  return created.ok ? created.value : assert.fail(`the app is refused: ${created.error.refusals.map((refused) => refused.message).join("; ")}`);
}

/** The trace id of `trace`, or `noCarrier`. */
function traceIdOf(trace: TraceArgument | undefined): string {
  return trace?.kind === "carrier" ? trace.traceIdHex : "noCarrier";
}

describe("Horsies.create and the start span", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("with otel tracing, a start and its retry each have one span, and the core stores the carrier of each", async () => {
    const core = fakeCore({ started: [{ kind: "failed", error: rawStartEnqueueFailed("hello", true) }, { kind: "started" }] });
    const app = workflowApp(core, { kind: "otelTracing" });
    const workflowId = WorkflowId.parse(WORKFLOW_ID);

    assert(workflowId.ok);

    const started = await app.start(hello, { who: "eu" }, { workflowId: workflowId.value });

    assert(!started.ok && started.error.code === "ENQUEUE_FAILED");
    assert((await app.resendStart(started.error)).ok);

    const [first, retry, ...others] = sdk.spans();

    assert.ok(first !== undefined && retry !== undefined && others.length === 0, `two spans, not ${String(sdk.spans().length)}`);
    assert.deepEqual(
      [first.name, first.attributes["error.type"], first.attributes["horsies.workflow.id"]],
      ["start hello", "ENQUEUE_FAILED", WORKFLOW_ID],
    );
    assert.deepEqual([retry.name, retry.status.code], ["start hello", otel.SpanStatusCode.UNSET]);
    assert.equal(traceIdOf(core.starts[0]?.trace), first.spanContext().traceId);
    assert.equal(traceIdOf(core.starts[1]?.trace), retry.spanContext().traceId);
  });

  test("with otel tracing, a spec that the core's parse refuses opens no span: the start never reached the core", async () => {
    const core = fakeCore({ refusedPrepare: { kind: "specRefused", fieldPath: "nodes[0].queue", cause: "no class" } });
    const started = await workflowApp(core, { kind: "otelTracing" }).start(hello, { who: "eu" });

    assert(!started.ok && started.error.code === "VALIDATION_FAILED");
    assert.deepEqual([sdk.spans(), core.starts], [[], []]);
  });

  test("with no tracing, the core gets no carrier and no span ends", async () => {
    const core = fakeCore();
    const app = workflowApp(core, { kind: "noTracing" });
    const caller = otel.trace.getTracer("caller").startSpan("caller");

    assert((await otel.context.with(otel.trace.setSpan(otel.context.active(), caller), async () => app.start(hello, { who: "eu" }))).ok);
    caller.end();
    assert.deepEqual(core.starts[0]?.trace, { kind: "noCarrier" });
    assert.deepEqual(
      sdk.spans().map((span) => span.name),
      ["caller"],
    );
  });
});
