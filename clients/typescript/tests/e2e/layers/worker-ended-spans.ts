// The attempt spans and the spans of the attempts that the worker ended, through a live worker (Python
// `clients/python/tests/e2e/test_worker_ended_spans.py`).
//
// The `worker_ended_spans` instance has OpenTelemetry tracing. Each task child, each seat isolate and the handler child
// set an SDK whose exporter writes one span line per ended span to the marker `span-lines` (`src/span-sdk.ts`). The
// test process sets an SDK of its own and sends each task (or starts each workflow) inside a span of its own, so the
// carrier that the send or the start stores puts the spans of the worker in that trace.
//
// - A task child makes the span of each attempt that it ran.
// - An isolated task killed at its timeout: its child is gone, and the handler child makes the span of the attempt
//   from the `AttemptEnded` event, with `horsies.span.source` = `worker`.
// - A workflow start has its span in the test process; every row of the tree stores its carrier, so the span of a node
//   is a child of the start span.
// - A shared async task that its child ended at the deadline: the child lives, so its span is the one span of the
//   attempt, and the handler child makes none.
// - A `SHARED_THREAD` task: its seat isolate makes the span. A sync body that holds its isolate past its deadline and
//   then returns ends its span at the settle of the run on the child's main thread, with `TASK_TIMEOUT`. A neighbour
//   with time left keeps the thread child alive past that deadline. A body that ends its isolate with `process.exit`
//   gives the run `WORKER_CRASHED`, and the main thread of the thread child makes the span, with `horsies.span.source`
//   = `worker`, in the trace of the send. `SHARED_THREAD` children run on Node only: on Bun
//   these cases are skipped (the worker refuses the profile).
//
// Run it with `run.ts` (see that file).

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe, test } from "node:test";

import * as otel from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { InMemorySpanExporter, type ReadableSpan, SimpleSpanProcessor, TracerProvider } from "@opentelemetry/sdk-trace";

import { loadAddon, type RerunEnqueuePolicy } from "#horsies";
import { parseSpanLine, type SpanLine, SPAN_LINES_MARKER } from "../src/span-lines.ts";
import * as basic from "../src/tasks/basic.ts";
import * as endedSpans from "../src/tasks/ended-spans.ts";
import { runtimeName } from "../src/tasks/waits.ts";
import { failedCode, sentHandle, succeeded } from "../support/outcome.ts";
import { found, NOT_YET, pollFor } from "../support/poll.ts";
import { pause } from "../support/process.ts";
import { e2eTest, READY_LIMIT_MS, type Suite, WORKER_READY_LINE } from "../support/suite.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 30_000;

/** The longest wait for a span line: the worker writes an events frame within its flush interval. */
const SPAN_LINE_WAIT_MS = 30_000;

/**
 * How long a test waits after the first span line of an attempt for a second one: past the flush interval of the
 * events (25 ms) and the dispatch of the handler child, which makes a span at once.
 */
const SECOND_SPAN_WAIT_MS = 3_000;

/** The attribute keys that the tests read (`horsies/src/telemetry/attributes.ts`). */
const TASK_ID_KEY = "horsies.task.id";
const TASK_ATTEMPT_KEY = "horsies.task.attempt";
const TASK_EXECUTION_KEY = "horsies.task.execution";
const SPAN_SOURCE_KEY = "horsies.span.source";
const ERROR_TYPE_KEY = "error.type";
const WORKFLOW_ID_KEY = "horsies.workflow.id";
const WORKFLOW_NODE_KEY = "horsies.workflow.node";
const WORKFLOW_ROOT_ID_KEY = "horsies.workflow.root_id";

/** The longest wait for a thread body to write its started marker. */
const STARTED_WAIT_MS = 30_000;

/** How long the neighbour of the thread overrun holds its isolate: past the overrun's return. */
const NEIGHBOUR_HOLD_MS = 6_000;

/** Why the thread seat cases do not run on Bun. */
const THREAD_SEATS_ON_NODE_ONLY = "SHARED_THREAD children run on Node only: the worker on Bun refuses the profile";

const NANOSECONDS_PER_MILLISECOND = 1_000_000n;

/** The SDK of the test process: the parent span of its sends, and the spans that ended here. */
type TestTracing = {
  readonly parent: otel.Span;
  readonly ended: () => readonly ReadableSpan[];
  readonly remove: () => void;
};

/** Set an SDK as the globals of the API in this process, and open the parent span. Call `remove` after the test. */
function installTestTracing(): TestTracing {
  const exporter = new InMemorySpanExporter();
  const provider = new TracerProvider({ spanProcessors: [new SimpleSpanProcessor({ exporter })] });

  otel.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  otel.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  otel.trace.setGlobalTracerProvider(provider);

  const parent = provider.getTracer("e2e").startSpan("e2e parent");

  return {
    parent,
    ended: () => exporter.getFinishedSpans(),
    remove: () => {
      parent.end();
      otel.trace.disable();
      otel.propagation.disable();
      otel.context.disable();
    },
  };
}

/** Run `call` with the parent span of `tracing` as the active span. */
async function inParentSpan<T>(tracing: TestTracing, call: () => Promise<T>): Promise<T> {
  return otel.context.with(otel.trace.setSpan(otel.context.active(), tracing.parent), call);
}

/** Run `body` with an SDK in this process, and remove it after `body` in every case. */
async function withTestTracing(body: (tracing: TestTracing) => Promise<void>): Promise<void> {
  const tracing = installTestTracing();

  try {
    await body(tracing);
  } finally {
    tracing.remove();
  }
}

/** Every span line that the processes of the worker wrote so far. Fails the test for a line that does not parse. */
function spanLines(suite: Suite): readonly SpanLine[] {
  const read = suite.markers.read(SPAN_LINES_MARKER);

  if (read.kind === "absent") {
    return [];
  }

  return read.text
    .split("\n")
    .filter((text) => text !== "")
    .map((text) => {
      const parsed = parseSpanLine(text);

      if (!parsed.ok) {
        assert.fail(`a span line: ${parsed.error}`);
      }

      return parsed.value;
    });
}

/** The span lines whose attribute `key` is the text `value`. */
function linesWith(suite: Suite, key: string, value: string): readonly SpanLine[] {
  return spanLines(suite).filter((line) => line.attributes[key] === value);
}

/** The span lines whose attribute `key` is the text `value`, once there is one. */
async function waitForSpans(suite: Suite, key: string, value: string): Promise<readonly SpanLine[]> {
  return pollFor(SPAN_LINE_WAIT_MS, `a span with ${key} = ${value}`, async () => {
    const matching = linesWith(suite, key, value);

    return matching.length > 0 ? found(matching) : NOT_YET;
  });
}

/** The one span of `spans`. */
function onlySpan(spans: readonly SpanLine[]): SpanLine {
  const [span, ...others] = spans;

  assert.ok(span !== undefined && others.length === 0, `one span, not ${String(spans.length)}: ${JSON.stringify(spans)}`);

  return span;
}

/** The send span of task `taskId` among the spans that ended in this process. */
function sendSpanOf(tracing: TestTracing, taskId: string): ReadableSpan {
  const sends = tracing.ended().filter((span) => span.attributes[TASK_ID_KEY] === taskId);
  const [send, ...others] = sends;

  assert.ok(send !== undefined && others.length === 0, `one send span of task ${taskId}, not ${String(sends.length)}`);

  return send;
}

/** The start span of workflow `workflowId` among the spans that ended in this process. */
function startSpanOf(tracing: TestTracing, workflowId: string): ReadableSpan {
  const starts = tracing.ended().filter((span) => span.attributes[WORKFLOW_ID_KEY] === workflowId);
  const [start, ...others] = starts;

  assert.ok(start !== undefined && others.length === 0, `one start span of workflow ${workflowId}, not ${String(starts.length)}`);

  return start;
}

/** The case of the thread seat: on Node a test of `body`; on Bun a skipped test. */
function threadSeatTest(name: string, body: (suite: Suite) => Promise<void>): void {
  switch (runtimeName()) {
    case "node":
      e2eTest(name, "worker_ended_spans", body);

      return;
    case "bun":
      test(name, { skip: THREAD_SEATS_ON_NODE_ONLY }, () => undefined);
  }
}

/** Start a worker with one thread child of 4 seats, and wait until it logged that it is ready. */
async function threadWorker(suite: Suite): Promise<void> {
  await suite.spawnWorker("thread-only-1x4").waitForLog(WORKER_READY_LINE, READY_LIMIT_MS);
}

/** Wait until the thread body of `token` wrote its started marker. */
async function threadStarted(suite: Suite, token: string): Promise<void> {
  await pollFor(STARTED_WAIT_MS, `the thread body ${token} started`, async () =>
    suite.markers.read(`${token}-started`).kind === "found" ? found(undefined) : NOT_YET,
  );
}

describe("attempt spans and the spans of the attempts that the worker ended", () => {
  e2eTest("the child of an attempt that returned a value makes its span, a child of the send span, in the trace of the sender", "worker_ended_spans", async (suite) => {
    await suite.worker("isolated-1");
    await withTestTracing(async (tracing) => {
      const handle = await inParentSpan(tracing, async () => sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 5 })));

      assert.equal(await succeeded(handle, RESULT_WAIT_MS), 10);

      const span = onlySpan(await waitForSpans(suite, TASK_ID_KEY, handle.taskId));
      const send = sendSpanOf(tracing, handle.taskId);

      assert.equal(send.name, "send e2e_simple");
      assert.equal(span.name, "run e2e_simple");
      assert.equal(span.status, "UNSET");
      assert.equal(span.traceId, tracing.parent.spanContext().traceId);
      assert.deepEqual(span.parent, { kind: "parent", spanId: send.spanContext().spanId });
      assert.equal(span.attributes[TASK_ATTEMPT_KEY], 1);
      assert.equal(span.attributes[TASK_EXECUTION_KEY], "ISOLATED");
      assert.equal(SPAN_SOURCE_KEY in span.attributes, false, "a child made the span");
    });
  });

  e2eTest("an isolated task killed at its timeout gets a span that the worker made, in the trace of the sender", "worker_ended_spans", async (suite) => {
    await suite.worker("isolated-1");
    await withTestTracing(async (tracing) => {
      const handle = await inParentSpan(tracing, async () => sentHandle("the isolated sleeper", suite.app.send(endedSpans.endedSpansSleeper, {})));

      assert.equal(await failedCode(handle, RESULT_WAIT_MS), "TASK_TIMEOUT");

      const span = onlySpan(await waitForSpans(suite, TASK_ID_KEY, handle.taskId));

      assert.equal(span.name, `run ${endedSpans.endedSpansSleeper.name}`);
      assert.equal(span.status, "ERROR");
      assert.equal(span.traceId, tracing.parent.spanContext().traceId);
      assert.equal(span.parent.kind, "parent");
      assert.ok(BigInt(span.startNanos) <= BigInt(span.endNanos), `start ${span.startNanos}, end ${span.endNanos}`);
      assert.equal(span.attributes[ERROR_TYPE_KEY], "TASK_TIMEOUT");
      assert.equal(span.attributes[SPAN_SOURCE_KEY], "worker");
      assert.equal(span.attributes[TASK_EXECUTION_KEY], "ISOLATED");
      assert.equal(span.attributes[TASK_ATTEMPT_KEY], 1);
    });
  });

  // The event of the attempt carries no workflow; the handler child reads the node by the task id, so the span has the
  // workflow attributes of the span of a child. Its parent comes from the carrier of the start span, which every row of
  // the tree stores (Python `test_a_workflow_task_killed_at_its_timeout_gets_a_span_with_its_node`).
  e2eTest("a workflow task killed at its timeout gets a span that the worker made, with its node, in the trace of the start", "worker_ended_spans", async (suite) => {
    await suite.worker("isolated-1");
    await withTestTracing(async (tracing) => {
      const started = await inParentSpan(tracing, async () => suite.app.start(endedSpans.endedSpansFlow, {}));

      assert.ok(started.ok, `start the workflow: ${started.ok ? "" : started.error.code}`);

      const workflowId = started.value.workflowId;
      const span = onlySpan(await waitForSpans(suite, WORKFLOW_ID_KEY, workflowId));
      const start = startSpanOf(tracing, workflowId);
      const run = await suite.app.monitoring.getWorkflowRun(workflowId);

      assert.equal(start.name, `start ${endedSpans.endedSpansFlow.name}`);
      assert.equal(start.kind, otel.SpanKind.PRODUCER);
      assert.equal(start.parentSpanContext?.spanId, tracing.parent.spanContext().spanId);
      assert.equal(run.ok && run.value.kind === "found" ? run.value.run.traceId : "no run", start.spanContext().traceId);
      assert.equal(span.traceId, tracing.parent.spanContext().traceId);
      assert.deepEqual(span.parent, { kind: "parent", spanId: start.spanContext().spanId });
      assert.equal(span.name, `run ${endedSpans.endedSpansSleeper.name}`);
      assert.equal(span.status, "ERROR");
      assert.equal(span.attributes[ERROR_TYPE_KEY], "TASK_TIMEOUT");
      assert.equal(span.attributes[SPAN_SOURCE_KEY], "worker");
      assert.equal(span.attributes[WORKFLOW_NODE_KEY], 0);
      assert.equal(span.attributes[WORKFLOW_ROOT_ID_KEY], workflowId);
    });
  });

  // The child of the shared run reports the timeout in its result and lives: the span of the child is the span of the
  // attempt, with the timeout, and the handler child makes none.
  e2eTest("a shared async task that its child ended at the deadline has one span, the span of its child", "worker_ended_spans", async (suite) => {
    await suite.worker("split-isolated-1-shared-1x4");
    await withTestTracing(async (tracing) => {
      const handle = await inParentSpan(tracing, async () =>
        sentHandle("the shared sleeper", suite.app.send(endedSpans.endedSpansSharedSleeper, {})),
      );

      assert.equal(await failedCode(handle, RESULT_WAIT_MS), "TASK_TIMEOUT");
      await waitForSpans(suite, TASK_ID_KEY, handle.taskId);
      await pause(SECOND_SPAN_WAIT_MS);

      const span = onlySpan(linesWith(suite, TASK_ID_KEY, handle.taskId));

      assert.equal(span.name, `run ${endedSpans.endedSpansSharedSleeper.name}`);
      assert.equal(span.status, "ERROR");
      assert.equal(span.traceId, tracing.parent.spanContext().traceId);
      assert.equal(span.attributes[ERROR_TYPE_KEY], "TASK_TIMEOUT");
      assert.equal(SPAN_SOURCE_KEY in span.attributes, false, "the child made the span");
      assert.equal(span.attributes[TASK_EXECUTION_KEY], "SHARED_ASYNC");
    });
  });

  threadSeatTest("a SHARED_THREAD body's seat isolate makes its span, a child of the send span, in the trace of the sender", async (suite) => {
    await threadWorker(suite);
    await withTestTracing(async (tracing) => {
      const input = { token: randomUUID(), holdMs: 0 };
      const handle = await inParentSpan(tracing, async () => sentHandle("the thread hold", suite.app.send(endedSpans.endedSpansThreadHold, input)));

      assert.equal(await succeeded(handle, RESULT_WAIT_MS), "held");

      const span = onlySpan(await waitForSpans(suite, TASK_ID_KEY, handle.taskId));

      assert.equal(span.name, `run ${endedSpans.endedSpansThreadHold.name}`);
      assert.equal(span.status, "UNSET");
      assert.equal(span.traceId, tracing.parent.spanContext().traceId);
      assert.deepEqual(span.parent, { kind: "parent", spanId: sendSpanOf(tracing, handle.taskId).spanContext().spanId });
      assert.equal(span.attributes[TASK_EXECUTION_KEY], "SHARED_THREAD");
      assert.equal(span.attributes[TASK_ATTEMPT_KEY], 1);
      assert.equal(SPAN_SOURCE_KEY in span.attributes, false, "the seat isolate made the span");
    });
  });

  threadSeatTest("a sync thread body past its deadline ends its span with TASK_TIMEOUT when the run settles, not at the deadline", async (suite) => {
    await threadWorker(suite);
    await withTestTracing(async (tracing) => {
      const neighbour = { token: randomUUID(), holdMs: NEIGHBOUR_HOLD_MS };

      await sentHandle("the neighbour", suite.app.send(endedSpans.endedSpansThreadHold, neighbour));
      await threadStarted(suite, neighbour.token);

      const handle = await inParentSpan(tracing, async () => sentHandle("the overrun", suite.app.send(endedSpans.endedSpansThreadOverrun, {})));

      assert.equal(await failedCode(handle, RESULT_WAIT_MS), "TASK_TIMEOUT");

      const readAt = BigInt(Date.now()) * NANOSECONDS_PER_MILLISECOND;
      const span = onlySpan(await waitForSpans(suite, TASK_ID_KEY, handle.taskId));
      const heldNanos = BigInt(endedSpans.THREAD_OVERRUN_HOLD_MS) * NANOSECONDS_PER_MILLISECOND;

      assert.equal(span.status, "ERROR");
      assert.equal(span.attributes[ERROR_TYPE_KEY], "TASK_TIMEOUT");
      assert.equal(span.traceId, tracing.parent.spanContext().traceId);
      assert.equal(SPAN_SOURCE_KEY in span.attributes, false, "the seat isolate made the span");
      assert.ok(BigInt(span.endNanos) - BigInt(span.startNanos) >= heldNanos, `the span holds the body: ${span.startNanos} to ${span.endNanos}`);
      assert.ok(BigInt(span.endNanos) <= readAt, `the span ended at the settle, before the outcome was read: ${span.endNanos}`);
    });
  });

  threadSeatTest("a seat body that ends its isolate with process.exit gets a WORKER_CRASHED span that the thread child's main thread made, in the trace of the sender", async (suite) => {
    await threadWorker(suite);
    await withTestTracing(async (tracing) => {
      const handle = await inParentSpan(tracing, async () => sentHandle("the thread exit", suite.app.send(endedSpans.endedSpansThreadExit, {})));

      assert.equal(await failedCode(handle, RESULT_WAIT_MS), "WORKER_CRASHED");

      const span = onlySpan(await waitForSpans(suite, TASK_ID_KEY, handle.taskId));

      assert.equal(span.name, `run ${endedSpans.endedSpansThreadExit.name}`);
      assert.equal(span.status, "ERROR");
      assert.equal(span.attributes[ERROR_TYPE_KEY], "WORKER_CRASHED");
      assert.equal(span.attributes[SPAN_SOURCE_KEY], "worker");
      assert.equal(span.attributes[TASK_EXECUTION_KEY], "SHARED_THREAD");
      assert.equal(span.attributes[TASK_ATTEMPT_KEY], 1);
      assert.equal(span.traceId, tracing.parent.spanContext().traceId);
      assert.deepEqual(span.parent, { kind: "parent", spanId: sendSpanOf(tracing, handle.taskId).spanContext().spanId });
      assert.ok(BigInt(span.startNanos) <= BigInt(span.endNanos), `start ${span.startNanos}, end ${span.endNanos}`);
    });
  });
});

/** The enqueue policy of the rerun in the case below. */
const RERUN_POLICY: RerunEnqueuePolicy = {
  retention: { kind: "forever" },
  retainRerunInput: "keep",
  reservationWindow: Temporal.Duration.from({ days: 1 }),
};

describe("the carrier of a rerun", () => {
  e2eTest("a rerun in a span stores the carrier of the active context, not the carrier of the source task", "worker_ended_spans", async (suite) => {
    await suite.worker("isolated-1");

    await withTestTracing(async (tracing) => {
      const source = await inParentSpan(tracing, async () => sentHandle("e2e_error", suite.app.send(basic.deliberateError, {})));

      await failedCode(source, RESULT_WAIT_MS);

      const caller = otel.trace.getTracer("e2e").startSpan("rerun caller");

      const rerun = await otel.context.with(otel.trace.setSpan(otel.context.active(), caller), async () =>
        suite.app.rerunTask({ sourceTaskId: source.taskId, goodUntil: { kind: "none" } }, RERUN_POLICY),
      );

      caller.end();
      assert.ok(rerun.ok && rerun.value.kind === "enqueued", `the rerun: ${JSON.stringify(rerun)}`);

      const sourceTrace = await suite.app.monitoring.getTaskDetail(source.taskId);
      const rerunTrace = await suite.app.monitoring.getTaskDetail(rerun.value.newTaskId);

      assert.ok(sourceTrace.ok && sourceTrace.value.kind === "found" && rerunTrace.ok && rerunTrace.value.kind === "found");
      assert.equal(rerunTrace.value.taskDetail.leaf.traceId, caller.spanContext().traceId);
      assert.equal(sourceTrace.value.taskDetail.leaf.traceId, tracing.parent.spanContext().traceId);
      assert.notEqual(rerunTrace.value.taskDetail.leaf.traceId, sourceTrace.value.taskDetail.leaf.traceId);
    });
  });
});
