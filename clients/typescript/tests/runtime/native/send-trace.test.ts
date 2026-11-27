// The trace carrier of a send through the addon, with a database: a send inside a span passes its carrier to the
// core and succeeds; a carrier over the core's bounds is not stored, and the send still succeeds; an SDK whose ids are
// upper-case hex (the API takes them as valid) changes no send and no rerun.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`) and `HORSIES_TEST_DATABASE_URL` names a
// throwaway PostgreSQL database.

import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";

import * as otel from "@opentelemetry/api";
import pg from "pg";

import { TaskId } from "../../../horsies/src/contract/ids.ts";
import { defineTask, h, Horsies } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { mintTaskId } from "../../../horsies/src/native/process-core/process-core.ts";
import { installSdk, onlySpan, type TestSdk } from "../../unit/telemetry/support.ts";
import { testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

const ping = defineTask({ name: "send_trace_ping", input: { n: h.int() }, output: h.int() });

/** A value over the core's bound of 1,024 bytes per carrier value. */
const OVER_BOUND_VALUE = "x".repeat(1025);

/** A propagator that writes `traceparent` and one value over the core's bound. */
const OVER_BOUND_PROPAGATOR: otel.TextMapPropagator = {
  inject: (_context, carrier, setter) => {
    setter.set(carrier, "traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    setter.set(carrier, "baggage", OVER_BOUND_VALUE);
  },
  extract: (context) => context,
  fields: () => ["traceparent", "baggage"],
};

/** The stored carrier text of task `taskId`: `null` when the row has none. Effects: one connection to the test database. */
async function storedCarrier(taskId: string): Promise<string | null> {
  const client = new pg.Client({ connectionString: testDatabaseUrl() });

  await client.connect();

  try {
    const result = await client.query<{ trace_carrier: string | null }>("SELECT trace_carrier FROM horsies_tasks WHERE id = $1", [taskId]);

    assert.equal(result.rows.length, 1, `one row for task ${taskId}`);

    return result.rows[0]?.trace_carrier ?? null;
  } finally {
    await client.end();
  }
}

/** The trace id that the detail read of `taskId` gives: `null` for a task with no trace. */
async function detailTraceId(app: Horsies, taskId: TaskId): Promise<string | null> {
  const read = await app.monitoring.getTaskDetail(taskId);

  assert.ok(read.ok && read.value.kind === "found", `the detail of task ${taskId}`);

  return read.value.taskDetail.leaf.traceId;
}

function tracedApp(): Horsies {
  const created = Horsies.create({
    database: { databaseUrl: { kind: "value", value: testDatabaseUrl() } },
    tracing: { kind: "otelTracing" },
    tasks: [ping],
  });

  if (!created.ok) {
    assert.fail(`Horsies.create refused: ${created.error.refusals.map((refusal) => refusal.message).join("; ")}`);
  }

  return created.value;
}

describe("the trace carrier of a send with a database", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("a send in a span gives a handle, and its send span ends with no error", async () => {
    const sent = await tracedApp().send(ping, { n: 1 }, { delaySeconds: 3_600 });

    assert.equal(sent.ok, true);
    assert.equal(onlySpan(sdk.spans(), "send send_trace_ping").status.code, otel.SpanStatusCode.UNSET);
  });

  test("the task detail traceId of a send in a span equals the trace id of the send span", async () => {
    const app = tracedApp();
    const sent = await app.send(ping, { n: 4 }, { delaySeconds: 3_600 });

    assert.ok(sent.ok);
    assert.equal(await detailTraceId(app, sent.value.taskId), onlySpan(sdk.spans(), "send send_trace_ping").spanContext().traceId);
  });

  test("a send with a propagator under the bounds stores the carrier", async () => {
    const app = tracedApp();
    const sent = await app.send(ping, { n: 5 }, { delaySeconds: 3_600 });

    assert.ok(sent.ok);
    assert.match(String(await storedCarrier(sent.value.taskId)), /"traceparent"/u);
  });

  test("a carrier over the core's bounds is not stored, and the send succeeds", async () => {
    // The installed SDK registered a propagator; the API keeps the first one, so remove it before the next.
    otel.propagation.disable();
    assert.equal(otel.propagation.setGlobalPropagator(OVER_BOUND_PROPAGATOR), true, "the over-bound propagator is the global one");

    const sent = await tracedApp().send(ping, { n: 2 }, { delaySeconds: 3_600 });

    assert.ok(sent.ok);
    assert.equal(await storedCarrier(sent.value.taskId), null);
  });
});

/** A UUIDv7 that no task has: minted now and never sent. */
function unsentTaskId(): TaskId {
  const minted = mintTaskId();

  assert.ok(minted.kind === "minted");

  const parsed = TaskId.parse(minted.taskId);

  assert.ok(parsed.ok, "a canonical UUID text");

  return parsed.value;
}

describe("an SDK that gives upper-case hex ids, with a database", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk("upperCaseIds");
  });

  afterEach(() => {
    sdk.remove();
  });

  test("a send inside a span succeeds", async () => {
    const caller = otel.trace.getTracer("caller").startSpan("caller");

    const sent = await otel.context.with(otel.trace.setSpan(otel.context.active(), caller), async () =>
      tracedApp().send(ping, { n: 3 }, { delaySeconds: 3_600 }),
    );

    caller.end();
    assert.equal(sent.ok ? "sent" : sent.error.code, "sent");
    assert.match(onlySpan(sdk.spans(), "send send_trace_ping").spanContext().traceId, /[A-F]/u);
  });

  test("a rerun inside a span reaches the core: an unsent id is sourceAbsent", async () => {
    const sourceTaskId = unsentTaskId();
    const caller = otel.trace.getTracer("caller").startSpan("caller");

    const answer = await otel.context.with(otel.trace.setSpan(otel.context.active(), caller), async () =>
      tracedApp().rerunTask(
        { sourceTaskId, goodUntil: { kind: "none" } },
        { retention: { kind: "forever" }, retainRerunInput: "keep", reservationWindow: Temporal.Duration.from({ days: 1 }) },
      ),
    );

    caller.end();
    assert.deepEqual(answer, { ok: true, value: { kind: "sourceAbsent", taskId: sourceTaskId, reason: "WITHIN_RETAINED_HISTORY" } });
  });
});

describe("a send with no SDK, with a database", () => {
  beforeEach(() => {
    otel.propagation.disable();
    otel.trace.disable();
  });

  test("the task detail gives traceId null and no carrier is stored", async () => {
    const app = tracedApp();
    const sent = await app.send(ping, { n: 6 }, { delaySeconds: 3_600 });

    assert.ok(sent.ok);
    assert.equal(await detailTraceId(app, sent.value.taskId), null);
    assert.equal(await storedCarrier(sent.value.taskId), null);
  });
});
