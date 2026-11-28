// The checks of one minor of `@opentelemetry/api`, run in a process of their own: the API keeps one registration
// container per process, and its version is the version of the module that registers first, so two minors cannot
// register in one process. Usage: `api-minors-probe.ts <installed|floor>`. Exits 0 when every check holds; a failed
// check throws, so the process exits with a non-zero status and the cause on stderr.

import assert from "node:assert/strict";

import * as installed from "@opentelemetry/api";
import * as floor from "@opentelemetry/api-floor";

import { taskIdFromCore } from "../../../../horsies/src/contract/ids.ts";
import type { BodyFactsRecord } from "../../../../horsies/src/native/child/addon.ts";
import { otelBodyObserver } from "../../../../horsies/src/telemetry/attempt-span.ts";
import type { OtelApi } from "../../../../horsies/src/telemetry/otel-api.ts";
import { otelSendObserver } from "../../../../horsies/src/telemetry/send-span.ts";
import { ok } from "../../../../horsies/src/wire/result.ts";
import { TASK_ID } from "../../app/support.ts";
import { type ApiGlobals, installSdk, onlySpan, recordedNotMade, recordedWarnings } from "../support.ts";
import { FLOOR_VERSION, MINOR_NAMES, type MinorName } from "./minors.ts";

/** The installed minor, as the package reads it. The assignment fails typecheck if a member does not fit. */
const INSTALLED_API: OtelApi = installed;

/** The floor minor, as the package reads it. The assignment fails typecheck if a member does not fit. */
const FLOOR_API: OtelApi = floor;

const FACTS = { taskName: "report", queueName: "default", taskId: TASK_ID };

/** The key of the registration container of the API 1.x on `globalThis`. */
const REGISTRATION_KEY = Symbol.for("opentelemetry.js.api.1");

/** The module of `minor` as the package's API and as the holder of the globals. */
function moduleOf(minor: MinorName): { readonly api: OtelApi; readonly globals: ApiGlobals } {
  switch (minor) {
    case "installed":
      return { api: INSTALLED_API, globals: installed };
    case "floor":
      return { api: FLOOR_API, globals: floor };
    default: {
      const unexpected: never = minor;

      throw new TypeError(`moduleOf: a minor outside its type: ${String(unexpected)}`);
    }
  }
}

/** The facts of a body whose task has the stored carrier `carrier`. */
function bodyFacts(carrier: Extract<BodyFactsRecord, { kind: "recorded" }>["carrier"]): BodyFactsRecord {
  return {
    kind: "recorded",
    taskId: TASK_ID,
    taskName: "report",
    queueName: "default",
    attempt: 1,
    workerId: "worker-1",
    execution: "ISOLATED",
    workflow: { kind: "notInWorkflow" },
    carrier,
  };
}

/** The version that the registration container of the API holds. */
function registeredVersion(): unknown {
  const container: unknown = Reflect.get(globalThis, REGISTRATION_KEY);

  return container instanceof Object ? Reflect.get(container, "version") : undefined;
}

/** Runs the checks on `minor`; throws at the first check that fails. */
async function check(minor: MinorName): Promise<void> {
  const { api, globals } = moduleOf(minor);
  const sdk = installSdk("sdkIds", globals);

  if (minor === "floor") {
    assert.equal(registeredVersion(), FLOOR_VERSION, "the floor module registered the globals");
  } else {
    assert.notEqual(registeredVersion(), FLOOR_VERSION, "the installed module registered the globals");
  }

  const { notMade, written } = recordedNotMade();
  const sender = otelSendObserver({ api, warnings: recordedWarnings().warnings, notMade, clock: () => performance.now() });
  let traceIdHex: string | undefined;
  let carrier = new Map<string, string>();

  void (await sender.observeSend(FACTS, async (trace) => {
    traceIdHex = trace.kind === "carrier" ? trace.traceIdHex : undefined;
    carrier = new Map(trace.kind === "carrier" ? Object.entries(trace.carrier) : []);

    return ok(taskIdFromCore(TASK_ID, "the api minors probe"));
  }));

  const send = onlySpan(sdk.spans(), "send report").spanContext();

  assert.equal(traceIdHex, send.traceId, "the carrier holds the trace of the send span");

  const metrics = await sdk.metrics();
  const sent = metrics.scopeMetrics.flatMap((scope) => scope.metrics).find((metric) => metric.descriptor.name === "horsies.task.sent");

  assert.equal(sent?.descriptor.valueType, api.ValueType.INT, "the send counter counts whole numbers");

  const entries = [...carrier].map(([key, value]) => ({ key, value }));

  for (const parentage of ["childOfSend", "linkedToSend"] as const) {
    const body = otelBodyObserver({ api, parentage, warnings: recordedWarnings().warnings, notMade });
    const observation = body.enter(bodyFacts({ kind: "carrier", entries }));

    await observation.run(async () => undefined);
    observation.end({ kind: "returned" });
  }

  const [child, linked, ...rest] = sdk.spans().filter((span) => span.name === "run report");

  assert.ok(child !== undefined && linked !== undefined && rest.length === 0, "two attempt spans ended");
  assert.equal(child.spanContext().traceId, send.traceId, "childOfSend: the same trace");
  assert.equal(child.parentSpanContext?.spanId, send.spanId, "childOfSend: the send span is the parent");
  assert.notEqual(linked.spanContext().traceId, send.traceId, "linkedToSend: a new trace");
  assert.deepEqual(linked.links.map((link) => link.context.spanId), [send.spanId], "linkedToSend: a link to the send span");
  assert.deepEqual(written, [], "no span was reported as not made");
  sdk.remove();
}

const [, , requested, ...extra] = process.argv;
const minor = MINOR_NAMES.find((name) => name === requested);

if (minor === undefined || extra.length > 0) {
  process.stderr.write(`usage: api-minors-probe.ts <${MINOR_NAMES.join("|")}>\n`);
  process.exit(2);
}

await check(minor);
