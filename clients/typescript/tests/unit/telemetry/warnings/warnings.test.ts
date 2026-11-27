// The warnings of the telemetry that are not a span not made: a point of the producer metrics, the carrier of a rerun
// and the flush of the tracer provider. Each kind is written once per process, with its own text; a throw of the SDK
// never changes a send; the flush never rejects.

import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";

import * as otel from "@opentelemetry/api";

import { taskIdFromCore } from "../../../../horsies/src/contract/ids.ts";
import { flushTracerProvider } from "../../../../horsies/src/telemetry/flush.ts";
import type { OtelApi } from "../../../../horsies/src/telemetry/otel-api.ts";
import { otelSendObserver } from "../../../../horsies/src/telemetry/send-span.ts";
import { TelemetryWarnings } from "../../../../horsies/src/telemetry/warnings.ts";
import { ok } from "../../../../horsies/src/wire/result.ts";
import { TASK_ID } from "../../app/support.ts";
import { installSdk, loadedApi, recordedNotMade, recordedWarnings, type TestSdk } from "../support.ts";

const FACTS = { taskName: "report", queueName: "default", taskId: TASK_ID };
const SENT = ok(taskIdFromCore(TASK_ID, "the warnings test"));

describe("the warnings of one process", () => {
  test("each kind has its own text, written once; a later warning of that kind is not written", () => {
    const { warnings, lines } = recordedWarnings();

    warnings.warnOnce("producerMetric", new Error("one"));
    warnings.warnOnce("producerMetric", new Error("two"));
    warnings.warnOnce("rerunCarrier", "a text");
    warnings.warnOnce("flush", new TypeError("three"));

    assert.deepEqual(lines, [
      "horsies telemetry: a point of the producer metrics was not recorded: Error: one. Later warnings of this kind are not written.",
      "horsies telemetry: the carrier of a rerun did not inject; the rerun goes on with no carrier: a text. Later warnings of this kind are not written.",
      "horsies telemetry: the flush of the tracer provider failed: TypeError: three. Later warnings of this kind are not written.",
    ]);
  });

  test("the set of one process does not hold back the warnings of another", () => {
    const lines: string[] = [];

    new TelemetryWarnings((line) => lines.push(line)).warnOnce("flush", "x");
    new TelemetryWarnings((line) => lines.push(line)).warnOnce("flush", "x");
    assert.equal(lines.length, 2);
  });
});

describe("a throw of the SDK in the producer metrics and the rerun carrier", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("a meter that throws is one producerMetric warning; each send gives its result unchanged", async () => {
    const api = loadedApi();

    const broken: OtelApi = {
      ...api,
      metrics: {
        getMeter: () => {
          throw new Error("the meter is broken");
        },
      },
    };

    const { warnings, lines } = recordedWarnings();
    const observer = otelSendObserver({ api: broken, warnings, notMade: recordedNotMade().notMade, clock: () => performance.now() });

    for (let send = 0; send < 2; send += 1) {
      assert.equal(await observer.observeSend(FACTS, async () => SENT), SENT);
    }

    assert.equal(lines.length, 1);
    assert.match(lines[0] ?? "", /a point of the producer metrics was not recorded: Error: the meter is broken/u);
  });

  test("a propagator that throws in a rerun is one rerunCarrier warning and no carrier", () => {
    const api = loadedApi();

    const broken: OtelApi = {
      ...api,
      propagation: {
        ...api.propagation,
        inject: () => {
          throw new Error("the propagator is broken");
        },
      },
    };

    const { warnings, lines } = recordedWarnings();
    const observer = otelSendObserver({ api: broken, warnings, notMade: recordedNotMade().notMade, clock: () => performance.now() });
    const caller = otel.trace.getTracer("caller").startSpan("caller");

    const carriers = otel.context.with(otel.trace.setSpan(otel.context.active(), caller), () => [
      observer.activeCarrier(),
      observer.activeCarrier(),
    ]);

    caller.end();
    assert.deepEqual(carriers, [{ kind: "noCarrier" }, { kind: "noCarrier" }]);
    assert.equal(lines.length, 1);
    assert.match(lines[0] ?? "", /the carrier of a rerun did not inject; the rerun goes on with no carrier: Error: the propagator is broken/u);
  });
});

/** The API whose global tracer provider is `provider`. */
function apiWithProvider(provider: object): OtelApi {
  const api = loadedApi();

  // SAFETY: the test gives a provider object with only the members that the flush reads.
  return { ...api, trace: { ...api.trace, getTracerProvider: () => provider as ReturnType<OtelApi["trace"]["getTracerProvider"]> } };
}

describe("the flush of the tracer provider", () => {
  test("a provider whose flush rejects is one flush warning, and the flush resolves", async () => {
    const { warnings, lines } = recordedWarnings();
    const api = apiWithProvider({ forceFlush: async () => Promise.reject(new Error("the exporter is down")) });

    await flushTracerProvider({ api, warnings });
    await flushTracerProvider({ api, warnings });
    assert.equal(lines.length, 1);
    assert.match(lines[0] ?? "", /the flush of the tracer provider failed: Error: the exporter is down/u);
  });

  test("a provider whose flush throws at once is one flush warning, and the flush resolves", async () => {
    const { warnings, lines } = recordedWarnings();

    await flushTracerProvider({
      api: apiWithProvider({
        forceFlush: () => {
          throw new Error("no flush");
        },
      }),
      warnings,
    });
    assert.equal(lines.length, 1);
  });

  test("a provider with no forceFlush has nothing to flush: no warning", async () => {
    const { warnings, lines } = recordedWarnings();

    await flushTracerProvider({ api: apiWithProvider({}), warnings });
    assert.deepEqual(lines, []);
  });

  test("a proxy provider flushes its delegate", async () => {
    const { warnings, lines } = recordedWarnings();
    let flushed = 0;

    const delegate = {
      forceFlush: async (): Promise<void> => {
        flushed += 1;
      },
    };

    await flushTracerProvider({ api: apiWithProvider({ getDelegate: () => delegate }), warnings });
    assert.equal(flushed, 1);
    assert.deepEqual(lines, []);
  });
});
