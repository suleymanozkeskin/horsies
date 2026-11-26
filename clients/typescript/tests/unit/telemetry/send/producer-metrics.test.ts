// The producer metrics: one point of each per send call, resends included; the `result` label `ok` and a code; the
// bucket bounds of the catalog; no point for a refusal before the database.

import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";

import { type DataPoint, DataPointType, type Histogram, type MetricData, type ResourceMetrics } from "@opentelemetry/sdk-metrics";

import { TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS } from "../../../../horsies/src/_catalog/producer_metrics.ts";
import { checkContractList, contractLookup } from "../../../../horsies/src/app/contract-list.ts";
import { SuppressionState } from "../../../../horsies/src/app/suppression.ts";
import { defineTask } from "../../../../horsies/src/contract/public.ts";
import { type SendContext, sendTask } from "../../../../horsies/src/send/send.ts";
import { otelSendObserver } from "../../../../horsies/src/telemetry/send-span.ts";
import { h } from "../../../../horsies/src/wire/public.ts";
import { fakeCoreHandle, fakeProducer, type ProducerScript, rawEnqueueFailed } from "../../app/support.ts";
import { installSdk, loadedApi, recordedNotMade, recordedWarnings, type TestSdk } from "../support.ts";

const report = defineTask({ name: "report", input: { n: h.int() }, output: h.int() });

function contextOf(script: ProducerScript, resend: boolean): SendContext {
  const list = checkContractList([report], { kind: "default" });
  const core = { kind: "addon", producer: fakeProducer(script), handle: fakeCoreHandle() } as const;

  const observer = otelSendObserver({
    api: loadedApi(),
    warnings: recordedWarnings().warnings,
    notMade: recordedNotMade().notMade,
    clock: () => performance.now(),
  });

  return {
    contracts: contractLookup(list.entries),
    resendOnTransientError: resend,
    core,
    observer,
    suppression: new SuppressionState({ kind: "open" }, () => undefined),
    pause: async () => undefined,
    now: () => Temporal.Now.instant(),
  };
}

/** The metric `name`, if the meter provider holds it. */
function metricOf(metrics: ResourceMetrics, name: string): MetricData | undefined {
  return metrics.scopeMetrics.flatMap((scope) => scope.metrics).find((metric) => metric.descriptor.name === name);
}

/** The points of the counter `horsies.task.sent`. */
function sentPoints(metrics: ResourceMetrics): readonly DataPoint<number>[] {
  const found = metricOf(metrics, "horsies.task.sent");

  return found?.dataPointType === DataPointType.SUM ? found.dataPoints : [];
}

/** The points of the histogram `horsies.task.send.duration`. */
function durationPoints(metrics: ResourceMetrics): readonly DataPoint<Histogram>[] {
  const found = metricOf(metrics, "horsies.task.send.duration");

  return found?.dataPointType === DataPointType.HISTOGRAM ? found.dataPoints : [];
}

describe("the producer metrics", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("a send that the core takes is one point of each, with the result ok", async () => {
    assert.equal((await sendTask(contextOf({}, false), report, { n: 1 }, {})).ok, true);

    const metrics = await sdk.metrics();
    const sent = sentPoints(metrics);
    const durations = durationPoints(metrics);

    assert.deepEqual(
      sent.map((point) => [point.attributes, point.value]),
      [[{ "horsies.task.name": "report", "horsies.queue": "default", "horsies.result": "ok" }, 1]],
    );
    assert.deepEqual(
      durations.map((point) => [point.attributes, point.value.count]),
      [[{ "horsies.queue": "default" }, 1]],
    );
  });

  test("a failed send has its code as the result, and the resends of one call are one point", async () => {
    const failed = { kind: "failed", error: rawEnqueueFailed("report", true) } as const;

    const outcome = await sendTask(contextOf({ enqueued: [failed] }, true), report, { n: 1 }, {});

    assert.equal(outcome.ok ? "sent" : outcome.error.code, "ENQUEUE_FAILED");

    const sent = sentPoints(await sdk.metrics());

    assert.deepEqual(
      sent.map((point) => [point.attributes["horsies.result"], point.value]),
      [["ENQUEUE_FAILED", 1]],
    );
  });

  test("the duration buckets are the bounds of the catalog", async () => {
    assert.equal((await sendTask(contextOf({}, false), report, { n: 1 }, {})).ok, true);

    const [duration] = durationPoints(await sdk.metrics());

    assert.deepEqual(duration?.value.buckets.boundaries, [...TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS]);
  });

  test("a refusal before the database records no point", async () => {
    const sent = await sendTask(contextOf({}, false), report, { n: "not an int" }, {});

    assert.equal(sent.ok, false);
    assert.deepEqual(sentPoints(await sdk.metrics()), []);
  });
});
