// `currentTask()`: `running` inside a body, also after an `await` and in a timer, with the ids of the attempt span;
// `noSpanIds` when the app does not trace; `noTask` outside every body.

import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, beforeEach, describe, test } from "node:test";

import { NO_BODY_OBSERVER, type BodyObserver } from "../../../../horsies/src/child/body-observer.ts";
import { type RunMark, runStore } from "../../../../horsies/src/child/run-store.ts";
import { taskIdFromCore } from "../../../../horsies/src/contract/ids.ts";
import type { BodyFactsRecord } from "../../../../horsies/src/native/child/addon.ts";
import { otelBodyObserver } from "../../../../horsies/src/telemetry/attempt-span.ts";
import { currentTask, type CurrentTask } from "../../../../horsies/src/telemetry/log-context.ts";
import { installSdk, loadedApi, onlySpan, recordedWarnings, type TestSdk } from "../support.ts";

const MARK: RunMark = {
  taskId: taskIdFromCore("0192f6a1-7b3c-7d4e-8f00-0123456789ab", "the currentTask tests"),
  taskName: "resize",
  attempt: 1,
};

const FACTS: BodyFactsRecord = {
  kind: "recorded",
  taskId: MARK.taskId,
  taskName: MARK.taskName,
  queueName: "default",
  attempt: 1,
  workerId: "worker-1",
  execution: "ISOLATED",
  workflow: { kind: "notInWorkflow" },
  carrier: { kind: "noCarrier" },
};

/** The task that `currentTask()` gives at three places of one body: at its start, after an `await`, in a timer. */
async function seenInBody(observer: BodyObserver, facts: BodyFactsRecord): Promise<readonly CurrentTask[]> {
  const seen: CurrentTask[] = [];
  const observation = observer.enter(facts);

  await runStore.run(MARK, async () =>
    observation.run(async () => {
      seen.push(currentTask());
      await sleep(1);
      seen.push(currentTask());
      await new Promise<void>((resolve) => {
        setTimeout(() => {
          seen.push(currentTask());
          resolve();
        }, 1);
      });
    }),
  );
  observation.end({ kind: "returned" });

  return seen;
}

describe("currentTask with tracing", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("running inside a body, after an await and in a timer, with the ids of the attempt span", async () => {
    const observer = otelBodyObserver({ api: loadedApi(), parentage: "childOfSend", warnings: recordedWarnings().warnings });
    const seen = await seenInBody(observer, FACTS);
    const { traceId, spanId } = onlySpan(sdk.spans(), "run resize").spanContext();
    const expected: CurrentTask = { kind: "running", ...MARK, span: { kind: "spanIds", traceId, spanId } };

    assert.deepEqual(seen, [expected, expected, expected]);
  });
});

describe("currentTask with an SDK that gives upper-case hex ids", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk("upperCaseIds");
  });

  afterEach(() => {
    sdk.remove();
  });

  test("the span ids are lower-case hex", async () => {
    const observer = otelBodyObserver({ api: loadedApi(), parentage: "childOfSend", warnings: recordedWarnings().warnings });
    const [seen] = await seenInBody(observer, FACTS);
    const { traceId, spanId } = onlySpan(sdk.spans(), "run resize").spanContext();

    assert.match(traceId, /[A-F]/u, "the SDK gives upper-case digits");
    assert.deepEqual(seen?.kind === "running" ? seen.span : undefined, {
      kind: "spanIds",
      traceId: traceId.toLowerCase(),
      spanId: spanId.toLowerCase(),
    });
  });
});

describe("currentTask with no tracing", () => {
  test("running with no span ids", async () => {
    const seen = await seenInBody(NO_BODY_OBSERVER, { kind: "notRecorded" });

    assert.deepEqual(seen, Array(3).fill({ kind: "running", ...MARK, span: { kind: "noSpanIds" } }));
  });

  test("noTask outside every body", () => {
    assert.deepEqual(currentTask(), { kind: "noTask" });
  });
});
