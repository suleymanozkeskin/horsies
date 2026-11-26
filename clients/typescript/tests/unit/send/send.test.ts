// The send path: each send error code, the facts of the request, the resend rule, `retrySend`, the hidden payload,
// the suppression of sends, and a send in a worker thread with no core.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { createHorsies } from "../../../horsies/src/app.ts";
import { checkContractList, contractLookup } from "../../../horsies/src/app/contract-list.ts";
import type { SendObserver } from "../../../horsies/src/app/observers.ts";
import { NO_OP_OBSERVER } from "../../../horsies/src/app/observers.ts";
import {
  initialSuppression,
  type ProcessStart,
  type SendSuppression,
  servingSuppression,
  SuppressionState,
} from "../../../horsies/src/app/suppression.ts";
import type { QueueSetup } from "../../../horsies/src/config/queues.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { TraceArgument } from "../../../horsies/src/native/values/records.ts";
import { sendErrorOfRaw, type RetryableSendError, type TaskSendError } from "../../../horsies/src/send/errors.ts";
import type { SendOptions } from "../../../horsies/src/send/options.ts";
import { retrySend, type SendContext, sendTask } from "../../../horsies/src/send/send.ts";
import { AddonOutsideMainThread } from "../../../horsies/src/thrown-errors.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import {
  DATABASE_URL,
  fakeEnvironment,
  fakeProducer,
  fakeCoreHandle,
  type FakeProducer,
  OPTIONS_TEXT,
  PLAIN_START,
  PREPARED,
  preparedWith,
  type ProducerScript,
  rawEnqueueFailed,
  rawKeyConflict,
  rawPayloadMismatch,
  rawPayloadTooLarge,
  rawValidationFailed,
  SENT_AT_MICROS,
  TASK_ID,
} from "../app/support.ts";

const report = defineTask({ name: "report", input: { n: h.int() }, output: h.int() });
const unlisted = defineTask({ name: "unlisted", input: {}, output: h.null() });
const upstreamOnly = defineTask({ name: "upstream_only", input: { upstream: h.resultOf(h.int()) }, output: h.null() });
const billed = defineTask({ name: "billed", queue: "billing", input: {}, output: h.null() });

/** The sent instant of the tests: `SENT_AT_MICROS` and 999 nanoseconds, which the request cuts. */
const NOW = Temporal.Instant.fromEpochNanoseconds(SENT_AT_MICROS * 1000n + 999n);

type Harness = {
  readonly context: SendContext;
  readonly producer: FakeProducer;
  readonly pauses: number[];
};

type HarnessParts = {
  readonly script?: ProducerScript;
  readonly resend?: boolean;
  readonly suppression?: SendSuppression;
  readonly observer?: SendObserver;
  readonly queues?: QueueSetup;
  readonly contracts?: readonly object[];
};

function harness(parts: HarnessParts = {}): Harness {
  const producer = fakeProducer(parts.script);
  const pauses: number[] = [];
  const list = checkContractList(parts.contracts ?? [report, upstreamOnly], parts.queues ?? { kind: "default" });

  assert.deepEqual(list.refusals, []);

  const context: SendContext = {
    contracts: contractLookup(list.entries),
    resendOnTransientErr: parts.resend ?? false,
    core: { kind: "addon", producer, handle: fakeCoreHandle() },
    observer: parts.observer ?? NO_OP_OBSERVER,
    suppression: new SuppressionState(parts.suppression ?? { kind: "open" }, () => undefined),
    pause: async (milliseconds) => {
      pauses.push(milliseconds);
    },
    now: () => NOW,
  };

  return { context, producer, pauses };
}

async function sendReport(
  context: SendContext,
  input: object = { n: 1 },
  options: SendOptions = {},
): Promise<TaskSendError<number, string> | "sent"> {
  const sent = await sendTask<number, string>(context, report, input, options);

  return sent.ok ? "sent" : sent.error;
}

function codeOf(outcome: TaskSendError<number, string> | "sent"): string {
  return outcome === "sent" ? "sent" : outcome.code;
}

function retryable(raw: Parameters<typeof sendErrorOfRaw>[0]): RetryableSendError<number, string> {
  const error = sendErrorOfRaw(raw);

  if (error.code !== "ENQUEUE_FAILED") {
    throw new Error(`not a retryable error: ${error.code}`);
  }

  return error;
}

describe("a send that the core takes", () => {
  test("gives a handle of the minted task id, with one FIRST enqueue", async () => {
    const { context, producer } = harness();
    const sent = await sendTask<number, string>(context, report, { n: 1 }, {});

    assert.equal(sent.ok ? sent.value.taskId : "", TASK_ID);
    assert.deepEqual(
      producer.enqueues.map((call) => call.sendKind),
      ["FIRST"],
    );
  });

  test("the prepare facts: the task name, the retention choice, the deadline and the input's UTF-8 bytes", async () => {
    const { context, producer } = harness();
    const goodUntil = Temporal.Instant.fromEpochNanoseconds(SENT_AT_MICROS * 1000n + 60_000_000_999n);

    await sendReport(context, { n: 1 }, { goodUntil, retention: { kind: "days", days: 7 } });
    assert.deepEqual(producer.prepares, [
      {
        taskName: "report",
        facts: {
          retention: { kind: "days", days: 7 },
          deadline: { kind: "until", epochMicros: SENT_AT_MICROS + 60_000_000n },
          inputBytes: 7,
        },
      },
    ]);
  });

  test("the request: the default queue route and its priority 100, the prepared facts, the input text, the cut sent instant", async () => {
    const { context, producer } = harness();

    await sendReport(context, { n: 1 }, { idempotencyKey: "key-1", delaySeconds: 30 });
    assert.deepEqual(producer.enqueues[0]?.request, {
      taskId: TASK_ID,
      taskName: "report",
      queueName: "default",
      priority: 100,
      inputJson: '{"n":1}',
      sentAtMicros: SENT_AT_MICROS,
      deadline: { kind: "noDeadline" },
      delay: { kind: "seconds", seconds: 30 },
      taskOptions: { kind: "text", text: OPTIONS_TEXT },
      idempotencyKey: { kind: "text", text: "key-1" },
      retention: PREPARED.kind === "prepared" ? PREPARED.retention : { kind: "forever" },
    });
  });

  test("a custom queue gives its name, and the priority that the core's accepted config holds for it", async () => {
    const queues: QueueSetup = {
      kind: "custom",
      queues: new Map([["billing", { name: "billing", maxConcurrency: { kind: "coreDefault" } }]]),
    };

    const { context, producer } = harness({
      queues,
      contracts: [billed],
      script: { prepared: [preparedWith({ kind: "configured", priority: 7 })] },
    });

    const sent = await sendTask(context, billed, {}, {});

    assert.equal(sent.ok, true);
    assert.deepEqual(
      [producer.enqueues[0]?.request.queueName, producer.enqueues[0]?.request.priority],
      ["billing", 7],
    );
  });

  test("the observer wraps the enqueue: it gets the facts, and its carrier goes to the core", async () => {
    const carrier: TraceArgument = { kind: "carrier", carrier: { traceparent: "00-a-b-01" }, traceIdHex: "a".repeat(32) };
    const observed: object[] = [];

    const observer: SendObserver = {
      observeSend: async (facts, run) => {
        observed.push(facts);

        return run(carrier);
      },
      activeCarrier: () => carrier,
    };

    const { context, producer } = harness({ observer });

    await sendReport(context);
    assert.deepEqual(observed, [{ taskName: "report", queueName: "default", taskId: TASK_ID }]);
    assert.equal(producer.enqueues[0]?.trace, carrier);
  });
});

describe("the send error codes", () => {
  test("SEND_SUPPRESSED, with no prepare and no enqueue", async () => {
    const { context, producer } = harness({ suppression: { kind: "suppressed", reason: "checkCommand" } });

    assert.equal(codeOf(await sendReport(context)), "SEND_SUPPRESSED");
    assert.deepEqual([producer.prepares.length, producer.enqueues.length], [0, 0]);
  });

  test("VALIDATION_FAILED for a contract that the app does not list, with the precondition cause", async () => {
    const { context, producer } = harness();
    const sent = await sendTask(context, unlisted, {}, {});
    const error = sent.ok ? undefined : sent.error;

    assert.equal(error?.code, "VALIDATION_FAILED");
    assert.deepEqual(error?.code === "VALIDATION_FAILED" ? [error.exception.kind, "failure" in error.exception ? error.exception.failure : ""] : [], [
      "coreFailure",
      "preconditionFailed",
    ]);
    assert.equal(producer.enqueues.length, 0);
  });

  test("VALIDATION_FAILED for a refused option, with a rule cause and no prepare", async () => {
    const { context, producer } = harness();
    const error = await sendReport(context, { n: 1 }, { delaySeconds: -1 });

    assert.equal(codeOf(error), "VALIDATION_FAILED");
    assert.equal(error !== "sent" && error.code === "VALIDATION_FAILED" ? error.exception.kind : "", "ruleRefused");
    assert.equal(producer.prepares.length, 0);
  });

  test("VALIDATION_FAILED for an input that does not fit the contract", async () => {
    const { context, producer } = harness();

    assert.equal(codeOf(await sendReport(context, { n: "one" })), "VALIDATION_FAILED");
    assert.equal(codeOf(await sendReport(context, { n: 1, extra: true })), "VALIDATION_FAILED");
    assert.equal(codeOf(await sendReport(context, {})), "VALIDATION_FAILED");
    assert.equal(producer.prepares.length, 0);
  });

  test("VALIDATION_FAILED for a task with a required h.resultOf parameter", async () => {
    const { context, producer } = harness();
    const sent = await sendTask(context, upstreamOnly, {}, {});

    assert.equal(sent.ok ? "sent" : sent.error.code, "VALIDATION_FAILED");
    assert.equal(sent.ok ? false : sent.error.message.includes("upstream"), true);
    assert.equal(producer.prepares.length, 0);
  });

  test("VALIDATION_FAILED from the core's prepare step (a retention period that the app does not register)", async () => {
    const { context, producer } = harness({ script: { prepared: [{ kind: "refused", error: rawValidationFailed("not registered") }] } });

    assert.equal(codeOf(await sendReport(context, { n: 1 }, { retention: { kind: "days", days: 9 } })), "VALIDATION_FAILED");
    assert.equal(producer.enqueues.length, 0);
  });

  test("PAYLOAD_TOO_LARGE from the core's prepare step, with no enqueue", async () => {
    const { context, producer } = harness({ script: { prepared: [{ kind: "refused", error: rawPayloadTooLarge() }] } });
    const error = await sendReport(context);

    assert.equal(codeOf(error), "PAYLOAD_TOO_LARGE");
    assert.deepEqual(
      error !== "sent" && error.code === "PAYLOAD_TOO_LARGE" ? [error.sizeBytes, error.limitBytes] : [],
      [2_000_000n, 1_000_000n],
    );
    assert.equal(producer.enqueues.length, 0);
  });

  test("ENQUEUE_FAILED from the enqueue, with its task id and flag", async () => {
    const { context } = harness({ script: { enqueued: [{ kind: "failed", error: rawEnqueueFailed("report", false) }] } });
    const error = await sendReport(context);

    assert.deepEqual(error !== "sent" && error.code === "ENQUEUE_FAILED" ? [error.taskId, error.retryable] : [], [TASK_ID, false]);
  });

  test("PAYLOAD_MISMATCH from the enqueue", async () => {
    const { context } = harness({ script: { enqueued: [{ kind: "failed", error: rawPayloadMismatch("report") }] } });

    assert.equal(codeOf(await sendReport(context)), "PAYLOAD_MISMATCH");
  });

  test("IDEMPOTENCY_KEY_CONFLICT from the enqueue, with the owner's task id", async () => {
    const { context } = harness({ script: { enqueued: [{ kind: "failed", error: rawKeyConflict("report") }] } });
    const error = await sendReport(context, { n: 1 }, { idempotencyKey: "key-1" });

    assert.equal(error !== "sent" && error.code === "IDEMPOTENCY_KEY_CONFLICT" ? error.ownerTaskId : "", "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5c");
    assert.equal(error !== "sent" && error.code === "IDEMPOTENCY_KEY_CONFLICT" ? error.retryable : true, false);
    assert.equal(error !== "sent" ? JSON.stringify(error).includes("payload") : true, false);
  });

  test("the payload of an error is not in JSON.stringify of the error", async () => {
    const { context } = harness({ script: { enqueued: [{ kind: "failed", error: rawEnqueueFailed("report", true) }] } });
    const error = await sendReport(context);
    const text = JSON.stringify(error);

    assert.equal(text.includes("payload"), false);
    assert.equal(text.includes('\\"n\\":1'), false);
    assert.equal(text.includes("key-1"), false);
    assert.equal(error !== "sent" && error.code === "ENQUEUE_FAILED" ? error.payload.inputJson : "", '{"n":1}');
  });
});

describe("the resend rule", () => {
  const transient = { kind: "failed", error: rawEnqueueFailed("report", true) } as const;
  const final = { kind: "failed", error: rawEnqueueFailed("report", false) } as const;

  test("off: a retryable ENQUEUE_FAILED is given after one enqueue", async () => {
    const { context, producer, pauses } = harness({ script: { enqueued: [transient] } });

    assert.equal(codeOf(await sendReport(context)), "ENQUEUE_FAILED");
    assert.deepEqual([producer.enqueues.length, pauses], [1, []]);
  });

  test("on, with 0 resends: the first enqueue succeeds", async () => {
    const { context, producer, pauses } = harness({ resend: true });

    assert.equal(codeOf(await sendReport(context)), "sent");
    assert.deepEqual([producer.enqueues.length, pauses], [1, []]);
  });

  test("on, with 1 resend: REPEAT after 200 ms, then the handle", async () => {
    const { context, producer, pauses } = harness({ resend: true, script: { enqueued: [transient, { kind: "enqueued", taskId: TASK_ID }] } });

    assert.equal(codeOf(await sendReport(context)), "sent");
    assert.deepEqual(
      producer.enqueues.map((call) => call.sendKind),
      ["FIRST", "REPEAT"],
    );
    assert.deepEqual(pauses, [200]);
    assert.equal(producer.enqueues[1]?.request, producer.enqueues[0]?.request);
  });

  test("on, with 3 resends: the error after 4 enqueues and waits of 200, 400 and 800 ms", async () => {
    const { context, producer, pauses } = harness({ resend: true, script: { enqueued: [transient] } });

    assert.equal(codeOf(await sendReport(context)), "ENQUEUE_FAILED");
    assert.deepEqual(
      producer.enqueues.map((call) => call.sendKind),
      ["FIRST", "REPEAT", "REPEAT", "REPEAT"],
    );
    assert.deepEqual(pauses, [200, 400, 800]);
  });

  test("on: an ENQUEUE_FAILED that is not retryable is not sent again", async () => {
    const { context, producer } = harness({ resend: true, script: { enqueued: [final] } });

    assert.equal(codeOf(await sendReport(context)), "ENQUEUE_FAILED");
    assert.equal(producer.enqueues.length, 1);
  });
});

describe("retrySend", () => {
  test("of ENQUEUE_FAILED: one REPEAT enqueue of the payload, under the same task id", async () => {
    const { context, producer } = harness();
    const retried = await retrySend(context, retryable(rawEnqueueFailed("report", true)));

    assert.equal(retried.ok ? retried.value.taskId : "", TASK_ID);
    assert.deepEqual(producer.enqueues[0]?.sendKind, "REPEAT");
    assert.deepEqual(producer.enqueues[0]?.request, {
      taskId: TASK_ID,
      taskName: "report",
      queueName: "default",
      priority: 100,
      inputJson: '{"n":1}',
      sentAtMicros: SENT_AT_MICROS,
      deadline: { kind: "until", epochMicros: SENT_AT_MICROS + 60_000_000n },
      delay: { kind: "seconds", seconds: 30 },
      taskOptions: { kind: "text", text: OPTIONS_TEXT },
      idempotencyKey: { kind: "text", text: "key-1" },
      retention: { kind: "forever" },
    });
    assert.equal(producer.prepares.length, 0);
  });

  test("of IDEMPOTENCY_KEY_CONFLICT (not retryable): VALIDATION_FAILED and no enqueue", async () => {
    const { context, producer } = harness();
    const conflict = sendErrorOfRaw(rawKeyConflict("report"));
    // SAFETY: a JavaScript caller can pass an error that the type of retrySend refuses.
    const retried = await retrySend(context, conflict as unknown as RetryableSendError<number, string>);

    assert.equal(retried.ok ? "" : retried.error.code, "VALIDATION_FAILED");
    assert.deepEqual(producer.enqueues, []);
  });

  test("of an error of another app instance whose task this app lists", async () => {
    const first = harness({ script: { enqueued: [{ kind: "failed", error: rawEnqueueFailed("report", true) }] } });
    const second = harness();
    const error = await sendReport(first.context);

    assert.equal(codeOf(error), "ENQUEUE_FAILED");

    const retried =
      error !== "sent" && error.code === "ENQUEUE_FAILED" ? await retrySend(second.context, error) : undefined;

    assert.equal(retried?.ok, true);
    assert.deepEqual(
      second.producer.enqueues.map((call) => call.sendKind),
      ["REPEAT"],
    );
  });

  test("of an error whose task this app does not list: VALIDATION_FAILED and no enqueue", async () => {
    const { context, producer } = harness();
    const retried = await retrySend(context, retryable(rawEnqueueFailed("not_listed", true)));

    assert.equal(retried.ok ? "sent" : retried.error.code, "VALIDATION_FAILED");
    assert.equal(producer.enqueues.length, 0);
  });

  test("with sends suppressed: SEND_SUPPRESSED and no enqueue", async () => {
    const { context, producer } = harness({ suppression: { kind: "suppressed", reason: "environment" } });
    const retried = await retrySend(context, retryable(rawEnqueueFailed("report", true)));

    assert.equal(retried.ok ? "sent" : retried.error.code, "SEND_SUPPRESSED");
    assert.equal(producer.enqueues.length, 0);
  });
});

describe("the suppression of sends", () => {
  async function sendInProcess(start: ProcessStart, variables: Readonly<Record<string, string>> = {}): Promise<string> {
    const environment = fakeEnvironment({ start, variables });
    const created = createHorsies({ broker: { databaseUrl: DATABASE_URL }, tasks: [report] }, environment);

    if (!created.ok) {
      throw new Error("the app is refused");
    }

    const sent = await created.value.send(report, { n: 1 });

    return sent.ok ? "sent" : sent.error.code;
  }

  test("a task child before it serves (--horsies-child)", async () => {
    assert.equal(await sendInProcess({ ...PLAIN_START, argumentAfterScript: "--horsies-child" }), "SEND_SUPPRESSED");
  });

  test("a seat isolate before it serves", async () => {
    assert.equal(await sendInProcess({ ...PLAIN_START, seatIsolate: true }), "SEND_SUPPRESSED");
  });

  test("the check command", async () => {
    assert.equal(await sendInProcess({ ...PLAIN_START, argumentAfterScript: "check" }), "SEND_SUPPRESSED");
  });

  test("the scheduler before it serves", async () => {
    assert.equal(await sendInProcess({ ...PLAIN_START, argumentAfterScript: "scheduler" }), "SEND_SUPPRESSED");
    assert.deepEqual(initialSuppression({ ...PLAIN_START, argumentAfterScript: "scheduler" }), {
      kind: "suppressed",
      reason: "schedulerBeforeServe",
    });
  });

  test("HORSIES_SUPPRESS_SENDS=1, with whitespace around it, suppresses; another value does not", async () => {
    assert.equal(await sendInProcess(PLAIN_START, { HORSIES_SUPPRESS_SENDS: "1" }), "SEND_SUPPRESSED");
    assert.equal(await sendInProcess(PLAIN_START, { HORSIES_SUPPRESS_SENDS: " 1\n" }), "SEND_SUPPRESSED");
    assert.equal(await sendInProcess(PLAIN_START, { HORSIES_SUPPRESS_SENDS: "0" }), "sent");
  });

  test("HORSIES_SUPPRESS_SENDS is read at each send: set after Horsies.create, it suppresses", async () => {
    const variables: Record<string, string> = {};
    const created = createHorsies({ broker: { databaseUrl: DATABASE_URL }, tasks: [report] }, fakeEnvironment({ variables }));

    if (!created.ok) {
      throw new Error("the app is refused");
    }

    const before = await created.value.send(report, { n: 1 });

    variables["HORSIES_SUPPRESS_SENDS"] = "1";

    const after = await created.value.send(report, { n: 1 });

    assert.equal(before.ok, true);
    assert.equal(after.ok ? "" : after.error.code, "SEND_SUPPRESSED");
  });

  test("a plain process sends", async () => {
    assert.equal(await sendInProcess(PLAIN_START), "sent");
  });

  test("the start of serving opens a task child, a seat and the scheduler, not the check command or the variable", () => {
    const opened = (start: ProcessStart): SendSuppression => {
      const state = new SuppressionState(initialSuppression(start), () => undefined);

      state.startServing();

      return state.current();
    };

    assert.deepEqual(opened({ ...PLAIN_START, argumentAfterScript: "--horsies-child" }), { kind: "open" });
    assert.deepEqual(opened({ ...PLAIN_START, seatIsolate: true }), { kind: "open" });
    assert.deepEqual(opened({ ...PLAIN_START, argumentAfterScript: "scheduler" }), { kind: "open" });
    assert.deepEqual(opened({ ...PLAIN_START, argumentAfterScript: "check" }), { kind: "suppressed", reason: "checkCommand" });
    assert.deepEqual(servingSuppression({ kind: "open" }), { kind: "open" });
  });
});

describe("a send in a worker thread with no core", () => {
  test("throws AddonOutsideMainThread", async () => {
    const created = createHorsies({ broker: { databaseUrl: DATABASE_URL }, tasks: [report] }, fakeEnvironment({ mainThread: false }));

    if (!created.ok) {
      throw new Error("the app is refused");
    }

    await assert.rejects(created.value.send(report, { n: 1 }), AddonOutsideMainThread);
  });
});
