// The workflow calls of a seat body past the bound of the relay: a start, a restart (`retryStart`) and each handle call
// give `SEAT_RELAY_FULL`, retryable, through the error union of the call, with the bound in the text and in the cause.
// Nothing goes to the main thread.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { checkContractList, contractLookup } from "../../../horsies/src/app/contract-list.ts";
import { WorkflowId } from "../../../horsies/src/contract/ids.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { IsolateRelay } from "../../../horsies/src/seats/relay.ts";
import { SeatRelayCode } from "../../../horsies/src/_catalog/task_send.ts";
import type { SeatRelayFull } from "../../../horsies/src/seats/relay-full.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import { createWorkflowArea, type WorkflowArea } from "../../../horsies/src/workflow/app-area.ts";
import { relayedWorkflowCore, type WorkflowCore } from "../../../horsies/src/workflow/core.ts";
import { type WorkflowHandle, workflowHandleOf } from "../../../horsies/src/workflow/handle.ts";
import type { BuildReturn } from "../../../horsies/src/workflow/node.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";
import { listedWorkflowOf, type WorkflowRegistry, workflowRegistry } from "../../../horsies/src/workflow/registry.ts";
import { NO_OP_START_OBSERVER } from "../../../horsies/src/workflow/start.ts";
import { TASK_ID } from "../app/support.ts";
import { fakeCore, rawStartEnqueueFailed, WORKFLOW_ID } from "../workflow/core-support.ts";
import { TEST_RELAY_BOUNDS } from "./support.ts";

const greet = defineTask({ name: "greet", input: { who: h.string() }, output: h.string() });

const hello = defineWorkflow(
  { name: "hello", definitionKey: "tests.hello.v1", output: h.string(), params: { who: h.string() }, checkCases: [{ who: "a" }] },
  (w, { who }) => ({ output: w.task(greet, { who }, { id: "greet" }) }),
);

function registry(): WorkflowRegistry {
  const entry = listedWorkflowOf(hello);

  assert(entry.ok);

  return workflowRegistry([entry.value], contractLookup(checkContractList([greet], { kind: "default" }).entries), {
    kind: "default",
  });
}

/** The text that names the relay bound and its value. */
const BOUND_TEXT = `its bound (MAX_RUN_WORK = ${String(TEST_RELAY_BOUNDS.opsWaiting)})`;

/** A workflow core of a seat isolate whose relay holds its whole waiting bound, and the operations that it sent. */
function fullCore(): { readonly core: WorkflowCore; readonly sent: string[] } {
  const sent: string[] = [];

  const relay = new IsolateRelay((message) => {
    if (message.kind === "op" && message.request.area === "workflows") {
      sent.push(message.request.operation.kind);
    }
  }, TEST_RELAY_BOUNDS);

  for (let index = 0; index < TEST_RELAY_BOUNDS.opsInFlight + TEST_RELAY_BOUNDS.opsWaiting; index += 1) {
    void relay.request("sends", { kind: "taskInfo", taskId: TASK_ID });
  }

  return { core: relayedWorkflowCore(relay), sent };
}

function areaOn(core: WorkflowCore): WorkflowArea {
  return createWorkflowArea({
    core,
    registry: registry(),
    observer: NO_OP_START_OBSERVER,
    now: () => Temporal.Instant.from("2026-11-26T10:00:00Z"),
    newWorkflowId: () => WORKFLOW_ID,
  });
}

function handleOn(core: WorkflowCore): WorkflowHandle<BuildReturn> {
  const workflowId = WorkflowId.parse(WORKFLOW_ID);

  assert(workflowId.ok);

  return workflowHandleOf<BuildReturn>({
    core,
    registry: registry(),
    workflow: hello,
    workflowId: workflowId.value,
    targets: new Map([["greet", { kind: "task", contract: greet } as const]]),
  });
}

/** `SEAT_RELAY_FULL` with the bound in its text and in its cause, a retryable precondition of the seat relay. */
function assertRelayFull(error: SeatRelayFull): void {
  assert.equal(error.code, SeatRelayCode.SEAT_RELAY_FULL);
  assert.equal(error.retryable, true);
  assert.ok(error.message.includes(BOUND_TEXT) && error.message.includes("nothing was sent; retry"), error.message);
  assert.deepEqual(error.exception, {
    kind: "coreFailure",
    failure: "preconditionFailed",
    subject: "seat relay",
    message: error.message,
    retryable: true,
  });
}

/** The error of a failed call, or the fail of the test. */
function errorOf<T, E>(result: Result<T, E>): E {
  return result.ok ? assert.fail("the call past the bound was answered") : result.error;
}

describe("each workflow call kind past the bound of the relay gives SEAT_RELAY_FULL", () => {
  test("a start", async () => {
    const { core, sent } = fullCore();
    const error = errorOf(await areaOn(core).start(hello, { who: "a" }));

    assert.ok(error.code === SeatRelayCode.SEAT_RELAY_FULL, error.code);
    assertRelayFull(error);
    assert.deepEqual(sent, []);
  });

  test("a restart (retryStart of an ENQUEUE_FAILED)", async () => {
    const failing = fakeCore({ started: [{ kind: "failed", error: rawStartEnqueueFailed("hello", true) }] });
    const first = errorOf(await areaOn(failing).start(hello, { who: "a" }));

    assert.ok(first.code === "ENQUEUE_FAILED", first.code);

    const { core, sent } = fullCore();
    const error = errorOf(await areaOn(core).retryStart(first));

    assert.ok(error.code === SeatRelayCode.SEAT_RELAY_FULL, error.code);
    assertRelayFull(error);
    assert.deepEqual(sent, []);
  });

  test("a status read", async () => {
    const { core, sent } = fullCore();
    const error = errorOf(await handleOn(core).status());

    assert.ok(error.code === SeatRelayCode.SEAT_RELAY_FULL, error.code);
    assertRelayFull(error);
    assert.deepEqual(sent, []);
  });

  test("a node results read", async () => {
    const { core, sent } = fullCore();
    const error = errorOf(await handleOn(core).results());

    assert.ok(error.code === SeatRelayCode.SEAT_RELAY_FULL, error.code);
    assertRelayFull(error);
    assert.deepEqual(sent, []);
  });

  test("a nodes read", async () => {
    const { core, sent } = fullCore();
    const error = errorOf(await handleOn(core).tasks());

    assert.ok(error.code === SeatRelayCode.SEAT_RELAY_FULL, error.code);
    assertRelayFull(error);
    assert.deepEqual(sent, []);
  });

  test("a node result read: the read error seatRelayFull", async () => {
    const { core, sent } = fullCore();
    const error = errorOf(await handleOn(core).resultFor("greet"));

    assert.ok(error.kind === "seatRelayFull", error.kind);
    assertRelayFull(error.error);
    assert.deepEqual(sent, []);
  });

  test("a cancel", async () => {
    const { core, sent } = fullCore();
    const error = errorOf(await handleOn(core).cancel());

    assert.ok(error.code === SeatRelayCode.SEAT_RELAY_FULL, error.code);
    assertRelayFull(error);
    assert.deepEqual(sent, []);
  });

  test("a pause", async () => {
    const { core, sent } = fullCore();
    const error = errorOf(await handleOn(core).pause());

    assert.ok(error.code === SeatRelayCode.SEAT_RELAY_FULL, error.code);
    assertRelayFull(error);
    assert.deepEqual(sent, []);
  });

  test("a resume", async () => {
    const { core, sent } = fullCore();
    const error = errorOf(await handleOn(core).resume());

    assert.ok(error.code === SeatRelayCode.SEAT_RELAY_FULL, error.code);
    assertRelayFull(error);
    assert.deepEqual(sent, []);
  });
});
