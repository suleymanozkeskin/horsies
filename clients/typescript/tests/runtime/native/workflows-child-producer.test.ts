// A workflow start and a workflow handle read from task code in a task child go through the child host's producer
// handle (a pool of the child's `task_send_pool` connections, no overflow), not through the app's handle. The start
// keeps the child spec source of the app's handle.
//
// As in `child-producer.test.ts`: the probe exports of a `test-probes` build give the bridge's choice
// (`ProducerHandle::chosen`) a child whose child host published a stand-in of the test database. The app's handle names
// a port where no server listens: the start and the read on it fail; through the probes they succeed, and the stand-in
// reports that an operation connected it. A start of a sub-workflow root in a child calls the source of the app's
// handle once.
//
// Runs only where a `build:dev` addon exists (`HORSIES_NATIVE_ADDON_PATH`) and `HORSIES_TEST_DATABASE_URL` names a
// throwaway PostgreSQL database.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

import { addonSettlement, settlingCallback } from "../../../horsies/src/native/call/callback.ts";
import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle, type CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { TraceArgument } from "../../../horsies/src/native/values/records.ts";
import {
  type ChildSpecAnswer,
  type ChildSpecCall,
  type ChildSpecSourceFunction,
  workflowsAddon,
} from "../../../horsies/src/native/workflows/addon.ts";
import { childProducerProbeAddon, type NativeProbeChildProducer } from "./child-producer-probe.ts";
import { manifestJson, manifestJsonWithPool, testDatabaseUrl } from "./manifest.ts";
import { subWorkflowRootText } from "./workflow-spec-texts.ts";
import { workflowsChildProducerProbeAddon } from "./workflows-child-producer-probe.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The `task_send_pool` of the stand-in: its pool size, with no overflow. */
const TASK_SEND_POOL = 2;

/** A database URL where no server listens: every operation on the app's handle fails to connect. */
const UNREACHABLE_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

const NO_CARRIER: TraceArgument = { kind: "noCarrier" };

/** A child spec source that the core never calls: the vector has no sub-workflow node. */
const neverCalled: ChildSpecSourceFunction = () => undefined;

/** The definition key of the child of the sub-workflow root (`workflow-spec-texts.ts`). */
const CHILD_KEY = "tests.strict_child.v1";

/** The workflow spec data vectors of the core. */
const VECTOR_FILE = fileURLToPath(
  new URL("../../../../../core/tests/fixtures/stored_forms/workflow_spec_data.json", import.meta.url),
);

/** The text of the spec data vector `name`. */
function vectorText(name: string): string {
  // SAFETY: the vector file of the core holds a `vectors` list of named texts; a changed file fails the find.
  const file = JSON.parse(readFileSync(VECTOR_FILE, "utf8")) as {
    readonly vectors: readonly { readonly name: string; readonly text: string }[];
  };

  const vector = file.vectors.find((row) => row.name === name);

  assert(vector !== undefined, `the vector ${name} exists`);

  return vector.text;
}

/** The app's handle, with the child spec source `source` that lists `definitionKeys`: its database cannot be reached. */
function unreachableAppHandle(
  source: ChildSpecSourceFunction = neverCalled,
  definitionKeys: readonly string[] = [],
): CoreHandle {
  const handle = buildCoreHandle(manifestJson(UNREACHABLE_DATABASE_URL, false), "PRODUCER");
  const registered = workflowsAddon().registerChildSpecSource(handle.native, source, [...definitionKeys]);

  assert.equal(envelopeValue(registered), "registered");

  return handle;
}

/** A new stand-in of the child host's handle: the test database, `TASK_SEND_POOL` connections, no overflow. */
function newStandIn(): NativeProbeChildProducer {
  const manifest = manifestJsonWithPool(testDatabaseUrl(), false, "MIGRATE_AND_VALIDATE", {
    size: TASK_SEND_POOL,
    maxOverflow: 0,
  });

  return envelopeValue(childProducerProbeAddon().probeChildProducer(manifest));
}

describe("the workflow operations of a task child", () => {
  test("a start in a child is written through the child host's handle; the app's handle cannot write it", async () => {
    const app = unreachableAppHandle();
    const child = newStandIn();
    const prepared = envelopeValue(workflowsAddon().prepareClientSpec(vectorText("one_task_node")));

    assert(prepared.kind === "prepared");

    const workflowId = randomUUID();
    const sentAt = 1_795_687_200_000_000n;

    const started = envelopeValue(
      await workflowsChildProducerProbeAddon().probeStartWorkflowInChild(
        app.native,
        child,
        prepared.spec,
        workflowId,
        sentAt,
        NO_CARRIER,
        false,
      ),
    );

    assert.deepEqual(started, { kind: "started" });

    const direct = envelopeValue(
      await workflowsAddon().startWorkflow(app.native, prepared.spec, randomUUID(), sentAt, NO_CARRIER, false),
    );

    assert.equal(direct.kind === "failed" ? direct.error.class : direct.kind, "WorkflowStartEnqueueFailed");
    assert.equal(envelopeValue(await child.close()), "CLOSED");
  });

  test("a start in a child builds a sub-workflow child with the child spec source of the app's handle", async () => {
    let calls = 0;

    const notListed = settlingCallback((call: ChildSpecCall): ChildSpecAnswer => {
      calls += 1;
      assert.equal(call.definitionKey, CHILD_KEY);

      return { kind: "notListed" };
    }, addonSettlement);

    const app = unreachableAppHandle(notListed, [CHILD_KEY]);
    const child = newStandIn();
    const prepared = envelopeValue(workflowsAddon().prepareClientSpec(subWorkflowRootText('{"n":1}')));

    assert(prepared.kind === "prepared");

    const workflowId = randomUUID();

    const started = envelopeValue(
      await workflowsChildProducerProbeAddon().probeStartWorkflowInChild(
        app.native,
        child,
        prepared.spec,
        workflowId,
        1_795_687_200_000_000n,
        NO_CARRIER,
        false,
      ),
    );

    assert.deepEqual(started, { kind: "started" });
    assert.equal(calls, 1);

    const reader = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");
    const read = envelopeValue(await workflowsAddon().nodeResult(reader.native, workflowId, "child"));

    await reader.close();
    assert(read.kind === "stored" && read.result.kind === "checked");
    assert.match(read.result.text, /"error_code":"SUBWORKFLOW_LOAD_FAILED"/u);
    assert.equal(envelopeValue(await child.close()), "CLOSED");
  });

  test("a resume in a child goes through the child host's handle; the app's handle cannot resume", async () => {
    // A PAUSED run of the test database, started and paused through a handle that reaches it.
    const reader = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");

    assert.equal(envelopeValue(workflowsAddon().registerChildSpecSource(reader.native, neverCalled, [])), "registered");

    const prepared = envelopeValue(workflowsAddon().prepareClientSpec(vectorText("one_task_node")));

    assert(prepared.kind === "prepared");

    const workflowId = randomUUID();
    const sentAt = 1_795_687_200_000_000n;

    const started = envelopeValue(
      await workflowsAddon().startWorkflow(reader.native, prepared.spec, workflowId, sentAt, NO_CARRIER, false),
    );

    assert.deepEqual(started, { kind: "started" });
    assert.deepEqual(envelopeValue(await workflowsAddon().pauseWorkflow(reader.native, workflowId)), {
      kind: "changed",
      changed: true,
    });

    const app = unreachableAppHandle();
    const child = newStandIn();

    const resumed = envelopeValue(
      await workflowsChildProducerProbeAddon().probeResumeWorkflowInChild(app.native, child, workflowId),
    );

    assert.deepEqual(resumed, { kind: "changed", changed: true });

    const direct = envelopeValue(await workflowsAddon().resumeWorkflow(app.native, workflowId));

    assert.equal(direct.kind === "failed" ? direct.error.class : direct.kind, "HandleDbOperationFailed");
    assert.equal(envelopeValue(await child.close()), "CLOSED");
    await reader.close();
  });

  test("a handle read in a child goes through the child host's handle", async () => {
    const app = unreachableAppHandle();
    const child = newStandIn();

    const read = envelopeValue(
      await workflowsChildProducerProbeAddon().probeWorkflowStatusInChild(app.native, child, randomUUID()),
    );

    assert.equal(read.kind === "failed" ? read.error.class : read.kind, "HandleWorkflowNotFound");

    const direct = envelopeValue(await workflowsAddon().workflowStatus(app.native, randomUUID()));

    assert.equal(direct.kind === "failed" ? direct.error.class : direct.kind, "HandleDbOperationFailed");
    assert.equal(envelopeValue(await child.close()), "CLOSED");
  });
});
