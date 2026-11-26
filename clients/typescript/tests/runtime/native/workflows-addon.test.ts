// The functions of the `workflows` area of the addon, called directly: the core's parse of the spec data vectors,
// the registration of a child spec source, and the reads of a workflow that does not exist.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`). The reads also need `HORSIES_TEST_DATABASE_URL`
// to name a throwaway PostgreSQL database.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { buildCoreHandle, type CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { type ChildSpecSourceFunction, workflowsAddon } from "../../../horsies/src/native/workflows/addon.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

/** The workflow spec data vectors of the core. */
const VECTOR_FILE = fileURLToPath(
  new URL("../../../../../core/tests/fixtures/stored_forms/workflow_spec_data.json", import.meta.url),
);

/** A database URL that no test connects to: the registration opens no connection. */
const UNUSED_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** One vector: Python's text of a document, with the workflow name of the document. */
type SpecDataVector = {
  readonly name: string;
  readonly document: { readonly name: string };
  readonly text: string;
};

/** One text that the core refuses, with the field path of the refusal. */
type RefusedSpecData = {
  readonly name: string;
  readonly text: string;
  readonly core_field_path: string;
};

type SpecDataFile = {
  readonly vectors: readonly SpecDataVector[];
  readonly refused_texts: readonly RefusedSpecData[];
};

function specDataFile(): SpecDataFile {
  // SAFETY: the vector file of the core holds these fields (its README, "Rows"); a changed file fails the asserts.
  return JSON.parse(readFileSync(VECTOR_FILE, "utf8")) as SpecDataFile;
}

/** A child spec source that the tests register and the core never calls. */
const neverCalled: ChildSpecSourceFunction = () => undefined;

/** A handle of the test database. */
function testHandle(): CoreHandle {
  return buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");
}

describe("the spec data parse of the addon", () => {
  test("each vector text is prepared, with the workflow name of its document", () => {
    for (const vector of specDataFile().vectors) {
      const prepared = envelopeValue(workflowsAddon().prepareClientSpec(vector.text));

      if (prepared.kind !== "prepared") {
        assert.fail(`${vector.name}: the core refuses the text at ${prepared.fieldPath}: ${prepared.cause}`);
      }

      assert.equal(envelopeValue(prepared.spec.workflowName()), vector.document.name, vector.name);
    }
  });

  test("each refused text gives the field path of the core and a cause", () => {
    for (const refused of specDataFile().refused_texts) {
      const prepared = envelopeValue(workflowsAddon().prepareClientSpec(refused.text));

      if (prepared.kind !== "refused") {
        assert.fail(`${refused.name}: the core reads a text that it must refuse`);
      }

      assert.equal(prepared.fieldPath, refused.core_field_path, refused.name);
      assert.ok(prepared.cause.length > 0, refused.name);
    }
  });
});

describe("the child spec source registration", () => {
  test("the first registration on a handle is registered, and a second one is alreadySet", () => {
    const handle = buildCoreHandle(manifestJson(UNUSED_DATABASE_URL, false), "PRODUCER");

    assert.equal(envelopeValue(workflowsAddon().registerChildSpecSource(handle.native, neverCalled)), "registered");
    assert.equal(envelopeValue(workflowsAddon().registerChildSpecSource(handle.native, neverCalled)), "alreadySet");
  });
});

describe("the workflow reads of a workflow that does not exist, with a database", () => {
  test("the status is the class HandleWorkflowNotFound", async () => {
    const handle = testHandle();
    const read = envelopeValue(await workflowsAddon().workflowStatus(handle.native, randomUUID()));

    assert.equal(read.kind === "failed" ? read.error.class : read.kind, "HandleWorkflowNotFound");
    await handle.close();
  });

  test("a bounded wait for the end is notFound", async () => {
    const handle = testHandle();

    const wait = envelopeValue(
      await workflowsAddon().waitWorkflowEnd(handle.native, randomUUID(), { kind: "upTo", milliseconds: 200 }),
    );

    assert.deepEqual(wait, { kind: "notFound" });
    await handle.close();
  });

  test("the result of a node is notReady", async () => {
    const handle = testHandle();
    const read = envelopeValue(await workflowsAddon().nodeResult(handle.native, randomUUID(), "charge"));

    assert.deepEqual(read, { kind: "notReady" });
    await handle.close();
  });

  test("the nodes and the node results are workflowAbsent", async () => {
    const handle = testHandle();
    const workflowId = randomUUID();

    assert.deepEqual(envelopeValue(await workflowsAddon().workflowNodes(handle.native, workflowId)), {
      kind: "workflowAbsent",
    });
    assert.deepEqual(envelopeValue(await workflowsAddon().workflowNodeResults(handle.native, workflowId)), {
      kind: "workflowAbsent",
    });
    await handle.close();
  });
});
