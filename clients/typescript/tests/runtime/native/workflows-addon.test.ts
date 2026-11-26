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

import { parseAppConfig } from "../../../horsies/src/config/app-config.ts";
import type { QueueConfigInput } from "../../../horsies/src/config/queues.ts";
import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle, type CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { NativeSendRules } from "../../../horsies/src/native/sends/addon.ts";
import { checkAppConfig } from "../../../horsies/src/native/sends/sends.ts";
import { type ChildSpecSourceFunction, workflowsAddon } from "../../../horsies/src/native/workflows/addon.ts";
import { DEFAULT_QUEUE_PRIORITY } from "../../../horsies/src/send/bounds.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { addonConversionFacts } from "../../../horsies/src/workflow/core.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";
import { subWorkflowRootText } from "./workflow-spec-texts.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

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

/** The send rules of the accepted config of `queues` with a database URL. */
function sendRulesOf(queues: QueueConfigInput): NativeSendRules {
  const broker = { databaseUrl: { kind: "value", value: UNUSED_DATABASE_URL } } as const;
  const config = parseAppConfig({ ...queues, broker }, () => undefined);

  if (!config.ok) {
    assert.fail(`the config is refused: ${JSON.stringify(config.error)}`);
  }

  const checked = checkAppConfig(config.value.coreConfigJson);

  if (checked.kind !== "accepted") {
    assert.fail(`the core refuses the config: ${JSON.stringify(checked.refusals)}`);
  }

  return checked.rules;
}

/** The conversion facts of a main thread with the rules `rules` and the queues `queueNames`, no task options. */
function conversionFactsOf(
  rules: NativeSendRules,
  queueNames: readonly string[],
): ReturnType<typeof addonConversionFacts> {
  const handle = buildCoreHandle(manifestJson(UNUSED_DATABASE_URL, false), "PRODUCER");

  return addonConversionFacts({ handle, rules, options: new Map(), queueNames, resendOnTransientError: false });
}

describe("the conversion facts of a main thread, from the core", () => {
  test("the default queue has the package's priority; a custom queue has its configured one", () => {
    const defaultMode = conversionFactsOf(sendRulesOf({}), ["default"]);

    assert.equal(defaultMode.queuePriority("default"), DEFAULT_QUEUE_PRIORITY);

    const queues = [{ name: "urgent", priority: 3 }, { name: "plain" }] as const;
    const custom = conversionFactsOf(sendRulesOf({ queueMode: "custom", customQueues: queues }), ["urgent", "plain"]);

    assert.equal(custom.queuePriority("urgent"), 3);
    assert.equal(custom.queuePriority("plain"), 1);
    assert.throws(() => custom.queuePriority("other"), ClientDefect);
  });

  test("each queue has the retention class of the core", () => {
    const facts = conversionFactsOf(sendRulesOf({}), ["default"]);

    assert.equal(facts.queueClass("default"), "standard_30d");
    assert.throws(() => facts.queueClass("other"), ClientDefect);
  });
});

describe("the child spec source registration", () => {
  test("the first registration on a handle is registered, and a second one is alreadySet", () => {
    const handle = buildCoreHandle(manifestJson(UNUSED_DATABASE_URL, false), "PRODUCER");

    assert.equal(envelopeValue(workflowsAddon().registerChildSpecSource(handle.native, neverCalled, [])), "registered");
    assert.equal(envelopeValue(workflowsAddon().registerChildSpecSource(handle.native, neverCalled, [])), "alreadySet");
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

/** Start the sub-workflow root of `input` on a handle whose source lists `definitionKeys`; the child node's result. */
async function startedChildResult(
  input: string,
  definitionKeys: readonly string[],
): Promise<{ text: string; calls: number }> {
  const handle = testHandle();
  let calls = 0;

  const counting: ChildSpecSourceFunction = () => {
    calls += 1;
  };

  assert.equal(
    envelopeValue(workflowsAddon().registerChildSpecSource(handle.native, counting, [...definitionKeys])),
    "registered",
  );

  const prepared = envelopeValue(workflowsAddon().prepareClientSpec(subWorkflowRootText(input)));

  assert(prepared.kind === "prepared", "the spec data parse does not read input_json");

  const workflowId = randomUUID();

  const started = envelopeValue(
    await workflowsAddon().startWorkflow(
      handle.native,
      prepared.spec,
      workflowId,
      1_795_687_200_000_000n,
      { kind: "noCarrier" },
      false,
    ),
  );

  assert.equal(started.kind, "started");

  const read = envelopeValue(await workflowsAddon().nodeResult(handle.native, workflowId, "child"));

  await handle.close();
  assert(read.kind === "stored" && read.result.kind === "checked");

  return { text: read.result.text, calls };
}

describe("the child spec source and the core's strict check, with a database", () => {
  test("a stored input that the core's strict read refuses fails the node and never calls the build", async () => {
    // A repeated key in the stored input: the core's strict read of the stored input refuses it first
    // (`child_static_input`), and the node fails with WORKER_SERIALIZATION_ERROR.
    const { text, calls } = await startedChildResult('{"n":1,"n":1}', ["tests.strict_child.v1"]);

    assert.match(text, /"error_code":\{"__builtin_task_code__":"WORKER_SERIALIZATION_ERROR"\}/u);
    assert.match(text, /Corrupt kwargs JSON for subworkflow task 0/u);
    assert.equal(calls, 0);
  });

  test("a child whose definition key the app does not list fails to load and never calls the build", async () => {
    const { text, calls } = await startedChildResult('{"n":1}', []);

    assert.match(text, /"error_code":\{"__builtin_task_code__":"SUBWORKFLOW_LOAD_FAILED"\}/u);
    assert.equal(calls, 0);
  });
});
