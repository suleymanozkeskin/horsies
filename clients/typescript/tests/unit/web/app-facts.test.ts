// The facts of the app that the monitoring handler and the `metrics` command read: the core handle, the queue names of
// the config, the schedule names and the package version.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import type { AppContext } from "../../../horsies/src/app/context.ts";
import { monitoringAppOf } from "../../../horsies/src/web/app-facts.ts";
import { queueSetupOf, type QueueSetup } from "../../../horsies/src/config/queues.ts";
import { clientVersion } from "../../../horsies/src/web/client-version.ts";
import { fakeCoreHandle, fakeProducer } from "../app/support.ts";

/** The `package.json` of the horsies package. */
const PACKAGE_JSON_URL = new URL("../../../horsies/package.json", import.meta.url);

/** A context of a main thread with `queues`: only the fields that the commands read. */
function contextWith(queues: QueueSetup): { readonly context: AppContext; readonly handle: object } {
  const handle = fakeCoreHandle();
  const partial = { core: { kind: "addon", handle, producer: fakeProducer() }, config: { queues } };

  // SAFETY: `monitoringAppOf` reads `core` and `config.queues` only; the test gives those two fields.
  return { context: partial as unknown as AppContext, handle };
}

describe("monitoringAppOf", () => {
  test("an app with the default queue names the default queue, and holds the app's handle", () => {
    const { context, handle } = contextWith({ kind: "default" });
    const facts = monitoringAppOf(context);

    assert.equal(facts.coreHandle, handle);
    assert.deepEqual(facts.queueNames, ["default"]);
    assert.deepEqual(facts.scheduleNames, []);
  });

  test("an app with custom queues names each queue in config order", () => {
    const queues = queueSetupOf({ queueMode: "custom", customQueues: [{ name: "urgent" }, { name: "bulk" }] });
    const facts = monitoringAppOf(contextWith(queues).context);

    assert.deepEqual(facts.queueNames, ["urgent", "bulk"]);
  });

  test("a context with no addon is a defect", () => {
    const context = { core: { kind: "noCore" }, config: { queues: { kind: "default" } } };

    // SAFETY: the test gives the two fields that `monitoringAppOf` reads.
    assert.throws(() => monitoringAppOf(context as unknown as AppContext), { name: "ClientDefect" });
  });
});

describe("clientVersion", () => {
  test("is the version of the package's package.json", () => {
    const manifest: { readonly version: string } = JSON.parse(readFileSync(PACKAGE_JSON_URL, "utf8"));

    assert.equal(clientVersion(), manifest.version);
  });
});
