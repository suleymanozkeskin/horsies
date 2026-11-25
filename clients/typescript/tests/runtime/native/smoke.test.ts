// The smoke test of the bridge core with a built addon and a database: a handle connects, installs
// the schema, reads its status, pings, and closes. A refused manifest and a refused connection give
// their values.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

describe("the bridge core with a database", () => {
  test("a handle connects, installs the schema, reads its status, pings and closes", async () => {
    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");

    assert.equal(handle.manifestVersion, 1);

    const installed = envelopeValue(await handle.installSchema());

    assert.equal(installed.kind, "installed");

    const status = envelopeValue(await handle.schemaStatus());

    assert.equal(status.state, "MATCH");
    assert.deepEqual(status.stored, { kind: "stored", version: status.expectedVersion });

    const ping = envelopeValue(await handle.pingDatabase());

    assert.equal(ping.kind, "done");
    assert.ok(ping.kind === "done" && ping.record.latencyMs >= 0);
    assert.equal(envelopeValue(await handle.close()), undefined);
  });

  test("a ping that cannot connect is the value BrokerOperationFailed with DB_PING_FAILED", async () => {
    const handle = buildCoreHandle(manifestJson("postgresql://horsies@127.0.0.1:1/none", false), "PRODUCER");
    const ping = envelopeValue(await handle.pingDatabase());

    assert.equal(ping.kind, "failed");

    if (ping.kind !== "failed") {
      return;
    }

    assert.equal(ping.error.class, "BrokerOperationFailed");
    assert.ok(
      ping.error.slots.some((slot) => slot.value.kind === "codeMember" && slot.value.value === "DB_PING_FAILED"),
      `slots: ${ping.error.slots.map((slot) => slot.name).join(", ")}`,
    );
  });

  test("a manifest that the core refuses throws ManifestRefused with its field path", () => {
    const refused = manifestJson(testDatabaseUrl(), false).replace('"pool_size":5', '"pool_size":"five"');

    assert.throws(() => buildCoreHandle(refused, "PRODUCER"), (cause: unknown) => {
      assert.ok(cause instanceof Error);
      assert.equal(cause.name, "ManifestRefused");
      assert.match("fieldPath" in cause ? String(cause.fieldPath) : "", /pool_size/u);

      return true;
    });
  });
});
