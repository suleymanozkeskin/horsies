// The smoke test of the bridge core with a built addon and a database: a handle connects, installs
// the schema, reads its status, pings, and closes. A refused manifest, a refused connection, and a
// schema mode that installs nothing give their values.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { RawCatalogError } from "../../../horsies/src/native/values/records.ts";
import { manifestJson, manifestJsonWithSchema, testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** A database URL whose connect is refused at once: nothing listens on port 1. */
const REFUSED_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** The kind of an outcome, and for a failed one its class and slot names: the message of a failed check. */
function outcomeText(outcome: { readonly kind: string } | { readonly kind: "failed"; readonly error: RawCatalogError }): string {
  return "error" in outcome
    ? `failed ${outcome.error.class}: ${outcome.error.slots.map((slot) => slot.name).join(", ")}`
    : outcome.kind;
}

/** Whether `error` is `DatabaseOperationFailed` with the code `code`. */
function isOperationFailed(error: RawCatalogError, code: string): boolean {
  return (
    error.class === "DatabaseOperationFailed" &&
    error.slots.some((slot) => slot.value.kind === "codeMember" && slot.value.value === code)
  );
}

describe("the bridge core with a database", () => {
  test("a handle connects, installs the schema, reads its status, pings and closes", async () => {
    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");

    assert.equal(handle.manifestVersion(), 1);

    const installed = await handle.installSchema();

    assert.equal(installed.kind, "installed");

    const status = await handle.schemaStatus();

    assert.equal(status.state, "MATCH");
    assert.deepEqual(status.stored, { kind: "stored", version: status.expectedVersion });

    const ping = await handle.pingDatabase();

    assert.equal(ping.kind, "done");
    assert.ok(ping.kind === "done" && ping.record.latencyMs >= 0);
    assert.equal(await handle.close(), undefined);
  });

  test("a ping that cannot connect is the value DatabaseOperationFailed with DATABASE_PING_FAILED", async () => {
    const handle = buildCoreHandle(manifestJson(REFUSED_DATABASE_URL, false), "PRODUCER");
    const ping = await handle.pingDatabase();

    assert.ok(ping.kind === "failed" && isOperationFailed(ping.error, "DATABASE_PING_FAILED"), outcomeText(ping));
  });

  test("an install that cannot connect is failed with SCHEMA_INIT_FAILED", async () => {
    const handle = buildCoreHandle(manifestJson(REFUSED_DATABASE_URL, false), "PRODUCER");
    const installed = await handle.installSchema();

    assert.ok(
      installed.kind === "failed" && isOperationFailed(installed.error, "SCHEMA_INIT_FAILED"),
      outcomeText(installed),
    );
  });

  test("an install under OBSERVE_ONLY runs nothing and is notRun", async () => {
    const handle = buildCoreHandle(manifestJsonWithSchema(REFUSED_DATABASE_URL, false, "OBSERVE_ONLY"), "PRODUCER");

    assert.deepEqual(await handle.installSchema(), { kind: "notRun" });
  });

  test("the status of a database that cannot be reached is UNKNOWN with no version", async () => {
    const handle = buildCoreHandle(manifestJson(REFUSED_DATABASE_URL, false), "PRODUCER");
    const status = await handle.schemaStatus();

    assert.equal(status.state, "UNKNOWN");
    assert.deepEqual(status.stored, { kind: "noVersion" });
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
