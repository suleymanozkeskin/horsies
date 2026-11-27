// `Horsies.create` and the connection settings of PgBouncer transaction mode, with a built addon: the session URL and
// the pooled URL that name one server (the schemes `postgres://` and `postgresql://` are one scheme) are refused at
// create, as the worker start refuses them. No database is needed: the check starts no core.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { defineTask, h, Horsies } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

const probe = defineTask({ name: "create_connection_probe", input: {}, output: h.int() });

/** The code of a refused database URL setting. */
const DATABASE_INVALID_URL = "HRS-203";

/** The text that the refusal of a session URL equal to the pooled URL holds. */
const SAME_URL_TEXT = "must not use the transaction-pool URL";

/** Creates an app in PgBouncer transaction mode with the pooled URL `pooled` and the session URL `session`. */
function createPooled(pooled: string, session: string): ReturnType<typeof Horsies.create> {
  return Horsies.create({
    database: {
      databaseUrl: { kind: "value", value: pooled },
      connection: { kind: "pgbouncerTransaction", sessionDatabaseUrl: { kind: "value", value: session } },
    },
    tasks: [probe],
  });
}

describe("Horsies.create in PgBouncer transaction mode", () => {
  const forms: ReadonlyArray<readonly [name: string, pooled: string, session: string]> = [
    ["the same text with `postgresql://`", "postgresql://127.0.0.1:6432/app", "postgresql://127.0.0.1:6432/app"],
    ["the same text with `postgres://`", "postgres://127.0.0.1:6432/app", "postgres://127.0.0.1:6432/app"],
    ["a pooled `postgres://` URL and a session `postgresql://` URL", "postgres://127.0.0.1:6432/app", "postgresql://127.0.0.1:6432/app"],
    ["a pooled `postgresql://` URL and a session `postgres://` URL", "postgresql://127.0.0.1:6432/app", "postgres://127.0.0.1:6432/app"],
  ];

  for (const [name, pooled, session] of forms) {
    test(`${name}: refused at create with the code of a database URL`, () => {
      const created = createPooled(pooled, session);

      assert.ok(!created.ok, "the create refuses one URL named twice");

      const refusal = created.error.refusals.find((refused) => refused.message.includes(SAME_URL_TEXT));

      assert.ok(refusal !== undefined, JSON.stringify(created.error.refusals.map((refused) => refused.message)));
      assert.equal(refusal.code, DATABASE_INVALID_URL);
    });
  }

  test("a session URL of another server, with either scheme, is accepted", () => {
    assert.ok(createPooled("postgres://127.0.0.1:6432/app", "postgresql://127.0.0.1:5432/app").ok);
    assert.ok(createPooled("postgresql://127.0.0.1:6432/app", "postgres://127.0.0.1:5432/app").ok);
  });
});
