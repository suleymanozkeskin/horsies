// A new empty database for one native test file, so the rows of the files that ran before cannot change a read that
// expects an empty table. The server is the one that `HORSIES_TEST_DATABASE_URL` names.

import assert from "node:assert/strict";

import { createDatabase, dropDatabase, serverSettings, settingsRefusedText, type ServerSettings } from "../../e2e/support/database.ts";

/** A new empty database and the end of its life. */
export interface FreshDatabase {
  readonly url: string;
  /** Drops the database with its open sessions. Fails the test when the drop fails. */
  readonly drop: () => Promise<void>;
}

/** Creates a new empty database on the test server. Fails the test when the settings are refused or the create fails. */
export async function createFreshDatabase(): Promise<FreshDatabase> {
  const settings = serverSettings((name) => process.env[name]);

  if (!settings.ok) {
    assert.fail(settingsRefusedText(settings.error));
  }

  const server: ServerSettings = settings.value;
  const created = await createDatabase(server);

  if (!created.ok) {
    assert.fail(`the new database was not created: ${JSON.stringify(created.error)}`);
  }

  const { name, url } = created.value;

  return {
    url,
    drop: async () => {
      const dropped = await dropDatabase(server, name);

      assert.ok(dropped.ok, `the database ${name} was not dropped`);
    },
  };
}
