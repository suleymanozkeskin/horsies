// The steady scenario starts a return workflow and a restock workflow on its pinned order ordinals, and the stored
// workflows say the same. No worker runs: the test counts the starts, not their ends. Opt-in: `ACME_DATABASE_URL`.

import assert from "node:assert/strict";
import { test } from "node:test";

import { loadSeedCatalog } from "../src/scenarios/catalog.ts";
import { runSteady } from "../src/scenarios/index.ts";
import { closeStore, type Store } from "../src/store/connection.ts";
import { tableExists } from "../src/store/schema.ts";
import { RESTOCK_SPAWN_EVERY, RETURN_SPAWN_EVERY, STEADY_MAX_PACE } from "../src/tuning.ts";
import { testDatabase } from "./support.ts";

/** The count of the stored workflows named `name`; 0 before the first app installs the horsies tables. */
async function workflowCount(store: Store, name: string): Promise<number> {
  const present = await tableExists(store, "horsies_workflows");

  assert.ok(present.ok);

  if (!present.value) {
    return 0;
  }

  const counted = await store.pool.query("SELECT count(*)::int AS workflows FROM horsies_workflows WHERE name = $1", [name]);
  const workflows: unknown = counted.rows[0]?.workflows;

  assert.ok(Number.isSafeInteger(workflows));

  return Number(workflows);
}

test("steady starts the return and the restock workflows on the pinned ordinals", { timeout: 300_000 }, async () => {
  const database = await testDatabase();

  if (database.kind === "skipped") {
    return;
  }

  try {
    const loaded = await loadSeedCatalog(database.store);

    assert.ok(loaded.ok, loaded.ok ? "" : loaded.error);

    const returnsBefore = await workflowCount(database.store, "returns_review");
    const restocksBefore = await workflowCount(database.store, "restock");
    const ran = await runSteady({ orders: { kind: "bounded", orders: RESTOCK_SPAWN_EVERY }, errors: "noCover", pace: STEADY_MAX_PACE });

    assert.ok(ran.ok, ran.ok ? "" : ran.error);
    assert.equal(ran.value.ordersPlaced, RESTOCK_SPAWN_EVERY);
    assert.equal(ran.value.returnsStarted, Math.floor(RESTOCK_SPAWN_EVERY / RETURN_SPAWN_EVERY));
    assert.equal(ran.value.restocksStarted, 1);
    assert.equal(await workflowCount(database.store, "returns_review"), returnsBefore + ran.value.returnsStarted);
    assert.equal(await workflowCount(database.store, "restock"), restocksBefore + 1);
  } finally {
    await closeStore(database.store);
  }
});
