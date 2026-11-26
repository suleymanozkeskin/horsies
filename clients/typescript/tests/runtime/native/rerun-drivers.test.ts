// The rerun in the caller's transaction with a built addon, a database, and each driver adapter:
// `pg` and `postgres.js`. The drivers are dev dependencies of the tests; this file loads them at run
// time, so the type check needs neither.
//
// Each case runs in a transaction that the test owns. The test, not the rerun, ends it.

import assert from "node:assert/strict";
import { before, describe, test } from "node:test";

import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { mintTaskId } from "../../../horsies/src/native/process-core/process-core.ts";
import { TaskId } from "../../../horsies/src/contract/ids.ts";
import { NO_ACTIVE_CARRIER, createRerunArea } from "../../../horsies/src/rerun/app-area.ts";
import type { RerunArea } from "../../../horsies/src/rerun/app-area.ts";
import type { CallerRerunError } from "../../../horsies/src/rerun/errors.ts";
import { pgTransaction } from "../../../horsies/src/rerun/pg.ts";
import type { PgTextQuery, PgTextResult } from "../../../horsies/src/rerun/pg.ts";
import { postgresTransaction } from "../../../horsies/src/rerun/postgres.ts";
import type { PostgresTransactionSql } from "../../../horsies/src/rerun/postgres.ts";
import type { RerunCommand, RerunOutcome, RerunPolicy } from "../../../horsies/src/rerun/types.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

/** The part of a `pg` client that the tests call. */
interface PgTestClient {
  connect(): Promise<void>;
  query(config: PgTextQuery): Promise<PgTextResult>;
  query(text: string): Promise<object>;
  escapeLiteral(text: string): string;
  end(): Promise<void>;
}

/** The part of the `pg` module that the tests call. */
interface PgModule {
  readonly Client: new (config: { readonly connectionString: string }) => PgTestClient;
}

/** A parameter of `postgres.js`, which the tests pass through. */
interface PostgresParameter {
  readonly type: number;
}

/** The part of a root `sql` of `postgres.js` that the tests call. */
interface PostgresTestSql {
  begin<T>(callback: (sql: PostgresTransactionSql<PostgresParameter>) => Promise<T>): Promise<T>;
  end(): Promise<void>;
}

/** The `postgres` function of `postgres.js`. */
type PostgresModule = (url: string, options: { readonly onnotice: () => void }) => PostgresTestSql;

/** A retention period that no install of the tests registers. */
const UNREGISTERED_DAYS = 1777;

const POLICY: RerunPolicy = {
  retention: { kind: "forever" },
  rerunInput: "keep",
  reservationWindow: Temporal.Duration.from({ days: 1 }),
};

/**
 * The default export of the driver package `name`: the exports object of `pg`, the `postgres` function
 * of `postgres.js`. Throws when the dev dependency is missing.
 */
async function driverDefault(name: "pg" | "postgres"): Promise<object> {
  // A specifier of type `string`: the type check does not resolve the package.
  const specifier: string = name;

  try {
    // SAFETY: an import of a package gives its module namespace, whose `default` is the export of the
    // package on Node (CommonJS) and on Bun (ES module).
    const loaded = (await import(specifier)) as { readonly default: object };

    return loaded.default;
  } catch (cause) {
    throw new Error(`the rerun driver tests need the dev dependency ${name} in clients/typescript/tests`, { cause });
  }
}

/** The `pg` module. */
async function pg(): Promise<PgModule> {
  // SAFETY: `pg` exports `Client`, a class with these methods (`@types/pg`).
  return (await driverDefault("pg")) as PgModule;
}

/** The `postgres` function of `postgres.js`. */
async function postgres(): Promise<PostgresModule> {
  // SAFETY: the default export of `postgres` is this function (`postgres/types`).
  return (await driverDefault("postgres")) as PostgresModule;
}

/** An unkeyed command for an unsent UUIDv7, with no deadline. */
function unsentCommand(): RerunCommand {
  const minted = mintTaskId();

  assert.ok(minted.kind === "minted");

  const sourceTaskId = TaskId.parse(minted.taskId);

  assert.ok(sourceTaskId.ok, "the core mints a canonical UUID text");

  return { sourceTaskId: sourceTaskId.value, deadline: { kind: "none" } };
}

/** The answer of `rerunIn` on a `pg` client in a transaction that the test rolls back. */
async function inPgTransaction(
  area: RerunArea,
  command: RerunCommand,
  policy: RerunPolicy,
): Promise<Result<RerunOutcome, CallerRerunError>> {
  const client = new (await pg()).Client({ connectionString: testDatabaseUrl() });

  await client.connect();

  try {
    await client.query("BEGIN");

    return await area.rerunIn(pgTransaction(client), command, policy);
  } finally {
    await client.query("ROLLBACK");
    await client.end();
  }
}

/** The answer of `rerunIn` on a `postgres.js` transaction `sql`; the test ends the transaction. */
async function inPostgresTransaction(
  area: RerunArea,
  command: RerunCommand,
  policy: RerunPolicy,
): Promise<Result<RerunOutcome, CallerRerunError>> {
  const sql = (await postgres())(testDatabaseUrl(), { onnotice: () => undefined });

  try {
    return await sql.begin(async (transactionSql) => area.rerunIn(postgresTransaction(transactionSql), command, policy));
  } finally {
    await sql.end();
  }
}

/** The adapters under test, each with its transaction runner. */
const DRIVERS: readonly {
  readonly name: string;
  readonly run: typeof inPgTransaction;
}[] = [
  { name: "pg", run: inPgTransaction },
  { name: "postgres.js", run: inPostgresTransaction },
];

describe("app.rerunIn with a database", () => {
  let area: RerunArea;

  before(async () => {
    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");

    assert.equal((await handle.installSchema()).kind, "installed");
    area = createRerunArea({ coreHandle: () => handle, activeCarrier: () => NO_ACTIVE_CARRIER });
  });

  for (const driver of DRIVERS) {
    test(`${driver.name}: the source read runs in the caller's transaction and an unsent id is sourceAbsent`, async () => {
      const command = unsentCommand();

      assert.deepEqual(await driver.run(area, command, POLICY), {
        ok: true,
        value: { kind: "sourceAbsent", taskId: command.sourceTaskId, reason: "WITHIN_RETAINED_HISTORY" },
      });
    });

    test(`${driver.name}: a period with no class row is CLASS_NOT_REGISTERED`, async () => {
      const answer = await driver.run(area, unsentCommand(), { ...POLICY, retention: { kind: "days", days: UNREGISTERED_DAYS } });

      assert.ok(!answer.ok && answer.error.code === "CLASS_NOT_REGISTERED", JSON.stringify(answer));
      assert.equal(answer.error.classKey, `standard_${UNREGISTERED_DAYS}d`);
    });
  }

  test("pg: a closed client is DRIVER_FAILED with the driver's error", async () => {
    const client = new (await pg()).Client({ connectionString: testDatabaseUrl() });

    await client.connect();
    await client.end();

    const answer = await area.rerunIn(pgTransaction(client), unsentCommand(), POLICY);

    assert.ok(!answer.ok && answer.error.code === "DRIVER_FAILED", JSON.stringify(answer));
    assert.equal(answer.error.statement, "rerun_source_read");
    assert.ok(answer.error.driverError instanceof Error);
  });

  test("postgres.js: a transaction sql after its end is DRIVER_FAILED with the driver's error", async () => {
    const sql = (await postgres())(testDatabaseUrl(), { onnotice: () => undefined });
    const ended = await sql.begin(async (transactionSql) => transactionSql);

    await sql.end();

    const answer = await area.rerunIn(postgresTransaction(ended), unsentCommand(), POLICY);

    assert.ok(!answer.ok && answer.error.code === "DRIVER_FAILED", JSON.stringify(answer));
    assert.equal(answer.error.statement, "rerun_source_read");
  });
});
