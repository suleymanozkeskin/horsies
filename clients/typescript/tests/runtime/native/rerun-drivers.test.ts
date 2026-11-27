// The rerun in the caller's transaction with a built addon, a database, and each driver adapter:
// `pg` and `postgres.js`. The drivers are dev dependencies of the tests; this file loads them at run
// time, so the type check needs neither.
//
// Each case runs in a transaction that the test owns. The test, not the rerun, ends it.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, describe, test } from "node:test";

import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
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
import type { RerunCommand, RerunOutcome, RerunEnqueuePolicy } from "../../../horsies/src/rerun/types.ts";
import type { Horsies } from "../../../horsies/src/index.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";
import { closeApp, KEEP_POLICY, sendCancelled, sentApp } from "./sent-support.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The part of a `pg` client that the tests call. */
interface PgTestClient {
  connect(): Promise<void>;
  query(config: PgTextQuery): Promise<PgTextResult>;
  query(text: string): Promise<object>;
  getTransactionStatus(): string | null;
  end(): Promise<void>;
}

/** The options of a `pg` client that the tests set. */
interface PgTestClientConfig {
  readonly connectionString: string;
  readonly binary?: boolean;
}

/** The part of the `pg` module that the tests call. */
interface PgModule {
  readonly Client: new (config: PgTestClientConfig) => PgTestClient;
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

const POLICY: RerunEnqueuePolicy = {
  retention: { kind: "forever" },
  retainRerunInput: "keep",
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

  return { sourceTaskId: sourceTaskId.value, goodUntil: { kind: "none" } };
}

/** The answer of `rerunIn` on a `pg` client in a transaction that the test rolls back. */
async function inPgTransaction(
  area: RerunArea,
  command: RerunCommand,
  policy: RerunEnqueuePolicy,
): Promise<Result<RerunOutcome, CallerRerunError>> {
  const client = new (await pg()).Client({ connectionString: testDatabaseUrl() });

  await client.connect();

  try {
    await client.query("BEGIN");

    return await area.rerunTaskIn(pgTransaction(client), command, policy);
  } finally {
    await client.query("ROLLBACK");
    await client.end();
  }
}

/** The answer of `rerunIn` on a `postgres.js` transaction `sql`; the test ends the transaction. */
async function inPostgresTransaction(
  area: RerunArea,
  command: RerunCommand,
  policy: RerunEnqueuePolicy,
): Promise<Result<RerunOutcome, CallerRerunError>> {
  const sql = (await postgres())(testDatabaseUrl(), { onnotice: () => undefined });

  try {
    return await sql.begin(async (transactionSql) => area.rerunTaskIn(postgresTransaction(transactionSql), command, policy));
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

describe("app.rerunTaskIn with a database", () => {
  let area: RerunArea;

  before(async () => {
    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");

    assert.equal((await handle.installSchema()).kind, "installed");
    area = createRerunArea({ coreHandle: () => handle, activeCarrier: () => NO_ACTIVE_CARRIER, registeredCodes: () => ({ tasks: [] }) });
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
      const answer = await driver.run(area, unsentCommand(), { ...POLICY, retention: { kind: "period", days: UNREGISTERED_DAYS } });

      assert.ok(!answer.ok && answer.error.code === "CLASS_NOT_REGISTERED", JSON.stringify(answer));
      assert.equal(answer.error.classKey, `standard_${UNREGISTERED_DAYS}d`);
    });
  }

  test("pg: a client closed in its transaction is DRIVER_FAILED with the driver's error", async () => {
    const client = new (await pg()).Client({ connectionString: testDatabaseUrl() });

    await client.connect();
    await client.query("BEGIN");
    await client.end();

    const answer = await area.rerunTaskIn(pgTransaction(client), unsentCommand(), POLICY);

    assert.ok(!answer.ok && answer.error.code === "DRIVER_FAILED", JSON.stringify(answer));
    assert.equal(answer.error.statement, "rerun_source_read");
    assert.ok(answer.error.driverError instanceof Error);
  });

  test("pg: a client with no BEGIN is NO_OPEN_TRANSACTION, and no statement runs", async () => {
    const client = new (await pg()).Client({ connectionString: testDatabaseUrl() });

    await client.connect();

    try {
      const answer = await area.rerunTaskIn(pgTransaction(client), unsentCommand(), POLICY);

      assert.ok(!answer.ok && answer.error.code === "NO_OPEN_TRANSACTION", JSON.stringify(answer));
      assert.equal(answer.error.statement, "rerun_source_read");
      assert.deepEqual(answer.error.state, { kind: "idle" });
      assert.equal(client.getTransactionStatus(), "I");
    } finally {
      await client.end();
    }
  });

  test("pg: a failed transaction is NO_OPEN_TRANSACTION, and the caller rolls it back", async () => {
    const client = new (await pg()).Client({ connectionString: testDatabaseUrl() });

    await client.connect();

    try {
      await client.query("BEGIN");
      await assert.rejects(client.query("SELECT 1 / 0"));
      // pg rejects a query at its ErrorResponse and sets the transaction status at the ReadyForQuery after it, which
      // can arrive in a later read. pg sends the next query only after that ReadyForQuery, so when this second query
      // is rejected, the status is the failed transaction.
      await assert.rejects(client.query("SELECT 1"));

      const answer = await area.rerunTaskIn(pgTransaction(client), unsentCommand(), POLICY);

      assert.ok(!answer.ok && answer.error.code === "NO_OPEN_TRANSACTION", JSON.stringify(answer));
      assert.deepEqual(answer.error.state, { kind: "failedTransaction" });
    } finally {
      await client.query("ROLLBACK");
      await client.end();
    }
  });

  test("pg: a client with binary: true is RESULT_NOT_TEXT", async () => {
    const client = new (await pg()).Client({ connectionString: testDatabaseUrl(), binary: true });

    await client.connect();

    try {
      await client.query("BEGIN");

      const answer = await area.rerunTaskIn(pgTransaction(client), unsentCommand(), POLICY);

      assert.ok(!answer.ok && answer.error.code === "RESULT_NOT_TEXT", JSON.stringify(answer));
      assert.equal(answer.error.statement, "rerun_source_read");
      assert.equal(answer.error.format, "binary");
    } finally {
      await client.query("ROLLBACK");
      await client.end();
    }
  });

  test("postgres.js: a transaction sql after its end is DRIVER_FAILED with the driver's error", async () => {
    const sql = (await postgres())(testDatabaseUrl(), { onnotice: () => undefined });
    const ended = await sql.begin(async (transactionSql) => transactionSql);

    await sql.end();

    const answer = await area.rerunTaskIn(postgresTransaction(ended), unsentCommand(), POLICY);

    assert.ok(!answer.ok && answer.error.code === "DRIVER_FAILED", JSON.stringify(answer));
    assert.equal(answer.error.statement, "rerun_source_read");
  });
});

/** How the caller ends the transaction after the rerun: commit it, or roll it back. */
type Ending = "commit" | "rollback";

/** A rerun in a transaction that the test ends as `ending` says (an error of the rerun always rolls back). */
type EndedRun = (
  app: Horsies,
  command: RerunCommand,
  ending: Ending,
) => Promise<Result<RerunOutcome, CallerRerunError>>;

/** Whether the caller commits after `answer`: only an `ending` of commit after a rerun with no error. */
function commits(ending: Ending, answer: Result<RerunOutcome, CallerRerunError>): boolean {
  return ending === "commit" && answer.ok;
}

/** The rerun of `command` on a `pg` client in a transaction that ends as `ending` says. */
const endedPgRun: EndedRun = async (app, command, ending) => {
  const client = new (await pg()).Client({ connectionString: testDatabaseUrl() });

  await client.connect();

  try {
    await client.query("BEGIN");

    const answer = await app.rerunTaskIn(pgTransaction(client), command, KEEP_POLICY);

    await client.query(commits(ending, answer) ? "COMMIT" : "ROLLBACK");

    return answer;
  } finally {
    await client.end();
  }
};

/** The answer of a rerun that a `postgres.js` transaction rolled back. */
class RolledBack extends Error {
  readonly answer: Result<RerunOutcome, CallerRerunError>;

  constructor(answer: Result<RerunOutcome, CallerRerunError>) {
    super("the test rolls the transaction back");
    this.answer = answer;
  }
}

/** The rerun of `command` on a `postgres.js` transaction that ends as `ending` says. */
const endedPostgresRun: EndedRun = async (app, command, ending) => {
  const sql = (await postgres())(testDatabaseUrl(), { onnotice: () => undefined });

  try {
    return await sql.begin(async (transactionSql) => {
      const answer = await app.rerunTaskIn(postgresTransaction(transactionSql), command, KEEP_POLICY);

      if (!commits(ending, answer)) {
        throw new RolledBack(answer);
      }

      return answer;
    });
  } catch (cause) {
    if (cause instanceof RolledBack) {
      return cause.answer;
    }

    throw cause;
  } finally {
    await sql.end();
  }
};

/** The id of the new task of an `enqueued` answer, or the failed test. */
function enqueuedIdOf(answer: Result<RerunOutcome, unknown>): string {
  assert.ok(answer.ok && answer.value.kind === "enqueued", JSON.stringify(answer));

  return answer.value.newTaskId;
}

/** A deadline with every microsecond digit set, a year ahead: it holds a value in each part of the stored time. */
function microsecondDeadline(): Temporal.Instant {
  return Temporal.Instant.from(`${String(Temporal.Now.plainDateISO().year + 1)}-03-04T05:06:07.123456Z`);
}

const ENDED_DRIVERS: readonly { readonly name: string; readonly run: EndedRun }[] = [
  { name: "pg", run: endedPgRun },
  { name: "postgres.js", run: endedPostgresRun },
];

describe("app.rerunTaskIn with a keyed rerun of a sent task", () => {
  let app: Horsies;

  before(() => {
    app = sentApp(testDatabaseUrl(), "keep");
  });

  after(async () => {
    await closeApp(app);
  });

  for (const driver of ENDED_DRIVERS) {
    test(`${driver.name}: after the caller commits, the same command in the app's transaction answers keyReplay with the new task`, async () => {
      const source = await sendCancelled(app, 20);
      const command: RerunCommand = { sourceTaskId: source, goodUntil: { kind: "none" }, idempotencyKey: `commit-${randomUUID()}` };
      const created = enqueuedIdOf(await driver.run(app, command, "commit"));
      const replay = await app.rerunTask(command, KEEP_POLICY);

      assert.deepEqual(replay, { ok: true, value: { kind: "keyReplay", existingTaskId: created } });
    });

    test(`${driver.name}: after the caller rolls back, the same command in the app's transaction answers enqueued with another task`, async () => {
      const source = await sendCancelled(app, 21);
      const command: RerunCommand = { sourceTaskId: source, goodUntil: { kind: "none" }, idempotencyKey: `rollback-${randomUUID()}` };
      const rolledBack = enqueuedIdOf(await driver.run(app, command, "rollback"));
      const again = await app.rerunTask(command, KEEP_POLICY);

      assert.ok(again.ok && again.value.kind === "enqueued", JSON.stringify(again));
      assert.notEqual(again.value.newTaskId, rolledBack);
    });

    test(`${driver.name}: a keyed rerun with a deadline sends each parameter type, and the detail read gives the deadline to the microsecond`, async () => {
      const source = await sendCancelled(app, 22);
      const goodUntil = microsecondDeadline();

      const command: RerunCommand = {
        sourceTaskId: source,
        goodUntil: { kind: "until", at: goodUntil },
        idempotencyKey: `types-${randomUUID()}`,
      };

      const created = enqueuedIdOf(await driver.run(app, command, "commit"));
      const parsed = TaskId.parse(created);

      assert.ok(parsed.ok, "the new task id is a canonical UUID");

      const detail = await app.monitoring.getTaskDetail(parsed.value);

      assert.ok(detail.ok && detail.value.kind === "found", JSON.stringify(detail));
      assert.equal(detail.value.taskDetail.taskRow.goodUntil?.epochNanoseconds, goodUntil.epochNanoseconds);
    });
  }
});
