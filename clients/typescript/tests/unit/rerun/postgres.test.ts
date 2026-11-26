// The `postgres.js` adapter with a fake transaction `sql`: each parameter goes typed as text, each
// cell comes back raw as the server's text, one row is the answer, and a driver failure is a value.
// It never commits.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { CallerStatement } from "../../../horsies/src/rerun/caller.ts";
import { postgresTransaction } from "../../../horsies/src/rerun/postgres.ts";
import type { PostgresRawRows, PostgresTransactionSql } from "../../../horsies/src/rerun/postgres.ts";

/** A parameter of the fake `sql`: its text and the type id that `typed` gave it. */
interface FakeParameter {
  readonly value: string;
  readonly type: number;
}

/** A statement with one parameter of each type, as the core renders them. */
const STATEMENT: CallerStatement = {
  name: "rerun_enqueue",
  sql: "SELECT outcome, task_id FROM horsies_enqueue_task($1::uuid, $2::bool, $3::bytea, $4::timestamptz, $5::int8, $6::bytea)",
  parameters: [
    "01890a5d-ac96-774b-bcce-b302099a8057",
    "true",
    "\\x000aff",
    "2026-11-26T09:10:12.654321Z",
    "-9223372036854775808",
    null,
  ],
  columns: [
    { name: "outcome", type: "text" },
    { name: "task_id", type: "uuid" },
  ],
};

/** The bytes of `text` as UTF-8. */
function bytesOf(text: string): Uint8Array {
  return new TextEncoder().encode(text);
}

/** A transaction `sql` whose raw queries answer `answer`, and the queries that it ran. */
function fakeSql(answer: () => Promise<PostgresRawRows>): {
  readonly sql: PostgresTransactionSql<FakeParameter>;
  readonly queries: { readonly query: string; readonly parameters: (FakeParameter | null)[] }[];
} {
  const queries: { readonly query: string; readonly parameters: (FakeParameter | null)[] }[] = [];

  return {
    queries,
    sql: {
      unsafe: (query, parameters) => {
        queries.push({ query, parameters });

        return { raw: answer };
      },
      typed: (value, type) => ({ value, type }),
    },
  };
}

describe("postgresTransaction", () => {
  test("sends each parameter typed as text, and null as null", async () => {
    const { sql, queries } = fakeSql(async () => [[bytesOf("INSERTED"), null]]);

    const answer = await postgresTransaction(sql).queryOne(STATEMENT);

    assert.ok(answer.ok);
    assert.deepEqual(queries, [
      {
        query: STATEMENT.sql,
        parameters: [
          { value: "01890a5d-ac96-774b-bcce-b302099a8057", type: 25 },
          { value: "true", type: 25 },
          { value: "\\x000aff", type: 25 },
          { value: "2026-11-26T09:10:12.654321Z", type: 25 },
          { value: "-9223372036854775808", type: 25 },
          null,
        ],
      },
    ]);
  });

  test("gives the one row with each raw cell as its text", async () => {
    const { sql } = fakeSql(async () => [[bytesOf("INSERTED"), bytesOf("\\x000aff"), bytesOf("2026-11-26 10:10:12.654321+01"), null]]);

    assert.deepEqual(await postgresTransaction(sql).queryOne(STATEMENT), {
      ok: true,
      value: { cells: ["INSERTED", "\\x000aff", "2026-11-26 10:10:12.654321+01", null] },
    });
  });

  test("refuses a result of no row or of two rows", async () => {
    for (const rows of [[], [[null], [null]]]) {
      const { sql } = fakeSql(async () => rows);

      assert.deepEqual(await postgresTransaction(sql).queryOne(STATEMENT), {
        ok: false,
        error: { kind: "rowRefused", detail: `statement rerun_enqueue gave ${rows.length} rows; it gives one row` },
      });
    }
  });

  test("refuses a cell that is not UTF-8 text", async () => {
    const { sql } = fakeSql(async () => [[Uint8Array.of(0xff, 0xfe)]]);
    const answer = await postgresTransaction(sql).queryOne(STATEMENT);

    assert.ok(!answer.ok && answer.error.kind === "rowRefused");
    assert.match(answer.error.detail, /^statement rerun_enqueue gave a cell that is not UTF-8 text: /u);
  });

  test("gives a rejection of the driver as driverFailed with the same error", async () => {
    const driverError = Object.assign(new Error("invalid input syntax for type integer"), { code: "22P02" });
    const { sql } = fakeSql(async () => Promise.reject(driverError));

    assert.deepEqual(await postgresTransaction(sql).queryOne(STATEMENT), { ok: false, error: { kind: "driverFailed", driverError } });
  });

  test("gives a throw of the driver before the query as driverFailed", async () => {
    const driverError = new Error("UNDEFINED_VALUE");

    const sql: PostgresTransactionSql<FakeParameter> = {
      unsafe: () => {
        throw driverError;
      },
      typed: (value, type) => ({ value, type }),
    };

    assert.deepEqual(await postgresTransaction(sql).queryOne(STATEMENT), { ok: false, error: { kind: "driverFailed", driverError } });
  });
});
