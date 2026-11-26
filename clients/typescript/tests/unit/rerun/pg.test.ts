// The `pg` adapter with a fake client: each parameter goes as text or null, each cell comes back as
// the server's text, one row is the answer, and a driver failure is a value. It never commits, it runs
// nothing on a client with no open transaction, and it refuses a result that is not text.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { CallerConnectionState, CallerStatement } from "../../../horsies/src/rerun/caller.ts";
import { pgTransaction } from "../../../horsies/src/rerun/pg.ts";
import type { PgClient, PgField, PgTextQuery, PgTextResult } from "../../../horsies/src/rerun/pg.ts";

/** A statement with one parameter of each type, as the core renders them. */
const STATEMENT: CallerStatement = {
  name: "rerun_enqueue",
  sql: "SELECT outcome, task_id FROM horsies_enqueue_task($1::uuid, $2::bool, $3::int2, $4::int4, $5::int8, $6::text, $7::bytea, $8::timestamptz, $9::bytea)",
  parameters: [
    "01890a5d-ac96-774b-bcce-b302099a8057",
    "true",
    "-7",
    "2147483647",
    "-9223372036854775808",
    "a 'quoted' text",
    "\\x000aff",
    "2026-11-26T09:10:12.654321Z",
    null,
  ],
  columns: [
    { name: "outcome", type: "text" },
    { name: "task_id", type: "uuid" },
  ],
};

/** The result columns of `STATEMENT` in text format. */
const TEXT_FIELDS: readonly PgField[] = [
  { name: "outcome", format: "text" },
  { name: "task_id", format: "text" },
];

/** The result of `STATEMENT` with `rows`, each column in text format. */
function textResult(rows: PgTextResult["rows"]): PgTextResult {
  return { fields: TEXT_FIELDS, rows };
}

/** The transaction status of a client on which the caller ran `BEGIN`. */
const IN_TRANSACTION = "T";

/**
 * A client whose transaction status is `status` and that answers each query with `answer`. It records
 * each query.
 */
function fakeClient(
  answer: () => Promise<PgTextResult>,
  status: string | null = IN_TRANSACTION,
): { readonly client: PgClient; readonly queries: PgTextQuery[] } {
  const queries: PgTextQuery[] = [];

  return {
    queries,
    client: {
      query: async (config) => {
        queries.push(config);

        return answer();
      },
      getTransactionStatus: () => status,
    },
  };
}

describe("pgTransaction", () => {
  test("sends the SQL text and each parameter as text or null, in array row mode", async () => {
    const { client, queries } = fakeClient(async () => textResult([["INSERTED", "01890a5d-ac96-774b-bcce-b302099a8058"]]));

    await pgTransaction(client).queryOne(STATEMENT);
    assert.equal(queries.length, 1);
    assert.equal(queries[0]?.text, STATEMENT.sql);
    assert.deepEqual(queries[0]?.values, STATEMENT.parameters);
    assert.equal(queries[0]?.rowMode, "array");
  });

  test("reads each cell of every type as the text that the server sent", async () => {
    const { client, queries } = fakeClient(async () => textResult([]));

    await pgTransaction(client).queryOne(STATEMENT);

    const types = queries[0]?.types;

    assert.ok(types !== undefined, "the query has type parsers");

    const cells: [number, string][] = [
      [16, "t"],
      [20, "-9223372036854775808"],
      [17, "\\x000aff"],
      [1184, "2026-11-26 10:10:12.654321+01"],
      [2950, "01890a5d-ac96-774b-bcce-b302099a8057"],
    ];

    for (const [oid, text] of cells) {
      assert.equal(types.getTypeParser(oid)(text), text, `${oid}`);
    }
  });

  test("gives the one row of the statement", async () => {
    const { client } = fakeClient(async () => textResult([["INSERTED", null]]));

    assert.deepEqual(await pgTransaction(client).queryOne(STATEMENT), { ok: true, value: { cells: ["INSERTED", null] } });
  });

  test("refuses a result of no row or of two rows", async () => {
    for (const rows of [[], [["INSERTED", null], ["INSERTED", null]]]) {
      const { client } = fakeClient(async () => textResult(rows));

      assert.deepEqual(await pgTransaction(client).queryOne(STATEMENT), {
        ok: false,
        error: { kind: "rowRefused", detail: `statement rerun_enqueue gave ${rows.length} rows; it gives one row` },
      });
    }
  });

  test("gives a rejection of the driver as driverFailed with the same error", async () => {
    const driverError = Object.assign(new Error("invalid input syntax for type integer"), { code: "22P02" });
    const { client } = fakeClient(async () => Promise.reject(driverError));

    assert.deepEqual(await pgTransaction(client).queryOne(STATEMENT), { ok: false, error: { kind: "driverFailed", driverError } });
  });

  test("runs nothing on a client with no open transaction, and names the state of each status", async () => {
    const cases: [string | null, CallerConnectionState][] = [
      ["I", { kind: "idle" }],
      ["E", { kind: "failedTransaction" }],
      [null, { kind: "notConnected" }],
      ["X", { kind: "unrecognized", status: "X" }],
    ];

    for (const [status, state] of cases) {
      const { client, queries } = fakeClient(async () => textResult([["INSERTED", null]]), status);

      assert.deepEqual(
        await pgTransaction(client).queryOne(STATEMENT),
        { ok: false, error: { kind: "noOpenTransaction", state } },
        String(status),
      );
      assert.deepEqual(queries, [], String(status));
    }
  });

  test("refuses a result column in binary format, as a client with binary: true gives it", async () => {
    const fields: readonly PgField[] = [
      { name: "outcome", format: "text" },
      { name: "task_id", format: "binary" },
    ];

    const { client } = fakeClient(async () => ({ fields, rows: [["INSERTED", "\u0001\u0089"]] }));

    assert.deepEqual(await pgTransaction(client).queryOne(STATEMENT), {
      ok: false,
      error: { kind: "resultNotText", column: "task_id", format: "binary" },
    });
  });

  test("wraps a rejection with a value that is not an Error", async () => {
    const { client } = fakeClient(async () => Promise.reject("connection reset"));
    const answer = await pgTransaction(client).queryOne(STATEMENT);

    assert.ok(!answer.ok && answer.error.kind === "driverFailed");
    assert.equal(answer.error.driverError.message, "connection reset");
  });
});
