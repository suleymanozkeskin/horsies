// The driver adapters take the drivers' own types: a `pg.Client` and a `pg.PoolClient`, and the transaction `sql` that
// `sql.begin` of `postgres.js` gives its callback.

import type { Client, PoolClient } from "pg";
import type { TransactionSql } from "postgres";

import type { CallerTransaction } from "../../../horsies/src/index.ts";
import { pgTransaction } from "../../../horsies/src/rerun/pg.ts";
import { postgresTransaction } from "../../../horsies/src/rerun/postgres.ts";

declare const client: Client;
declare const poolClient: PoolClient;
declare const sql: TransactionSql;

export const ofClient: CallerTransaction = pgTransaction(client);
export const ofPoolClient: CallerTransaction = pgTransaction(poolClient);
export const ofTransactionSql: CallerTransaction = postgresTransaction(sql);
