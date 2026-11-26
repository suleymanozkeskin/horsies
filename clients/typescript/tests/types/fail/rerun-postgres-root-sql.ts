// expect: TS2345 at 9
// `postgresTransaction` takes the transaction `sql` of `sql.begin`, not the root `sql`: each query of the root `sql`
// can run on another connection, outside the caller's transaction.

import type { Sql } from "postgres";
import { postgresTransaction } from "../../../horsies/src/rerun/postgres.ts";

declare const sql: Sql;
export const transaction = postgresTransaction(sql);
