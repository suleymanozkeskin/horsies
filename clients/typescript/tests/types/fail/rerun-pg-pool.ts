// expect: TS2741 at 9; floor: TS2345 at 9
// `pgTransaction` takes a client on which the caller ran `BEGIN`, not a `pg.Pool`: each query of a pool can run on
// another connection, outside the caller's transaction.

import type { Pool } from "pg";
import { pgTransaction } from "../../../horsies/src/rerun/pg.ts";

declare const pool: Pool;
export const transaction = pgTransaction(pool);
