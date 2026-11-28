// expect: TS2322 at 8
// The pool recycle by age is `{ kind: "after", seconds }`; `"afterSeconds"` is no kind.

import type { DatabaseConfigInput } from "../../../horsies/src/index.ts";

export const broker: DatabaseConfigInput = {
  databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" },
  poolRecycle: { kind: "afterSeconds", seconds: 3_600 },
};
