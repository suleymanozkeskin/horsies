// expect: TS2322 at 8
// The pool recycle by age is `{ kind: "after", seconds }`; `"afterSeconds"` is no kind.

import type { BrokerConfigInput } from "../../../horsies/src/index.ts";

export const broker: BrokerConfigInput = {
  databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" },
  poolRecycle: { kind: "afterSeconds", seconds: 3_600 },
};
