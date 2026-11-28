// expect: TS2322 at 8
// A soft cap claim mode with no lease: the core refuses a prefetch buffer above 0 without a claim lease.

import type { AppConfigInput } from "../../../horsies/src/index.ts";

export const config: AppConfigInput = {
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  claim: { kind: "softCap", prefetchBuffer: 4 },
};
