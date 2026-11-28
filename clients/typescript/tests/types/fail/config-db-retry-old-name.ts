// expect: TS2353 at 8
// The retry of a transient database error in a worker is `databaseRetryMaxMs` (and `databaseRetryInitialMs`,
// `databaseRetryMaxAttempts`); `dbRetryMaxMs` is no resilience field.

import type { ResilienceConfigInput } from "../../../horsies/src/index.ts";

export const resilience: ResilienceConfigInput = {
  dbRetryMaxMs: 5_000,
};
