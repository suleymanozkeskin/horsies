// expect: TS2322 at 7
// A worker profile setting names its kind: `{ kind: "isolatedChildren", count }` or `{ kind: "noIsolatedChildren" }`.

import type { WorkerProfileInput } from "../../../horsies/src/index.ts";

export const profile: WorkerProfileInput = {
  isolated: { count: 2 },
};
