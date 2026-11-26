// expect: TS2322 at 9
// A PgBouncer transaction-pool connection with no session database URL: the core refuses the mode without one.

import type { AppConfigInput } from "../../../horsies/src/index.ts";

export const config: AppConfigInput = {
  broker: {
    databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" },
    connection: { kind: "pgbouncerTransaction" },
  },
};
