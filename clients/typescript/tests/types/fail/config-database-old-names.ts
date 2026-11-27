// expect: TS2353 at 9, TS2322 at 13
// The database section of the config is `database` (not `broker`), and the cluster-wide cap is a
// `ClusterCapSetting`: `{ kind: "clusterCap", tasks }` or `{ kind: "noClusterCap" }`, not the queue cap tags.

import type { AppConfigInput, DatabaseConfigInput } from "../../../horsies/src/index.ts";

const database: DatabaseConfigInput = { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } };

export const oldSection: AppConfigInput = { broker: database };

export const oldCap: AppConfigInput = {
  database,
  clusterWideCap: { kind: "cap", tasks: 4 },
};
