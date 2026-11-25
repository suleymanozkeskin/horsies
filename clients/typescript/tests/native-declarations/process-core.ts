// The generated declarations of the `process_core` area match the hand-kept ones.

import type * as Generated from "../../bridge/index.js";
import type { CoreState, MigrationSqlFile, MintOutcome, ProcessCoreAddon } from "../../horsies/src/native/process-core/addon.ts";
import type { Mutual } from "./mutual.ts";

export const functionsMatch: Mutual<Pick<typeof Generated, keyof ProcessCoreAddon>, ProcessCoreAddon> = true;
export const coreStateMatches: Mutual<Generated.CoreState, CoreState> = true;
export const mintOutcomeMatches: Mutual<Generated.MintOutcome, MintOutcome> = true;
export const migrationFileMatches: Mutual<Generated.MigrationSqlFile, MigrationSqlFile> = true;
