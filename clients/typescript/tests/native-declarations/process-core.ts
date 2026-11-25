// The generated declarations of the `process_core` area match the hand-kept ones, function by
// function (arguments and result) and type by type.

import type * as Generated from "../../bridge/index.js";
import type { DefectAnswer, RecordHandoff } from "../../horsies/src/native/call/defect.ts";
import type {
  CoreState,
  MigrationSqlFile,
  MintOutcome,
  ProcessCoreAddon,
} from "../../horsies/src/native/process-core/addon.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

type Kept = ProcessCoreAddon;

export const coreStateMatches: SameFunction<typeof Generated.coreState, Kept["coreState"]> = true;
export const mintTaskIdMatches: SameFunction<typeof Generated.mintTaskId, Kept["mintTaskId"]> = true;

export const migrationSqlFilesMatches: SameFunction<typeof Generated.migrationSqlFiles, Kept["migrationSqlFiles"]> =
  true;

export const setPanicRuleMatches: SameFunction<typeof Generated.setPanicRule, Kept["setPanicRule"]> = true;

export const environmentCountMatches: SameFunction<typeof Generated.environmentCount, Kept["environmentCount"]> =
  true;

export const reportDefectMatches: SameFunction<typeof Generated.reportDefect, Kept["reportDefect"]> = true;
export const abortForDefectMatches: SameFunction<typeof Generated.abortForDefect, Kept["abortForDefect"]> = true;

export const coreStateTypeMatches: Mutual<Generated.CoreState, CoreState> = true;
export const mintOutcomeMatches: Mutual<Generated.MintOutcome, MintOutcome> = true;
export const migrationFileMatches: Mutual<Generated.MigrationSqlFile, MigrationSqlFile> = true;
export const defectAnswerMatches: Mutual<Generated.DefectAnswer, DefectAnswer> = true;
export const recordHandoffMatches: Mutual<Generated.RecordHandoff, RecordHandoff> = true;
