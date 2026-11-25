// The generated declarations of the `handle` area match the hand-kept ones.

import type * as Generated from "../../bridge/index.js";
import type {
  DatabasePingRecord,
  HandleAddon,
  HandleProfile,
  InstallCoverageArg,
  InstallOutcome,
  NativeCoreHandle,
  SchemaStatusRecord,
} from "../../horsies/src/native/handle/addon.ts";
import type { Mutual } from "./mutual.ts";

export const functionsMatch: Mutual<Pick<typeof Generated, keyof HandleAddon>, HandleAddon> = true;
export const handleMatches: Mutual<Generated.CoreHandle, NativeCoreHandle> = true;
export const profileMatches: Mutual<Generated.HandleProfile, HandleProfile> = true;
export const pingMatches: Mutual<Generated.DatabasePingRecord, DatabasePingRecord> = true;
export const installMatches: Mutual<Generated.InstallOutcome, InstallOutcome> = true;
export const statusMatches: Mutual<Generated.SchemaStatusRecord, SchemaStatusRecord> = true;
export const coverageMatches: Mutual<Generated.InstallCoverageArg, InstallCoverageArg> = true;
