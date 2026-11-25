// The generated declarations of the `handle` area match the hand-kept ones, function by function and
// method by method (arguments and result), and type by type.

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
import type { Mutual, SameFunction } from "./mutual.ts";

type Handle = Generated.CoreHandle;

export const buildCoreHandleMatches: SameFunction<typeof Generated.buildCoreHandle, HandleAddon["buildCoreHandle"]> =
  true;

export const manifestVersionMatches: SameFunction<Handle["manifestVersion"], NativeCoreHandle["manifestVersion"]> =
  true;

export const withInstallCoverageMatches: SameFunction<
  Handle["withInstallCoverage"],
  NativeCoreHandle["withInstallCoverage"]
> = true;

export const closeMatches: SameFunction<Handle["close"], NativeCoreHandle["close"]> = true;
export const pingDatabaseMatches: SameFunction<Handle["pingDatabase"], NativeCoreHandle["pingDatabase"]> = true;
export const installSchemaMatches: SameFunction<Handle["installSchema"], NativeCoreHandle["installSchema"]> = true;
export const schemaStatusMatches: SameFunction<Handle["schemaStatus"], NativeCoreHandle["schemaStatus"]> = true;

// The member sets: no method on one side only.
export const handleMatches: Mutual<Handle, NativeCoreHandle> = true;

export const profileMatches: Mutual<Generated.HandleProfile, HandleProfile> = true;
export const pingMatches: Mutual<Generated.DatabasePingRecord, DatabasePingRecord> = true;
export const installMatches: Mutual<Generated.InstallOutcome, InstallOutcome> = true;
export const statusMatches: Mutual<Generated.SchemaStatusRecord, SchemaStatusRecord> = true;
export const coverageMatches: Mutual<Generated.InstallCoverageArg, InstallCoverageArg> = true;
