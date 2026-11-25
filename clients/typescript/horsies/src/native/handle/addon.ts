// The functions of the `handle` area of the addon, the records of its operations, and the accessor.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { NativeOutcome, RawCatalogError } from "../values/records.ts";

/** The pool profile of the process of a handle. */
export type HandleProfile = "PRODUCER" | "WORKER";

/** One round trip of a database ping. */
export interface DatabasePingRecord {
  readonly latencyMs: number;
}

/** How one schema install ended. `notRun`: the schema mode of the manifest runs no install. */
export type InstallOutcome =
  | { readonly kind: "installed"; readonly version: bigint }
  | { readonly kind: "notRun" }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** The schema state of a database. */
export type SchemaStateName = "MATCH" | "MISMATCH" | "ABSENT" | "ZERO_X" | "UNKNOWN";

/** The stored version of a database. */
export type StoredVersion = { readonly kind: "stored"; readonly version: bigint } | { readonly kind: "noVersion" };

/** The schema state of the database, against the version this core expects. */
export interface SchemaStatusRecord {
  readonly state: SchemaStateName;
  readonly stored: StoredVersion;
  readonly expectedVersion: bigint;
}

/** The values of the coverage pass that a schema install runs. Each count is a whole number from 0 to 4294967295. */
export interface InstallCoverageArg {
  readonly historyHorizonDays: number;
  readonly heartbeatHorizonHours: number;
  readonly declaredPeriodDays: number[];
}

/** The core handle of one producer manifest. */
export interface NativeCoreHandle {
  manifestVersion(): NativeEnvelope<number>;
  withInstallCoverage(coverage: InstallCoverageArg): NativeEnvelope<NativeCoreHandle>;
  close(): Promise<NativeEnvelope<undefined>>;
  pingDatabase(): Promise<NativeEnvelope<NativeOutcome<DatabasePingRecord>>>;
  installSchema(): Promise<NativeEnvelope<InstallOutcome>>;
  schemaStatus(): Promise<NativeEnvelope<SchemaStatusRecord>>;
}

/** The functions of the `handle` area. */
export interface HandleAddon {
  buildCoreHandle(manifestJson: string, profile: HandleProfile): NativeEnvelope<NativeCoreHandle>;
}

/** The `handle` functions of the addon. Throws as `requireAddon` does. */
export function handleAddon(): HandleAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as HandleAddon;
}
