// The core handle of the package: each method calls the addon through the call wrapper, so a throw
// or a rejection of the call is a defect under the defect rule, and a thrown envelope is its error.

import { nativeValue, nativeValueAsync } from "../call/envelope.ts";
import type { NativeOutcome } from "../values/records.ts";
import { handleAddon } from "./addon.ts";
import type {
  DatabasePingRecord,
  HandleProfile,
  InstallCoverageArg,
  InstallOutcome,
  NativeCoreHandle,
  SchemaStatusRecord,
} from "./addon.ts";

/**
 * A core handle of the producer manifest `manifestJson`, with the pool profile of this process.
 * Starts nothing. Throws `ManifestRefused` for a manifest that the core refuses, and the error of a failed
 * addon load (`CoreStartFailed`, `AddonOutsideMainThread`), not a defect of the call.
 */
export function buildCoreHandle(manifestJson: string, profile: HandleProfile): CoreHandle {
  const addon = handleAddon();

  return new CoreHandle(nativeValue("buildCoreHandle", () => addon.buildCoreHandle(manifestJson, profile)));
}

/** The core handle of one producer manifest. */
export class CoreHandle {
  /** The addon's handle. The functions of later areas take it as their first argument. */
  readonly native: NativeCoreHandle;

  constructor(native: NativeCoreHandle) {
    this.native = native;
  }

  /** The version of the manifest this handle was built from. */
  manifestVersion(): number {
    return nativeValue("manifestVersion", () => this.native.manifestVersion());
  }

  /**
   * A new handle of the same manifest and profile whose schema install ends with the coverage pass
   * of `coverage`. Starts nothing. Values that the core refuses are a `ClientDefect`.
   */
  withInstallCoverage(coverage: InstallCoverageArg): CoreHandle {
    return new CoreHandle(nativeValue("withInstallCoverage", () => this.native.withInstallCoverage(coverage)));
  }

  /** Close the handle's core pools. */
  async close(): Promise<undefined> {
    return nativeValueAsync("close", () => this.native.close());
  }

  /** One round trip to the database, or the catalog error of a ping that failed. */
  async pingDatabase(): Promise<NativeOutcome<DatabasePingRecord>> {
    return nativeValueAsync("pingDatabase", () => this.native.pingDatabase());
  }

  /** Install the schema, as the schema mode of the manifest says. */
  async installSchema(): Promise<InstallOutcome> {
    return nativeValueAsync("installSchema", () => this.native.installSchema());
  }

  /** The schema state of the database. A database that cannot be read is `UNKNOWN`. */
  async schemaStatus(): Promise<SchemaStatusRecord> {
    return nativeValueAsync("schemaStatus", () => this.native.schemaStatus());
  }
}
