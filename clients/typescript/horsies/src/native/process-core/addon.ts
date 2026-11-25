// The functions of the `process_core` area of the addon, and the accessor of them.

import type { PanicRule } from "../../thrown-errors.ts";
import { requireAddon } from "../call/addon.ts";
import type { DefectAddon } from "../call/defect.ts";
import type { NativeEnvelope } from "../call/envelope.ts";

/** The state of the core in this process. `inherited`: this process is a fork of the process where the core started. */
export type CoreState = "not_started" | "running" | "stopped" | "inherited";

/** A minted task id (UUIDv7, lower-case hyphenated text), or the refusal of the id source. */
export type MintOutcome =
  | { readonly kind: "minted"; readonly taskId: string }
  | { readonly kind: "refused"; readonly cause: string };

/** One SQL file of the migration chain. */
export interface MigrationSqlFile {
  readonly version: bigint;
  readonly name: string;
  readonly text: string;
}

/**
 * The functions of the `process_core` area, with the two steps of the defect rule. None of them
 * starts the core.
 */
export interface ProcessCoreAddon extends DefectAddon {
  coreState(): NativeEnvelope<CoreState>;
  mintTaskId(): NativeEnvelope<MintOutcome>;
  migrationSqlFiles(): NativeEnvelope<MigrationSqlFile[]>;
  setPanicRule(rule: PanicRule): NativeEnvelope<undefined>;
  environmentCount(): NativeEnvelope<number>;
}

/** The `process_core` functions of the addon. Throws as `requireAddon` does. */
export function processCoreAddon(): ProcessCoreAddon {
  return processCoreView(requireAddon());
}

/** The `process_core` view of a loaded export object. */
export function processCoreView(exports: object): ProcessCoreAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return exports as ProcessCoreAddon;
}
