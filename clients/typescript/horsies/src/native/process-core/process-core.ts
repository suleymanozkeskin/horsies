// The process-level functions of the addon: the core state, the panic rule, the defect report, the
// task id source, the migration files and the environment count. None of them starts the core.

import type { PanicRule } from "../../thrown-errors.ts";
import { nativeValue } from "../call/envelope.ts";
import { processCoreAddon } from "./addon.ts";
import type { CoreState, MigrationSqlFile, MintOutcome } from "./addon.ts";

/** The state of the core in this process. */
export function coreState(): CoreState {
  return nativeValue("coreState", () => processCoreAddon().coreState());
}

/**
 * Set the panic rule of this process, before the first core call. The rule in force again changes
 * nothing; another rule throws `PanicRuleFixed`.
 */
export function setPanicRule(rule: PanicRule): void {
  nativeValue("setPanicRule", () => processCoreAddon().setPanicRule(rule));
}

/** Mint a new task id (UUIDv7). Throws `CoreStopped` in a process whose core stopped. */
export function mintTaskId(): MintOutcome {
  return nativeValue("mintTaskId", () => processCoreAddon().mintTaskId());
}

/** Every SQL file of the migration chain, in the order they run. */
export function migrationSqlFiles(): readonly MigrationSqlFile[] {
  return nativeValue("migrationSqlFiles", () => processCoreAddon().migrationSqlFiles());
}

/** The count of Node-API environments that registered the addon in this process. */
export function environmentCount(): number {
  return nativeValue("environmentCount", () => processCoreAddon().environmentCount());
}
