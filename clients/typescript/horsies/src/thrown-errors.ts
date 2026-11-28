// The errors that a call into the core throws.
//
// An expected failure of an operation is not thrown: the operation resolves with a value that names
// it. Two families are thrown:
// - `CoreError`: the call could not use the core in this process (a process-state refusal).
// - `CoreDefect`: a defect of the package, or a panic in the native bridge. Do not catch it to go
//   on: the process needs a fix or a restart. In a worker process a defect ends the process.

/** What remains true after a call that a stop of the core ended early. */
export type StateAfter = "noChange" | "outcomeUnknown";

/** The panic rule of a process. */
export type PanicRule = "STOP_CORE" | "ABORT_PROCESS";

/** The codes of the process-state refusals. */
export type CoreErrorCode =
  | "CORE_STOPPED"
  | "RUNTIME_START_FAILED"
  | "TOO_MANY_PENDING_CALLS"
  | "FORKED_AFTER_CORE_START";

/** The codes of the defects. */
export type CoreDefectCode =
  | "MANIFEST_REFUSED"
  | "PANIC_RULE_FIXED"
  | "CORE_PANIC"
  | "ADDON_OUTSIDE_MAIN_THREAD"
  | "CLIENT_DEFECT";

/** Base of the process-state refusals: the call could not use the core in this process. */
export abstract class CoreError extends Error {
  abstract readonly code: CoreErrorCode;
}

/** Base of the defects of the package and of the panics of the native bridge. */
export abstract class CoreDefect extends Error {
  abstract readonly code: CoreDefectCode;
}

/**
 * A core background task panicked, so the core is stopped in this process. `subject` and `cause`
 * are those of the first panic. `stateAfter`: `noChange` for a read; `outcomeUnknown` for a write,
 * which may have committed.
 */
export class CoreStopped extends CoreError {
  override readonly name = "CoreStopped";
  readonly code = "CORE_STOPPED";
  readonly subject: string;
  override readonly cause: string;
  readonly stateAfter: StateAfter;

  constructor(facts: { readonly subject: string; readonly cause: string; readonly stateAfter: StateAfter }) {
    super(`the core stopped in this process: panic in ${facts.subject}: ${facts.cause}`);
    this.subject = facts.subject;
    this.cause = facts.cause;
    this.stateAfter = facts.stateAfter;
  }
}

/** The core could not start in this process: its runtime, its log forwarder or its native addon. */
export class RuntimeStartFailed extends CoreError {
  override readonly name = "RuntimeStartFailed";
  readonly code = "RUNTIME_START_FAILED";
  override readonly cause: string;

  constructor(facts: { readonly cause: string }) {
    super(`the core could not start: ${facts.cause}`);
    this.cause = facts.cause;
  }
}

/** The process already has the most core calls in flight that it may have, `bound`. */
export class TooManyPendingCalls extends CoreError {
  override readonly name = "TooManyPendingCalls";
  readonly code = "TOO_MANY_PENDING_CALLS";
  readonly subject: string;
  readonly bound: number;

  constructor(facts: { readonly subject: string; readonly bound: number }) {
    super(`${facts.subject}: the process has ${facts.bound} core calls in flight, the most it may have`);
    this.subject = facts.subject;
    this.bound = facts.bound;
  }
}

/** This process is a fork without exec of the process where the core started. */
export class ForkedAfterCoreStart extends CoreError {
  override readonly name = "ForkedAfterCoreStart";
  readonly code = "FORKED_AFTER_CORE_START";
  readonly corePid: number;
  readonly thisPid: number;

  constructor(facts: { readonly corePid: number; readonly thisPid: number }) {
    super(
      `the core started in process ${facts.corePid}, and this process (${facts.thisPid}) is a fork of it without exec`,
    );
    this.corePid = facts.corePid;
    this.thisPid = facts.thisPid;
  }
}

/** The core refused a manifest that the package built. */
export class ManifestRefused extends CoreDefect {
  override readonly name = "ManifestRefused";
  readonly code = "MANIFEST_REFUSED";
  readonly fieldPath: string;
  override readonly cause: string;

  constructor(facts: { readonly fieldPath: string; readonly cause: string }) {
    super(`the core refused the manifest at ${facts.fieldPath}: ${facts.cause}`);
    this.fieldPath = facts.fieldPath;
    this.cause = facts.cause;
  }
}

/** The panic rule of this process was set to another rule before, or fixed when the core started. */
export class PanicRuleFixed extends CoreDefect {
  override readonly name = "PanicRuleFixed";
  readonly code = "PANIC_RULE_FIXED";
  readonly current: PanicRule;

  constructor(facts: { readonly current: PanicRule }) {
    super(`the panic rule of this process is fixed at ${facts.current}`);
    this.current = facts.current;
  }
}

/** The native bridge panicked in a call. The core runs on. */
export class CorePanic extends CoreDefect {
  override readonly name = "CorePanic";
  readonly code = "CORE_PANIC";
  readonly subject: string;
  override readonly cause: string;

  constructor(facts: { readonly subject: string; readonly cause: string }) {
    super(`panic in core operation ${facts.subject}: ${facts.cause}`);
    this.subject = facts.subject;
    this.cause = facts.cause;
  }
}

/** A `worker_threads` isolate called the native addon, which runs on the main thread only. */
export class AddonOutsideMainThread extends CoreDefect {
  override readonly name = "AddonOutsideMainThread";
  readonly code = "ADDON_OUTSIDE_MAIN_THREAD";
  readonly threadId: number;

  constructor(facts: { readonly threadId: number }) {
    super(`the native addon of horsies runs on the main thread of a process only; thread ${facts.threadId} called it`);
    this.threadId = facts.threadId;
  }
}

/** The package passed a value or made a call that it should have refused first. */
export class ClientDefect extends CoreDefect {
  override readonly name = "ClientDefect";
  readonly code = "CLIENT_DEFECT";
  readonly subject: string;
  readonly rule: string;

  constructor(facts: { readonly subject: string; readonly rule: string }) {
    super(`a defect of the horsies package: ${facts.subject}: ${facts.rule}`);
    this.subject = facts.subject;
    this.rule = facts.rule;
  }
}
