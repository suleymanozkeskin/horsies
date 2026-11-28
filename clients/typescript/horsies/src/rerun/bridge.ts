// The rerun functions of the addon behind one interface: the flows of `rerun.ts` call it, and a unit
// test gives a fake. Each call goes through the call wrapper, so a thrown envelope is its error and a
// throw of the call itself is a defect.

import { nativeValue, nativeValueAsync } from "../native/call/envelope.ts";
import { unreachable } from "../native/call/never.ts";
import { rerunAddon } from "../native/facades/rerun.ts";
import type {
  AppRerunAnswer,
  CallerRowArg,
  CallerStatementRecord,
  CallerStepRefusal,
  PreparedRerunWrite,
  RegisteredTaskCodesArg,
  RerunOutcomeRecord,
  RerunPreparationRecord,
  WriteDecodeRecord,
} from "../native/facades/rerun.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { TraceArgument } from "../native/values/records.ts";
import type { SeatRelayFullOutcome } from "../seats/relay-full.ts";
import type { RerunRequest } from "./parse.ts";

/** The write of one rerun: its statement, and the decode of its row. */
export interface BridgeWrite {
  statement(): CallerStatementRecord;
  decodeRow(row: CallerRowArg): WriteDecodeRecord;
}

/** What the source read's row decides. */
export type BridgePreparation =
  | { readonly kind: "write"; readonly write: BridgeWrite }
  | { readonly kind: "answered"; readonly outcome: RerunOutcomeRecord }
  | { readonly kind: "refused"; readonly error: CallerStepRefusal };

/** The rerun functions of the addon, with each envelope read. */
export interface RerunBridge {
  /** The rerun in a transaction of `handle`; in a seat isolate, `seatRelayFull` when the relay sent nothing. */
  rerunTask(
    handle: CoreHandle,
    request: RerunRequest,
    trace: TraceArgument,
    codes: RegisteredTaskCodesArg,
  ): Promise<AppRerunAnswer | SeatRelayFullOutcome>;
  /** The source read as a caller statement. */
  readStatement(request: RerunRequest): CallerStatementRecord;
  /** What the source read's row decides. */
  prepareWrite(
    request: RerunRequest,
    trace: TraceArgument,
    readRow: CallerRowArg,
    codes: RegisteredTaskCodesArg,
  ): BridgePreparation;
}

/**
 * The rerun functions of the loaded addon. Each one takes the addon before the call wrapper, so a
 * failed load throws its own error (`RuntimeStartFailed`, `AddonOutsideMainThread`), not a defect of the
 * call.
 */
export const addonRerunBridge: RerunBridge = {
  rerunTask: async (
    handle: CoreHandle,
    request: RerunRequest,
    trace: TraceArgument,
    codes: RegisteredTaskCodesArg,
  ): Promise<AppRerunAnswer> => {
    const addon = rerunAddon();

    return nativeValueAsync("rerunTask", () =>
      addon.rerunTask(handle.native, request.command, request.policy, trace, codes),
    );
  },
  readStatement: (request: RerunRequest): CallerStatementRecord => {
    const addon = rerunAddon();

    return nativeValue("rerunReadStatement", () => addon.rerunReadStatement(request.command, request.policy));
  },
  prepareWrite: (
    request: RerunRequest,
    trace: TraceArgument,
    readRow: CallerRowArg,
    codes: RegisteredTaskCodesArg,
  ): BridgePreparation => {
    const addon = rerunAddon();

    return bridgePreparation(
      nativeValue("prepareRerunWrite", () =>
        addon.prepareRerunWrite(request.command, request.policy, trace, readRow, codes),
      ),
    );
  },
};

/** The preparation with its write behind the call wrapper. */
function bridgePreparation(preparation: RerunPreparationRecord): BridgePreparation {
  switch (preparation.kind) {
    case "write":
      return { kind: "write", write: bridgeWrite(preparation.write) };
    case "answered":
    case "refused":
      return preparation;
    default:
      return unreachable(preparation);
  }
}

/** The write of the addon behind the call wrapper. */
function bridgeWrite(write: PreparedRerunWrite): BridgeWrite {
  return {
    statement: (): CallerStatementRecord => nativeValue("PreparedRerunWrite.statement", () => write.statement()),
    decodeRow: (row: CallerRowArg): WriteDecodeRecord =>
      nativeValue("PreparedRerunWrite.decodeRow", () => write.decodeRow(row)),
  };
}
