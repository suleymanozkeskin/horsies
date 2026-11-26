// The generated declarations of the `call` area match the hand-kept ones, function by function
// (arguments and result) and type by type.

import type * as Generated from "../../bridge/index.js";
import type { CallAddon, NotRunCause, SettleOutcome } from "../../horsies/src/native/call/callback.ts";
import type { NativeEnvelope, NativeThrown } from "../../horsies/src/native/call/envelope.ts";
import type { CallbackProbe, ProbeAddon } from "../runtime/native/callback-probe.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

export const settleReturnedMatches: SameFunction<
  typeof Generated.settleCallbackReturned,
  CallAddon["settleCallbackReturned"]
> = true;

export const settleThrewMatches: SameFunction<typeof Generated.settleCallbackThrew, CallAddon["settleCallbackThrew"]> =
  true;

export const thrownMatches: Mutual<Generated.NativeThrown, NativeThrown> = true;
export const envelopeMatches: Mutual<Generated.NativeEnvelope<string>, NativeEnvelope<string>> = true;
export const settleOutcomeMatches: Mutual<Generated.SettleOutcome, SettleOutcome> = true;
export const notRunCauseMatches: Mutual<Generated.NotRunCause, NotRunCause> = true;

// The test export of a build with the cargo feature `test-probes` (`build:dev`).
export const probeCallbackMatches: SameFunction<typeof Generated.probeCallback, ProbeAddon["probeCallback"]> = true;
export const probeMatches: Mutual<Generated.CallbackProbe, CallbackProbe> = true;
