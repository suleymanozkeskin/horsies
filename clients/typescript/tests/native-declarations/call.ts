// The generated declarations of the `call` area match the hand-kept ones.

import type * as Generated from "../../bridge/index.js";
import type { CallAddon, CallbackProbe, NotRunCause, SettleOutcome } from "../../horsies/src/native/call/callback.ts";
import type { NativeEnvelope, NativeThrown } from "../../horsies/src/native/call/envelope.ts";
import type { Mutual } from "./mutual.ts";

export const functionsMatch: Mutual<Pick<typeof Generated, keyof CallAddon>, CallAddon> = true;
export const thrownMatches: Mutual<Generated.NativeThrown, NativeThrown> = true;
export const envelopeMatches: Mutual<Generated.NativeEnvelope<string>, NativeEnvelope<string>> = true;
export const settleOutcomeMatches: Mutual<Generated.SettleOutcome, SettleOutcome> = true;
export const notRunCauseMatches: Mutual<Generated.NotRunCause, NotRunCause> = true;
export const probeMatches: Mutual<Generated.CallbackProbe, CallbackProbe> = true;
