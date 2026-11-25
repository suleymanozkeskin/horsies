// The generated declarations of the `call` area match the hand-kept ones.

import type * as Generated from "../../bridge/index.js";
import type { CallAddon, NotRunCause, SettleOutcome } from "../../horsies/src/native/call/callback.ts";
import type { NativeEnvelope, NativeThrown } from "../../horsies/src/native/call/envelope.ts";
import type { CallbackProbe, ProbeAddon } from "../runtime/native/callback-probe.ts";
import type { Mutual } from "./mutual.ts";

export const functionsMatch: Mutual<Pick<typeof Generated, keyof CallAddon>, CallAddon> = true;
export const thrownMatches: Mutual<Generated.NativeThrown, NativeThrown> = true;
export const envelopeMatches: Mutual<Generated.NativeEnvelope<string>, NativeEnvelope<string>> = true;
export const settleOutcomeMatches: Mutual<Generated.SettleOutcome, SettleOutcome> = true;
export const notRunCauseMatches: Mutual<Generated.NotRunCause, NotRunCause> = true;

// The test export of a build with the cargo feature `test-probes` (`build:dev`).
export const probeFunctionsMatch: Mutual<Pick<typeof Generated, keyof ProbeAddon>, ProbeAddon> = true;
export const probeMatches: Mutual<Generated.CallbackProbe, CallbackProbe> = true;
