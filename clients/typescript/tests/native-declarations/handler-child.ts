// The generated declarations of the `handler_child` area match the hand-kept ones, function by function (arguments
// and result), and type by type.

import type * as Generated from "../../bridge/index.js";
import type {
  EndedAttemptRecord,
  EndedSpansMode,
  EndedWorkflowRecord,
  HandlerChildAddon,
  HandlerChildPlan,
  HandlerEntry,
  HandlerEventAnswer,
  HandlerEventCall,
} from "../../horsies/src/native/handler-child/addon.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

export const fillMatches: SameFunction<typeof Generated.fillHandlerChild, HandlerChildAddon["fillHandlerChild"]> = true;

export const entryMatches: Mutual<Generated.HandlerEntry, HandlerEntry> = true;
export const modeMatches: Mutual<Generated.EndedSpansMode, EndedSpansMode> = true;
export const planMatches: Mutual<Generated.HandlerChildPlan, HandlerChildPlan> = true;
export const callMatches: Mutual<Generated.HandlerEventCall, HandlerEventCall> = true;
export const answerMatches: Mutual<Generated.HandlerEventAnswer, HandlerEventAnswer> = true;
export const workflowMatches: Mutual<Generated.EndedWorkflowRecord, EndedWorkflowRecord> = true;
export const attemptMatches: Mutual<Generated.EndedAttemptRecord, EndedAttemptRecord> = true;
