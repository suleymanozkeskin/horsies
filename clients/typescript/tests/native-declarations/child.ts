// The generated declarations of the `child` area match the hand-kept ones, function by function (arguments and
// result), and type by type.

import type * as Generated from "../../bridge/index.js";
import type {
  BodyCarrierRecord,
  BodyEndRecord,
  BodyExecution,
  BodyFactsMode,
  BodyFactsRecord,
  BodyForm,
  BodySettle,
  BodyWorkflowRecord,
  CancelCauseName,
  CarrierEntry,
  ChildAddon,
  ChildArgumentsCheck,
  ChildExitName,
  ChildHostPlan,
  ChildPost,
  ChildServeEnd,
  ChildTaskEntry,
  MemoryLimitRecord,
  RunCancelStateRecord,
  SpawnEnd,
  SpawnRegistration,
  StartFactsRecord,
  StartHookCall,
  StartHookEnd,
  TaskContextUse,
  WorkStopCauseName,
} from "../../horsies/src/native/child/addon.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

export const childArgumentsMatches: SameFunction<typeof Generated.childArguments, ChildAddon["childArguments"]> = true;
export const serveChildMatches: SameFunction<typeof Generated.serveChild, ChildAddon["serveChild"]> = true;
export const settleValueMatches: SameFunction<typeof Generated.settleBodyValue, ChildAddon["settleBodyValue"]> = true;
export const settleErrorMatches: SameFunction<typeof Generated.settleBodyError, ChildAddon["settleBodyError"]> = true;
export const cancelStateMatches: SameFunction<typeof Generated.runCancelStateOf, ChildAddon["runCancelStateOf"]> = true;
export const registerSpawnMatches: SameFunction<typeof Generated.registerSpawn, ChildAddon["registerSpawn"]> = true;
export const endSpawnMatches: SameFunction<typeof Generated.endSpawn, ChildAddon["endSpawn"]> = true;
export const loopTurnedMatches: SameFunction<typeof Generated.loopTurned, ChildAddon["loopTurned"]> = true;
export const loopHoldMatches: SameFunction<typeof Generated.loopHold, ChildAddon["loopHold"]> = true;
export const loopReleaseMatches: SameFunction<typeof Generated.loopRelease, ChildAddon["loopRelease"]> = true;
export const endAtMemoryLimitMatches: SameFunction<typeof Generated.endAtMemoryLimit, ChildAddon["endAtMemoryLimit"]> = true;

export const cancelCauseMatches: Mutual<Generated.CancelCauseName, CancelCauseName> = true;
export const stopCauseMatches: Mutual<Generated.WorkStopCauseName, WorkStopCauseName> = true;
export const executionMatches: Mutual<Generated.BodyExecution, BodyExecution> = true;
export const workflowMatches: Mutual<Generated.BodyWorkflowRecord, BodyWorkflowRecord> = true;
export const carrierEntryMatches: Mutual<Generated.CarrierEntry, CarrierEntry> = true;
export const carrierMatches: Mutual<Generated.BodyCarrierRecord, BodyCarrierRecord> = true;
export const factsMatches: Mutual<Generated.BodyFactsRecord, BodyFactsRecord> = true;
export const postMatches: Mutual<Generated.ChildPost, ChildPost> = true;
export const bodyEndMatches: Mutual<Generated.BodyEndRecord, BodyEndRecord> = true;
export const memoryLimitMatches: Mutual<Generated.MemoryLimitRecord, MemoryLimitRecord> = true;
export const settleMatches: Mutual<Generated.BodySettle, BodySettle> = true;
export const cancelStateRecordMatches: Mutual<Generated.RunCancelStateRecord, RunCancelStateRecord> = true;
export const registrationMatches: Mutual<Generated.SpawnRegistration, SpawnRegistration> = true;
export const spawnEndMatches: Mutual<Generated.SpawnEnd, SpawnEnd> = true;
export const formMatches: Mutual<Generated.BodyForm, BodyForm> = true;
export const factsModeMatches: Mutual<Generated.BodyFactsMode, BodyFactsMode> = true;
export const contextUseMatches: Mutual<Generated.TaskContextUse, TaskContextUse> = true;
export const taskEntryMatches: Mutual<Generated.ChildTaskEntry, ChildTaskEntry> = true;
export const planMatches: Mutual<Generated.ChildHostPlan, ChildHostPlan> = true;
export const argumentsMatches: Mutual<Generated.ChildArgumentsCheck, ChildArgumentsCheck> = true;
export const exitNameMatches: Mutual<Generated.ChildExitName, ChildExitName> = true;
export const serveEndMatches: Mutual<Generated.ChildServeEnd, ChildServeEnd> = true;
export const startFactsMatches: Mutual<Generated.StartFactsRecord, StartFactsRecord> = true;
export const hookCallMatches: Mutual<Generated.StartHookCall, StartHookCall> = true;
export const hookEndMatches: Mutual<Generated.StartHookEnd, StartHookEnd> = true;
