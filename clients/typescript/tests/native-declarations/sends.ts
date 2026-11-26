// The generated declarations of the `sends` area match the hand-kept ones, function by function and
// method by method (arguments and result), and type by type.

import type * as Generated from "../../bridge/index.js";
import type {
  AbsenceReason,
  AppConfigCheck,
  AttemptOutcomeName,
  CoreRefusal,
  CoreRefusalCode,
  DeadlineFact,
  DeclaredCodesCheck,
  DeclaredCodesFact,
  DelayFact,
  EnqueueOutcome,
  EnqueueRequest,
  EnvelopeFact,
  ErrorCodePart,
  ExceptionPart,
  ExecutionName,
  ExecutionRecord,
  MaxDelayFact,
  MillisecondsFact,
  NativeSendRules,
  NativeTaskError,
  NativeTaskOptions,
  OptionalText,
  PreparedSend,
  ProcessRoleName,
  RecordedCount,
  RecordedInstant,
  RecordedText,
  ResultRead,
  ResultRecord,
  RetentionChoice,
  RetentionRecord,
  RetryFacts,
  SendFacts,
  SendKind,
  SendsAddon,
  TaskAttemptRecord,
  TaskErrorBuild,
  TaskErrorParts,
  TaskInfoRead,
  TaskInfoRecord,
  TaskOptionsCheck,
  TaskOptionsFacts,
  TaskStatusName,
  WaitBound,
} from "../../horsies/src/native/sends/addon.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

export const checkAppConfigMatches: SameFunction<typeof Generated.checkAppConfig, SendsAddon["checkAppConfig"]> =
  true;

export const parseDeclaredErrorCodesMatches: SameFunction<
  typeof Generated.parseDeclaredErrorCodes,
  SendsAddon["parseDeclaredErrorCodes"]
> = true;

export const checkTaskOptionsMatches: SameFunction<
  typeof Generated.checkTaskOptions,
  SendsAddon["checkTaskOptions"]
> = true;

export const enqueueMatches: SameFunction<typeof Generated.enqueue, SendsAddon["enqueue"]> = true;

export const getResultMatches: SameFunction<typeof Generated.getResult, SendsAddon["getResult"]> = true;
export const taskInfoMatches: SameFunction<typeof Generated.taskInfo, SendsAddon["taskInfo"]> = true;
export const processRoleMatches: SameFunction<typeof Generated.processRole, SendsAddon["processRole"]> = true;

export const buildTaskErrorMatches: SameFunction<typeof Generated.buildTaskError, SendsAddon["buildTaskError"]> =
  true;

// The classes, method by method, and their member sets: no method on one side only.
export const prepareSendFactsMatches: SameFunction<
  Generated.SendRules["prepareSendFacts"],
  NativeSendRules["prepareSendFacts"]
> = true;

export const sendRulesMatches: Mutual<Generated.SendRules, NativeSendRules> = true;

export const optionsTextMatches: SameFunction<
  Generated.TaskOptionsRecord["storedText"],
  NativeTaskOptions["storedText"]
> = true;

export const taskOptionsMatches: Mutual<Generated.TaskOptionsRecord, NativeTaskOptions> = true;

export const errorTextMatches: SameFunction<Generated.TaskErrorRecord["storedText"], NativeTaskError["storedText"]> =
  true;

export const taskErrorMatches: Mutual<Generated.TaskErrorRecord, NativeTaskError> = true;

export const refusalCodeMatches: Mutual<Generated.CoreRefusalCode, CoreRefusalCode> = true;
export const refusalMatches: Mutual<Generated.CoreRefusal, CoreRefusal> = true;
export const appConfigCheckMatches: Mutual<Generated.AppConfigCheck, AppConfigCheck> = true;
export const declaredCodesCheckMatches: Mutual<Generated.DeclaredCodesCheck, DeclaredCodesCheck> = true;
export const executionNameMatches: Mutual<Generated.ExecutionName, ExecutionName> = true;
export const millisecondsMatches: Mutual<Generated.MillisecondsFact, MillisecondsFact> = true;
export const maxDelayMatches: Mutual<Generated.MaxDelayFact, MaxDelayFact> = true;
export const retryMatches: Mutual<Generated.RetryFacts, RetryFacts> = true;
export const declaredCodesMatches: Mutual<Generated.DeclaredCodesFact, DeclaredCodesFact> = true;
export const taskOptionsFactsMatches: Mutual<Generated.TaskOptionsFacts, TaskOptionsFacts> = true;
export const taskOptionsCheckMatches: Mutual<Generated.TaskOptionsCheck, TaskOptionsCheck> = true;
export const retentionChoiceMatches: Mutual<Generated.RetentionChoice, RetentionChoice> = true;
export const retentionRecordMatches: Mutual<Generated.RetentionRecord, RetentionRecord> = true;
export const deadlineMatches: Mutual<Generated.DeadlineFact, DeadlineFact> = true;
export const sendFactsMatches: Mutual<Generated.SendFacts, SendFacts> = true;
export const preparedSendMatches: Mutual<Generated.PreparedSend, PreparedSend> = true;
export const delayMatches: Mutual<Generated.DelayFact, DelayFact> = true;
export const optionalTextMatches: Mutual<Generated.OptionalText, OptionalText> = true;
export const enqueueRequestMatches: Mutual<Generated.EnqueueRequest, EnqueueRequest> = true;
export const sendKindMatches: Mutual<Generated.SendKind, SendKind> = true;
export const enqueueOutcomeMatches: Mutual<Generated.EnqueueOutcome, EnqueueOutcome> = true;
export const waitBoundMatches: Mutual<Generated.WaitBound, WaitBound> = true;
export const statusMatches: Mutual<Generated.TaskStatusName, TaskStatusName> = true;
export const absenceMatches: Mutual<Generated.AbsenceReason, AbsenceReason> = true;
export const envelopeMatches: Mutual<Generated.EnvelopeFact, EnvelopeFact> = true;
export const resultRecordMatches: Mutual<Generated.ResultRecord, ResultRecord> = true;
export const resultReadMatches: Mutual<Generated.ResultRead, ResultRead> = true;
export const recordedTextMatches: Mutual<Generated.RecordedText, RecordedText> = true;
export const recordedInstantMatches: Mutual<Generated.RecordedInstant, RecordedInstant> = true;
export const recordedCountMatches: Mutual<Generated.RecordedCount, RecordedCount> = true;
export const executionRecordMatches: Mutual<Generated.ExecutionRecord, ExecutionRecord> = true;
export const attemptOutcomeMatches: Mutual<Generated.AttemptOutcomeName, AttemptOutcomeName> = true;
export const attemptMatches: Mutual<Generated.TaskAttemptRecord, TaskAttemptRecord> = true;
export const infoRecordMatches: Mutual<Generated.TaskInfoRecord, TaskInfoRecord> = true;
export const infoReadMatches: Mutual<Generated.TaskInfoRead, TaskInfoRead> = true;
export const roleMatches: Mutual<Generated.ProcessRoleName, ProcessRoleName> = true;
export const errorCodeMatches: Mutual<Generated.ErrorCodePart, ErrorCodePart> = true;
export const exceptionMatches: Mutual<Generated.ExceptionPart, ExceptionPart> = true;
export const errorPartsMatches: Mutual<Generated.TaskErrorParts, TaskErrorParts> = true;
export const errorBuildMatches: Mutual<Generated.TaskErrorBuild, TaskErrorBuild> = true;
