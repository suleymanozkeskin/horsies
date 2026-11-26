// The generated declarations of the rerun functions of the `facades` area match the hand-kept ones,
// function by function and method by method (arguments and result), and type by type.

import type * as Generated from "../../bridge/index.js";
import type {
  AbsenceReasonName,
  AppRerunAnswer,
  AppRerunFailure,
  CallerColumnRecord,
  CallerKeyArg,
  CallerRowArg,
  CallerStatementRecord,
  CallerStepRefusal,
  CallerTypeName,
  DeadlineArg,
  FamilyName,
  NotEligibleName,
  PreparedRerunWrite,
  RegisteredTaskArg,
  RegisteredTaskCodesArg,
  RerunAddon,
  RerunCommandArg,
  RerunInputArg,
  RerunOutcomeRecord,
  RerunPolicyArg,
  RerunPreparationRecord,
  RetentionArg,
  SqlTextValue,
  StatementOutcome,
  StoredFamilyRecord,
  WriteDecodeRecord,
} from "../../horsies/src/native/facades/rerun.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

type Write = Generated.PreparedRerunWrite;

export const rerunTaskMatches: SameFunction<typeof Generated.rerunTask, RerunAddon["rerunTask"]> = true;

export const rerunReadStatementMatches: SameFunction<
  typeof Generated.rerunReadStatement,
  RerunAddon["rerunReadStatement"]
> = true;

export const prepareRerunWriteMatches: SameFunction<
  typeof Generated.prepareRerunWrite,
  RerunAddon["prepareRerunWrite"]
> = true;

export const statementMatches: SameFunction<Write["statement"], PreparedRerunWrite["statement"]> = true;
export const decodeRowMatches: SameFunction<Write["decodeRow"], PreparedRerunWrite["decodeRow"]> = true;

// The member sets: no method on one side only.
export const writeMatches: Mutual<Write, PreparedRerunWrite> = true;

export const commandMatches: Mutual<Generated.RerunCommandArg, RerunCommandArg> = true;
export const deadlineMatches: Mutual<Generated.DeadlineArg, DeadlineArg> = true;
export const callerKeyMatches: Mutual<Generated.CallerKeyArg, CallerKeyArg> = true;
export const registeredTaskMatches: Mutual<Generated.RegisteredTaskArg, RegisteredTaskArg> = true;
export const registeredTaskCodesMatches: Mutual<Generated.RegisteredTaskCodesArg, RegisteredTaskCodesArg> = true;
export const policyMatches: Mutual<Generated.RerunPolicyArg, RerunPolicyArg> = true;
export const retentionMatches: Mutual<Generated.RetentionArg, RetentionArg> = true;
export const rerunInputMatches: Mutual<Generated.RerunInputArg, RerunInputArg> = true;
export const sqlTextMatches: Mutual<Generated.SqlTextValue, SqlTextValue> = true;
export const callerTypeMatches: Mutual<Generated.CallerTypeName, CallerTypeName> = true;
export const columnMatches: Mutual<Generated.CallerColumnRecord, CallerColumnRecord> = true;
export const callerStatementMatches: Mutual<Generated.CallerStatementRecord, CallerStatementRecord> = true;
export const callerRowMatches: Mutual<Generated.CallerRowArg, CallerRowArg> = true;
export const absenceReasonMatches: Mutual<Generated.AbsenceReasonName, AbsenceReasonName> = true;
export const notEligibleMatches: Mutual<Generated.NotEligibleName, NotEligibleName> = true;
export const outcomeMatches: Mutual<Generated.RerunOutcomeRecord, RerunOutcomeRecord> = true;
export const familyMatches: Mutual<Generated.FamilyName, FamilyName> = true;
export const storedFamilyMatches: Mutual<Generated.StoredFamilyRecord, StoredFamilyRecord> = true;
export const statementOutcomeMatches: Mutual<Generated.StatementOutcome, StatementOutcome> = true;
export const appFailureMatches: Mutual<Generated.AppRerunFailure, AppRerunFailure> = true;
export const appAnswerMatches: Mutual<Generated.AppRerunAnswer, AppRerunAnswer> = true;
export const stepRefusalMatches: Mutual<Generated.CallerStepRefusal, CallerStepRefusal> = true;
export const writeDecodeMatches: Mutual<Generated.WriteDecodeRecord, WriteDecodeRecord> = true;
export const preparationMatches: Mutual<Generated.RerunPreparationRecord, RerunPreparationRecord> = true;
