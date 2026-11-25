// The generated declarations of the `values` area match the hand-kept ones.

import type * as Generated from "../../bridge/index.js";
import type {
  CauseRecord,
  ExceptionRecord,
  NativeOutcome,
  RawCatalogError,
  RawRecordValue,
  RawSlotValue,
  TraceArgument,
} from "../../horsies/src/native/values/records.ts";
import type { Mutual } from "./mutual.ts";

export const catalogErrorMatches: Mutual<Generated.RawCatalogError, RawCatalogError> = true;
export const slotValueMatches: Mutual<Generated.RawSlotValue, RawSlotValue> = true;
export const recordValueMatches: Mutual<Generated.RawRecordValue, RawRecordValue> = true;
export const exceptionMatches: Mutual<Generated.ExceptionRecord, ExceptionRecord> = true;
export const causeMatches: Mutual<Generated.CauseRecord, CauseRecord> = true;
export const traceArgumentMatches: Mutual<Generated.TraceArgument, TraceArgument> = true;
export const outcomeMatches: Mutual<Generated.NativeOutcome<number>, NativeOutcome<number>> = true;
