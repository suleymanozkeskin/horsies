// The generated declarations of the `logging` area match the hand-kept ones, function by function
// (arguments and result) and type by type.

import type * as Generated from "../../bridge/index.js";
import type { LoggerName, LogLevel, LogThreshold } from "../../horsies/src/logging.ts";
import type { LogLevelsArg, LogRecordOut, LoggingAddon, NotMadeSpan } from "../../horsies/src/native/logging/addon.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

export const setLogLevelsMatches: SameFunction<typeof Generated.setLogLevels, LoggingAddon["setLogLevels"]> = true;
export const setLogSinkMatches: SameFunction<typeof Generated.setLogSink, LoggingAddon["setLogSink"]> = true;
export const resetLogSinkMatches: SameFunction<typeof Generated.resetLogSink, LoggingAddon["resetLogSink"]> = true;
export const logSpanNotMadeMatches: SameFunction<typeof Generated.logSpanNotMade, LoggingAddon["logSpanNotMade"]> = true;

export const loggerNameMatches: Mutual<Generated.LoggerName, LoggerName> = true;
export const levelMatches: Mutual<Generated.RecordLevel, LogLevel> = true;
export const thresholdMatches: Mutual<Generated.LogThreshold, LogThreshold> = true;
export const recordMatches: Mutual<Generated.LogRecordOut, LogRecordOut> = true;
export const levelsArgumentMatches: Mutual<Generated.LogLevelsArg, LogLevelsArg> = true;
export const notMadeSpanMatches: Mutual<Generated.NotMadeSpan, NotMadeSpan> = true;
