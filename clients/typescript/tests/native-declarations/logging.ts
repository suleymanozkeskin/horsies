// The generated declarations of the `logging` area match the hand-kept ones.

import type * as Generated from "../../bridge/index.js";
import type { LoggerName, LogLevel, LogThreshold } from "../../horsies/src/logging.ts";
import type { LogLevelsArg, LogRecordOut, LoggingAddon } from "../../horsies/src/native/logging/addon.ts";
import type { Mutual } from "./mutual.ts";

export const functionsMatch: Mutual<Pick<typeof Generated, keyof LoggingAddon>, LoggingAddon> = true;
export const loggerNameMatches: Mutual<Generated.LoggerName, LoggerName> = true;
export const levelMatches: Mutual<Generated.RecordLevel, LogLevel> = true;
export const thresholdMatches: Mutual<Generated.LogThreshold, LogThreshold> = true;
export const recordMatches: Mutual<Generated.LogRecordOut, LogRecordOut> = true;
export const levelsArgumentMatches: Mutual<Generated.LogLevelsArg, LogLevelsArg> = true;
