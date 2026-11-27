// expect: TS2322 at 6, TS2322 at 8
// The schema mode is one of the three manifest texts, and the log format names its kind as `textLogs` or `jsonLogs`.

import type { LogFormat, SchemaMode } from "../../../horsies/src/index.ts";

export const mode: SchemaMode = "MIGRATE";

export const format: LogFormat = { kind: "json" };
