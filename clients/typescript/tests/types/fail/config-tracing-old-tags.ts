// expect: TS2322 at 7, TS2322 at 10
// The tracing setting names its kind as `otelTracing` or `noTracing`; `otel` and `none` are no tracing kinds.

import type { TracingConfigInput } from "../../../horsies/src/index.ts";

export const traced: TracingConfigInput = {
  kind: "otel",
};

export const untraced: TracingConfigInput = { kind: "none" };
