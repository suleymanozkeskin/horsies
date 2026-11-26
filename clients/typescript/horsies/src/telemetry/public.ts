// The public names of the telemetry types. The export line of the main entry point names this module.

export type { AttemptParentage, TracingConfigInput, WorkerEndedSpans } from "./tracing-config.ts";

export {
  currentTask,
  type CurrentTask,
  type NoRunningTask,
  type NoSpanIds,
  type RunningTask,
  type SpanIds,
} from "./log-context.ts";
