// The bounds of the monitoring queries that the Python and Rust clients export by these names. The values come from
// the core through the generated `monitoring_bounds.ts`.

import { MONITORING_WINDOW_DEFAULT_SECONDS } from "../_catalog/monitoring_bounds.ts";

/** The history window of a read whose window names no bound: the 24 hours before now (the core's default). */
export const MONITORING_WINDOW_DEFAULT: Temporal.Duration = Temporal.Duration.from({
  seconds: MONITORING_WINDOW_DEFAULT_SECONDS,
});
