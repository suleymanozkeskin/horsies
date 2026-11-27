// expect: TS2353 at 8
// The policy of `createMonitoringHandler` is `authorizationPolicy`, as in the Python and Rust clients; `auth` is no
// option.

import { createMonitoringHandler, type Horsies, viewOnly } from "../../../horsies/src/index.ts";

export function mount(app: Horsies): unknown {
  return createMonitoringHandler(app, { auth: viewOnly() });
}
