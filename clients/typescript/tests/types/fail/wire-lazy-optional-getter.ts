// expect: TS2322 at 6
// The getter of h.lazy returns a wire type that is always present: .optional() is for the member that holds it.

import { h } from "../../../horsies/src/index.ts";

export const item = h.lazy(() => h.int().optional());
