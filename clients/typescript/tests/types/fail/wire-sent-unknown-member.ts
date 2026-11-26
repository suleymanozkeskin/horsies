// expect: TS2353 at 9
// A sender gives no member that the type does not declare.

import { h } from "../../../horsies/src/index.ts";
import type { SentOf } from "../../../horsies/src/wire/infer.ts";

const order = h.object({ id: h.int() });

export const sent: SentOf<typeof order> = { id: 1, extra: 2 };
