// expect: TS2741 at 9
// A sender gives every required member.

import { h } from "../../../horsies/src/index.ts";
import type { SentOf } from "../../../horsies/src/wire/infer.ts";

const order = h.object({ id: h.int(), note: h.string().optional() });

export const sent: SentOf<typeof order> = { note: "x" };
