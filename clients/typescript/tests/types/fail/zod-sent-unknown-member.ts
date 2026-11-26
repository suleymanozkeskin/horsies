// expect: TS2353 at 10
// A sender gives no member that the zod object does not declare.

import { z } from "zod";

import type { ZodSentOf } from "../../../horsies/src/zod/infer.ts";

const order = z.object({ id: z.int() });

export const sent: ZodSentOf<typeof order> = { id: 1, extra: 2 };
