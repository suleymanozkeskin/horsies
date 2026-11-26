// expect: TS2741 at 10
// A sender gives every member that has no .optional(), .default() or .prefault().

import { z } from "zod";

import type { ZodSentOf } from "../../../horsies/src/zod/infer.ts";

const order = z.object({ id: z.int(), retries: z.int().default(3) });

export const sent: ZodSentOf<typeof order> = { retries: 1 };
