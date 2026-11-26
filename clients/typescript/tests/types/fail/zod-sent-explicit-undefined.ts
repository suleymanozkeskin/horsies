// expect: TS2375 at 10
// A sender leaves an optional member out; an explicit undefined is refused.

import { z } from "zod";

import type { ZodSentOf } from "../../../horsies/src/zod/infer.ts";

const order = z.object({ id: z.int(), note: z.string().optional() });

export const sent: ZodSentOf<typeof order> = { id: 1, note: undefined };
