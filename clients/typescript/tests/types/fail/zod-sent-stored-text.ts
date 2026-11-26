// expect: TS2322 at 11
// A sender gives the value of a codec, not its stored text.

import { z } from "zod";

import { hz } from "../../../horsies/src/zod.ts";
import type { ZodSentOf } from "../../../horsies/src/zod/infer.ts";

const event = z.object({ at: hz.instant() });

export const sent: ZodSentOf<typeof event> = { at: "2026-08-12T09:10:12Z" };
