// expect: TS2741 at 11
// A sender gives each zod parameter that has no .optional(), .default() or .prefault().

import { z } from "zod";

import { h } from "../../../horsies/src/index.ts";
import type { ParametersSent } from "../../../horsies/src/zod/infer.ts";

const input = { orderId: h.uuid(), order: z.object({ sku: z.string() }) };

export const sent: ParametersSent<typeof input> = { orderId: "00000000-0000-4000-8000-000000000000" };
