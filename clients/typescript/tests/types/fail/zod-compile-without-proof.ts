// expect: TS2554 at 8
// The adapter compiles only after a passed self-check: it takes the proof that only the self-check makes.

import { z } from "zod";

import { compileMember } from "../../../horsies/src/zod/adapter.ts";

export const compiled = compileMember(z.string());
