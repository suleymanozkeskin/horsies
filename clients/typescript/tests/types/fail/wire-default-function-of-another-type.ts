// expect: TS2345 at 6
// A default function gives a value of the sent type of its wire type.

import { h } from "../../../horsies/src/index.ts";

export const retries = h.int().default(() => "3");
