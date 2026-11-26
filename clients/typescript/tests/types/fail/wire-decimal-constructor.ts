// expect: TS2673 at 6
// A Decimal comes from Decimal.parse only, which returns a Result.

import { Decimal } from "../../../horsies/src/index.ts";

export const total = new Decimal("19.90");
