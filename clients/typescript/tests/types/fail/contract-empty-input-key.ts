// expect: TS2353 at 6
// A task with no parameters (`input: {}`) takes only `{}`.

import type { SendInput } from "../../../horsies/src/contract/public.ts";

export const sent: SendInput<{}> = { orderId: 1 };
