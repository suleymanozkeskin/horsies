// expect: TS2339 at 9
// The schedules are the `schedule` section of the app config, as in the Python and Rust clients: the app has no
// `schedules` member.

import type { Horsies } from "../../../horsies/src/index.ts";

declare const app: Horsies;

app.schedules([]);
