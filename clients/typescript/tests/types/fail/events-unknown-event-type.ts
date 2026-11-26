// expect: TS2345 at 8
// An event type that the event contract does not have.

import { Horsies } from "../../../horsies/src/index.ts";

declare const app: Horsies;

app.onEvent("AttemptFinished", { name: "audit" }, () => undefined);
