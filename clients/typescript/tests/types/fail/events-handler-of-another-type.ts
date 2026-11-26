// expect: TS2339 at 9
// A handler of `TaskClaimed` events reads a field of `AttemptEnded`: the event type of the handler is the one named.

import { Horsies } from "../../../horsies/src/index.ts";

declare const app: Horsies;

app.onEvent("TaskClaimed", { name: "audit" }, (event) => {
  void event.end;
});
