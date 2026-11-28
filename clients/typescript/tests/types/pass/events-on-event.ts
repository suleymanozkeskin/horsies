// Compile-pass cases of the event registration: a handler of one event type gets that event's fields, sync or async,
// and the start hook of the handler child is sync or async.

import { Horsies } from "../../../horsies/src/index.ts";
import type { AttemptEnded, EventOf, TaskClaimed } from "../../../horsies/src/index.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

declare const app: Horsies;

export const ended = expectType<Equal<EventOf<"AttemptEnded">, AttemptEnded>>();

export const claimed = expectType<Equal<EventOf<"TaskClaimed">, TaskClaimed>>();

app.onEvent("AttemptEnded", { name: "alerts" }, (event) => {
  if (event.end.kind === "reportedByChild" && event.end.result.kind === "failed") {
    const code: string = event.end.result.errorCode.kind === "customOverBound" ? "over" : event.end.result.errorCode.code;

    void code;
  }
});

app.onEvent("TaskClaimed", { name: "audit" }, async (event) => {
  const queue: string = event.queue;

  await Promise.resolve(queue);
});

app.onHandlerChildStart(() => undefined);

app.onHandlerChildStart(async () => undefined);
