// The store of the attempt span ids of the body that runs in the current async context: the attempt-span observer
// sets it around the body call; `currentTask()` reads it. A module of its own, so the published declarations of the
// log context name no Node type.

import { AsyncLocalStorage } from "node:async_hooks";

import type { SpanIds } from "./log-context.ts";

/** The span ids of the attempt span whose body runs now. */
export const attemptSpanStore: AsyncLocalStorage<SpanIds> = new AsyncLocalStorage<SpanIds>();
