// The run store of a child: one `AsyncLocalStorage`. The body adapter runs each body call, and the start of each
// spawned work, inside it with the run's mark, so a callback that the body scheduled still knows its run: a throw in
// such a callback is logged with the run (the process events), and on Node the culprit marks of the loop watch read
// the run of each continuation after an `await`.

import { AsyncLocalStorage } from "node:async_hooks";

/** The run whose code runs now. `attempt` is the fencing token's value, from 1. */
export type RunMark = {
  readonly taskId: string;
  readonly taskName: string;
  readonly attempt: number;
};

/** The run store of this thread. */
export const runStore: AsyncLocalStorage<RunMark> = new AsyncLocalStorage<RunMark>();

/** The run whose code runs now, or `noRun` outside every body call and spawned work. */
export type CurrentRun = { readonly kind: "run"; readonly mark: RunMark } | { readonly kind: "noRun" };

/** The run of the code that runs now. Reads the store. */
export function currentRun(): CurrentRun {
  const mark = runStore.getStore();

  return mark === undefined ? { kind: "noRun" } : { kind: "run", mark };
}
