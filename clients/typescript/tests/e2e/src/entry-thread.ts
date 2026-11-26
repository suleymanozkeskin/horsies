// The thread rule of the worker entry (`main.ts`).
//
// The worker starts each child with the entry, and a seat isolate of a `SHARED_THREAD` child runs the entry again on a
// worker thread of that child. No addon loads on a worker thread, and the `process.argv` of a worker thread holds no
// argument after the script. So the steps that run once per process (the entry marker and the load of the dev build of
// the addon) run on the main thread of each process only, as in the entry of the parity runs.

/** The thread that runs the entry: the main thread of a process, or a seat isolate (a worker thread). */
export type EntryThread = "main" | "seatIsolate";

/** Whether the steps of the process ran on this thread. */
export type ProcessSteps = "ran" | "skipped";

/** The thread of `isMainThread` (`node:worker_threads`). Pure. */
export function entryThread(isMainThread: boolean): EntryThread {
  return isMainThread ? "main" : "seatIsolate";
}

/** Run `steps` when `thread` is the main thread of the process; a seat isolate runs none of them. */
export function runProcessSteps(thread: EntryThread, steps: () => void): ProcessSteps {
  switch (thread) {
    case "main":
      steps();

      return "ran";
    case "seatIsolate":
      return "skipped";
    default: {
      const unexpected: never = thread;

      throw new TypeError(`the entry: a thread outside its type: ${String(unexpected)}`);
    }
  }
}
