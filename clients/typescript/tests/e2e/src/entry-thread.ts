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

/** A start member of the app that marks each of its starts: the provider or the `onChildStart` hook. */
export type StartMember = "provider" | "hook";

/**
 * The marker name of one start of `member` in process `pid`: `<member>-<pid>` on the main thread, and
 * `<member>-<pid>-seat<threadId>` in a seat isolate. A `SHARED_THREAD` child runs the providers once in each of its seat
 * isolates, so one process holds one start of a member per thread. Pure.
 */
export function startMarkerName(member: StartMember, pid: number, thread: EntryThread, threadId: number): string {
  switch (thread) {
    case "main":
      return `${member}-${String(pid)}`;
    case "seatIsolate":
      return `${member}-${String(pid)}-seat${String(threadId)}`;
    default: {
      const unexpected: never = thread;

      throw new TypeError(`the start marker: a thread outside its type: ${String(unexpected)}`);
    }
  }
}
