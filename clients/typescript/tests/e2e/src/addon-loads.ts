// The addon loads of a seat isolate: each `process.dlopen` call of the thread, counted from the first line of the
// worker entry. `process.dlopen` opens every `.node` file (`require` of a `.node` file calls it, and so does the
// package loader), so a count of 0 in a thread body says that nothing in the isolate opened an addon: not the package,
// not the app, not the relay of its sends.
//
// The worker entry imports this module before any other module (`main.ts`), so the count covers every module of the
// entry. The main thread of a process is not counted: it loads the dev build of the addon once (`entry-thread.ts`).

import { isMainThread } from "node:worker_threads";

import { entryThread } from "./entry-thread.ts";

/** The addon loads of this thread: counted in a seat isolate; the main thread has no count. */
export type AddonLoads = { readonly kind: "counted"; readonly loads: number } | { readonly kind: "mainThread" };

/** The `process.dlopen` calls of this seat isolate since the entry started. */
let seatLoads = 0;

/** Count each `process.dlopen` call of this thread, then make the call with the same arguments. */
function countLoads(): void {
  const open = process.dlopen.bind(process);

  process.dlopen = (...call: Parameters<typeof process.dlopen>): void => {
    seatLoads += 1;
    open(...call);
  };
}

switch (entryThread(isMainThread)) {
  case "seatIsolate":
    countLoads();
    break;
  case "main":
    break;
}

/** The addon loads of this thread since the entry started. */
export function addonLoads(): AddonLoads {
  switch (entryThread(isMainThread)) {
    case "seatIsolate":
      return { kind: "counted", loads: seatLoads };
    case "main":
      return { kind: "mainThread" };
  }
}
