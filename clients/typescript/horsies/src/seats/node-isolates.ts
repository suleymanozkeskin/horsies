// The seat isolates of this process: `worker_threads` isolates of the app entry, each with the seat marker in its
// `workerData` and a cap of its JavaScript heap (`resourceLimits`).
//
// - The entry is the script of this child (`process.argv[1]`, an absolute path: the worker refuses a child command
//   with another entry). An isolate takes the runtime flags of this process (the default of `execArgv`), so it loads
//   the entry with the same loaders as the child.
// - The cap is the heap limit of this child's main isolate (`v8.getHeapStatistics()`): the limit that
//   `--max-old-space-size` on the worker command sets for every child, or V8's own default. An isolate whose
//   JavaScript heap reaches it ends alone with `ERR_WORKER_OUT_OF_MEMORY`, and the child lives. The cap holds for the
//   JavaScript heap only: `ArrayBuffer` and `Buffer` memory is outside it, so a body that keeps buffers grows the whole
//   child (Node has no cap of that memory per isolate). The start facts of a child give no memory value of the worker
//   profile.
// - Each isolate has an `error` listener: an isolate that ends with an error must not throw on this thread. A message
//   of an isolate that does not deserialize goes to the pool as a defect. A `Worker` that the runtime does not start
//   (for example at a thread limit of the OS) is an isolate that did not start, not a throw.
// - The isolates do not keep the child alive: the child ends its process by its exit rule.

import { getHeapStatistics } from "node:v8";
import { type ResourceLimits, Worker } from "node:worker_threads";

import { seatBounds } from "../native/seats/seats.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import type { IsolateStart, SeatHost, StartIsolate } from "./pool.ts";
import { seatWorkerData } from "./protocol.ts";

const BYTES_PER_MEGABYTE = 1024 * 1024;

/** The cap of the JavaScript heap of a seat isolate whose child's main isolate has the heap limit `heapLimitBytes`. */
export function seatResourceLimits(heapLimitBytes: number): ResourceLimits {
  return { maxOldGenerationSizeMb: Math.floor(heapLimitBytes / BYTES_PER_MEGABYTE) };
}

/** The starter of the seat isolates of this process, from the entry `entry`. */
export function workerIsolates(entry: string, limits: ResourceLimits): StartIsolate {
  return (marker, events): IsolateStart => {
    let worker: Worker;

    try {
      worker = new Worker(entry, { workerData: seatWorkerData(marker), resourceLimits: limits });
    } catch (cause) {
      const facts = thrownFacts(cause);

      return { kind: "notStarted", cause: `${facts.name}: ${facts.message}` };
    }

    worker.on("message", (message) => {
      events.message(Object(message));
    });
    worker.on("messageerror", (cause) => {
      events.unreadable(cause);
    });
    worker.on("error", (cause) => {
      events.error(cause);
    });
    worker.on("exit", (exitCode) => {
      events.exit(exitCode);
    });
    // The unref comes after the listeners, because a `message` listener refs the port of the isolate.
    worker.unref();

    return {
      kind: "started",
      isolate: {
        post: (message) => {
          worker.postMessage(message);
        },
      },
    };
  };
}

/** The seat host of this process: its isolates, the seat bounds of the core, its clocks and its timers. */
export function processSeatHost(): SeatHost {
  const entry = process.argv[1];

  return {
    start: (marker, events) =>
      entry === undefined
        ? { kind: "notStarted", cause: "the child runs no entry script" }
        : workerIsolates(entry, seatResourceLimits(getHeapStatistics().heap_size_limit))(marker, events),
    bounds: seatBounds,
    clock: () => performance.now(),
    now: () => Temporal.Now.instant(),
    timer: (milliseconds, callback) => {
      const timer = setTimeout(callback, milliseconds);

      timer.unref();

      return () => {
        clearTimeout(timer);
      };
    },
  };
}
