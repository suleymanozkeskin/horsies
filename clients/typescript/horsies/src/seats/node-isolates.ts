// The seat isolates of this process: `worker_threads` isolates of the app entry, each with the seat marker in its
// `workerData` and a memory cap (`resourceLimits`).
//
// - The entry is the script of this child (`process.argv[1]`, an absolute path: the worker refuses a child command
//   with another entry). An isolate takes the runtime flags of this process (the default of `execArgv`), so it loads
//   the entry with the same loaders as the child.
// - The memory cap of each isolate is the heap limit of this child's main isolate (`v8.getHeapStatistics()`): the
//   limit that `--max-old-space-size` on the worker command sets for every child, or V8's own default. An isolate past
//   it ends alone with `ERR_WORKER_OUT_OF_MEMORY`, and the child lives. The start facts of a child give no memory
//   value of the worker profile.
// - Each isolate has an `error` listener: an isolate that ends with an error must not throw on this thread. A message
//   of an isolate that does not deserialize is a defect: its throw ends the child.

import { getHeapStatistics } from "node:v8";
import { type ResourceLimits, Worker } from "node:worker_threads";

import { ClientDefect } from "../thrown-errors.ts";
import type { SeatHost, StartIsolate } from "./pool.ts";
import { seatWorkerData } from "./protocol.ts";

const BYTES_PER_MEGABYTE = 1024 * 1024;

/** The memory cap of a seat isolate whose child's main isolate has the heap limit `heapLimitBytes`. Pure. */
export function seatResourceLimits(heapLimitBytes: number): ResourceLimits {
  return { maxOldGenerationSizeMb: Math.floor(heapLimitBytes / BYTES_PER_MEGABYTE) };
}

/** The starter of the seat isolates of this process, from the entry `entry`. */
export function workerIsolates(entry: string, limits: ResourceLimits): StartIsolate {
  return (marker, events) => {
    const worker = new Worker(entry, { workerData: seatWorkerData(marker), resourceLimits: limits });

    worker.on("message", (message) => {
      events.message(Object(message));
    });
    worker.on("messageerror", (cause) => {
      // A message that does not deserialize is a defect of the package: the throw ends the child.
      throw new ClientDefect({ subject: "seat isolates", rule: `a message of a seat isolate does not deserialize: ${String(cause)}` });
    });
    worker.on("error", (cause) => {
      events.error(cause);
    });
    worker.on("exit", (exitCode) => {
      events.exit(exitCode);
    });
    // The isolates do not keep the child alive: the child ends its process by its exit rule. The unref comes after the
    // listeners, because a `message` listener refs the port of the isolate.
    worker.unref();

    return {
      post: (message) => {
        worker.postMessage(message);
      },
    };
  };
}

/** The seat host of this process: its isolates, its monotonic clock, and its timers. Reads `process.argv`. */
export function processSeatHost(): SeatHost {
  const entry = process.argv[1];

  return {
    start: (marker, events) => {
      if (entry === undefined) {
        throw new ClientDefect({ subject: "seat isolates", rule: "a child of the worker runs an entry script" });
      }

      return workerIsolates(entry, seatResourceLimits(getHeapStatistics().heap_size_limit))(marker, events);
    },
    clock: () => performance.now(),
    timer: (milliseconds, callback) => {
      const timer = setTimeout(callback, milliseconds);

      timer.unref();

      return () => {
        clearTimeout(timer);
      };
    },
  };
}
