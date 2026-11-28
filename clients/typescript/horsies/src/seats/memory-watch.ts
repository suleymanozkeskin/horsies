// The in-run memory watch of a `SHARED_THREAD` child, on its main thread.
//
// - The bound is `recycleAbove` of the start facts (`recycle.above` of the profile). With `{ kind: "off" }` there is no
//   watch: no timer starts and the memory is never read.
// - With `{ kind: "atMb", mb }`, every `MEMORY_WATCH_INTERVAL_MS` the watch asks the seat pool whether the child holds a
//   run (a run on a seat, a settled run whose core end has not come, or a run that waits for a seat). Only then it
//   reads the resident set size of the process: the measure that the worker reads for the bound of the other
//   execution classes, in MiB (2^20 bytes). The seat isolates are threads of this process, so the read counts their
//   `ArrayBuffer` and `Buffer` memory too.
// - At or above the bound (the comparison of the worker: `measured >= bound`), the watch ends the child through the
//   addon (`endAtMemoryLimit`): one stderr line with the bound and the measured value, then exit code 81
//   (`MEMORY_LIMIT`). The worker replaces the child as a memory recycle; each run that the child held is lost with it
//   and follows the retry policy of its task.
// - Below the bound, or with no run held, the watch waits for the next interval. The timer does not keep the child
//   alive; the child ends its process by its exit rule.

import type { MemoryLimitRecord } from "../native/child/addon.ts";
import { unreachable } from "../wire/unreachable.ts";

/** The pause between two reads of the resident memory of a thread child that holds runs, in milliseconds. */
export const MEMORY_WATCH_INTERVAL_MS = 100;

const BYTES_PER_MIB = 1024 * 1024;

/** The resident memory of the child process, and the end of the child at its bound. */
export interface ChildMemory {
  /** The resident set size of this process, in bytes. */
  readonly residentBytes: () => number;
  /** Write the line of the end with `boundMb` and `measuredMb` (MiB), then end the process with exit code 81. */
  readonly endAtLimit: (boundMb: number, measuredMb: number) => never;
}

/** What the watch reads and calls. */
export interface MemoryWatchDeps {
  readonly memory: ChildMemory;
  /** Whether the child holds a run now. */
  readonly holdsRuns: () => boolean;
  /** Call `callback` after `milliseconds`; the result cancels it. */
  readonly timer: (milliseconds: number, callback: () => void) => () => void;
}

/** The watch of a child: none (`recycleAbove` is off), or a watch against the bound of `boundMb` MiB. */
export type MemoryWatch = { readonly kind: "noWatch" } | { readonly kind: "watching"; readonly boundMb: number };

/** The result of one read: no run was held (no read), or the measured memory is below the bound. */
export type MemoryRead = { readonly kind: "noRunHeld" } | { readonly kind: "belowBound"; readonly measuredMb: number };

/** Start the watch of `limit`. Effects: with `atMb`, the first timer; with `off`, none. */
export function startMemoryWatch(limit: MemoryLimitRecord, deps: MemoryWatchDeps): MemoryWatch {
  switch (limit.kind) {
    case "off":
      return Object.freeze({ kind: "noWatch" });
    case "atMb":
      nextRead(limit.mb, deps);

      return Object.freeze({ kind: "watching", boundMb: limit.mb });
    default:
      return unreachable(limit, "startMemoryWatch");
  }
}

/** Arm the timer of the next read; after the read, arm the next one. The child's exit ends the chain. */
function nextRead(boundMb: number, deps: MemoryWatchDeps): void {
  // The chain stops only with the process, so the cancel of the timer is not kept. A read below the bound or with no
  // run held changes nothing.
  void deps.timer(MEMORY_WATCH_INTERVAL_MS, () => {
    void readMemory(boundMb, deps);
    nextRead(boundMb, deps);
  });
}

/**
 * One read of the watch against the bound of `boundMb` MiB: no read while the child holds no run; the measured value
 * below the bound; at or above it, the end of the child (does not return).
 */
export function readMemory(boundMb: number, deps: Pick<MemoryWatchDeps, "memory" | "holdsRuns">): MemoryRead {
  if (!deps.holdsRuns()) {
    return { kind: "noRunHeld" };
  }

  const measuredMb = deps.memory.residentBytes() / BYTES_PER_MIB;

  if (measuredMb >= boundMb) {
    return deps.memory.endAtLimit(boundMb, measuredMb);
  }

  return { kind: "belowBound", measuredMb };
}
