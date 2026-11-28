// The in-run memory watch of a `SHARED_THREAD` child: no watch when `recycleAbove` is off; with a bound, a read every
// `MEMORY_WATCH_INTERVAL_MS` only while the child holds runs, and the end of the child at or above the bound.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  type ChildMemory,
  MEMORY_WATCH_INTERVAL_MS,
  type MemoryWatchDeps,
  readMemory,
  startMemoryWatch,
} from "../../../horsies/src/seats/memory-watch.ts";

const MIB = 1024 * 1024;

/** The end of the fake child at its bound. */
class EndedAtLimit extends Error {
  readonly boundMb: number;
  readonly measuredMb: number;

  constructor(boundMb: number, measuredMb: number) {
    super(`ended at ${String(measuredMb)} MiB, bound ${String(boundMb)} MiB`);
    this.boundMb = boundMb;
    this.measuredMb = measuredMb;
  }
}

/** A fake child: its resident memory, whether it holds runs, its reads, and its armed timers. */
class FakeChild {
  residentMb = 100;
  holding = true;
  reads = 0;
  readonly timers: { readonly milliseconds: number; readonly fire: () => void }[] = [];

  readonly memory: ChildMemory = {
    residentBytes: () => {
      this.reads += 1;

      return this.residentMb * MIB;
    },
    endAtLimit: (boundMb, measuredMb) => {
      throw new EndedAtLimit(boundMb, measuredMb);
    },
  };

  deps(): MemoryWatchDeps {
    return {
      memory: this.memory,
      holdsRuns: () => this.holding,
      timer: (milliseconds, fire) => {
        this.timers.push({ milliseconds, fire });

        return () => undefined;
      },
    };
  }

  /** Fire the last armed timer. */
  fireLast(): void {
    (this.timers.at(-1) ?? assert.fail("no timer is armed")).fire();
  }
}

describe("the memory watch of a SHARED_THREAD child", () => {
  test("recycleAbove off starts no watch: no timer, no read", () => {
    const child = new FakeChild();

    assert.deepEqual(startMemoryWatch({ kind: "off" }, child.deps()), { kind: "noWatch" });
    assert.deepEqual([child.timers.length, child.reads], [0, 0]);
  });

  test("a bound starts the watch: a read after each interval, and the next timer after each read", () => {
    const child = new FakeChild();

    assert.deepEqual(startMemoryWatch({ kind: "atMb", mb: 256 }, child.deps()), { kind: "watching", boundMb: 256 });
    assert.deepEqual(child.timers.map((timer) => timer.milliseconds), [MEMORY_WATCH_INTERVAL_MS]);
    assert.equal(child.reads, 0);
    child.fireLast();
    child.fireLast();
    assert.equal(child.reads, 2);
    assert.deepEqual(child.timers.map((timer) => timer.milliseconds), [MEMORY_WATCH_INTERVAL_MS, MEMORY_WATCH_INTERVAL_MS, MEMORY_WATCH_INTERVAL_MS]);
  });

  test("a child that holds no run is not read", () => {
    const child = new FakeChild();

    child.holding = false;
    child.residentMb = 4_096;
    assert.deepEqual(readMemory(256, child.deps()), { kind: "noRunHeld" });
    assert.equal(child.reads, 0);
  });

  test("below the bound, the read gives the measured MiB and the child lives", () => {
    const child = new FakeChild();

    child.residentMb = 255.5;
    assert.deepEqual(readMemory(256, child.deps()), { kind: "belowBound", measuredMb: 255.5 });
  });

  test("at or above the bound, the child ends with the bound and the measured MiB", () => {
    for (const residentMb of [256, 300.25]) {
      const child = new FakeChild();

      child.residentMb = residentMb;
      assert.throws(() => readMemory(256, child.deps()), { boundMb: 256, measuredMb: residentMb });
    }
  });
});
