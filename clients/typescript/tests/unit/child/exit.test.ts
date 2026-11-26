// The exit rule: the flush runs before the exit; a flush past its bound does not hold the exit; the end runs once.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { CHILD_EXIT_FLUSH_BOUND_MS, ChildEnd, exitAfterFlush } from "../../../horsies/src/child/exit.ts";

class Ended extends Error {
  readonly code: number;

  constructor(code: number) {
    super(`ended with ${String(code)}`);
    this.code = code;
  }
}

async function codeOf(run: Promise<never>): Promise<number> {
  return run.then(
    () => assert.fail("the process did not end"),
    (cause: unknown) => (cause instanceof Ended ? cause.code : assert.fail(String(cause))),
  );
}

describe("the exit rule", () => {
  test("the flush runs before the exit", async () => {
    const order: string[] = [];

    const code = await codeOf(
      exitAfterFlush(0, {
        flush: async () => {
          order.push("flush");
        },
        wait: async () => new Promise<void>(() => undefined),
        exit: (exitCode) => {
          order.push("exit");

          throw new Ended(exitCode);
        },
      }),
    );

    assert.equal(code, 0);
    assert.deepEqual(order, ["flush", "exit"]);
  });

  test("a flush past its bound does not hold the exit; a flush that rejects does not either", async () => {
    const waits: number[] = [];

    const exit = (exitCode: number): never => {
      throw new Ended(exitCode);
    };

    const hung = await codeOf(
      exitAfterFlush(173, {
        flush: async () => new Promise<void>(() => undefined),
        wait: async (milliseconds) => {
          waits.push(milliseconds);
        },
        exit,
      }),
    );

    assert.equal(hung, 173);
    assert.deepEqual(waits, [CHILD_EXIT_FLUSH_BOUND_MS]);

    const rejected = await codeOf(
      exitAfterFlush(1, {
        flush: async () => Promise.reject(new Error("the exporter is down")),
        wait: async () => new Promise<void>(() => undefined),
        exit,
      }),
    );

    assert.equal(rejected, 1);
  });

  test("the end runs once; a later end waits for the first code", async () => {
    let exits = 0;

    const end = new ChildEnd({
      flush: async () => undefined,
      wait: async () => new Promise<void>(() => undefined),
      exit: (exitCode) => {
        exits += 1;

        throw new Ended(exitCode);
      },
    });

    const first = codeOf(end.end(1));
    const second = codeOf(end.end(0));

    assert.deepEqual(await Promise.all([first, second]), [1, 1]);
    assert.equal(exits, 1);
  });
});
