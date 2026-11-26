// The process events of a child: no-op signal handlers; a detached throw with a run is logged with the run and the
// child lives; a throw with no run ends the child with 1; a rejection that no body awaits is logged and the child
// lives.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { installProcessEvents, UNCAUGHT_EXIT_CODE } from "../../../horsies/src/child/process-events.ts";
import { currentRun, runStore } from "../../../horsies/src/child/run-store.ts";
import { RUN_TASK_ID } from "./support.ts";

function installed(): {
  readonly lines: string[];
  readonly ends: number[];
  readonly signals: Map<string, () => void>;
  uncaught: (cause: unknown) => void;
  rejection: (cause: unknown) => void;
} {
  const lines: string[] = [];
  const ends: number[] = [];
  const signals = new Map<string, () => void>();

  const handlers = {
    lines,
    ends,
    signals,
    uncaught: (cause: unknown): void => void cause,
    rejection: (cause: unknown): void => void cause,
  };

  installProcessEvents({
    hooks: {
      onSignal: (signal, listener) => {
        signals.set(signal, listener);
      },
      onUncaught: (listener) => {
        handlers.uncaught = listener;
      },
      onRejection: (listener) => {
        handlers.rejection = listener;
      },
    },
    writeLine: (line) => lines.push(line),
    end: (code) => ends.push(code),
    currentRun,
  });

  return handlers;
}

const MARK = { taskId: RUN_TASK_ID, taskName: "charge", attempt: 3 } as const;

describe("the process events of a child", () => {
  test("SIGINT and SIGTERM have handlers that do nothing", () => {
    const events = installed();

    assert.deepEqual([...events.signals.keys()], ["SIGINT", "SIGTERM"]);
    events.signals.get("SIGINT")?.();
    events.signals.get("SIGTERM")?.();
    assert.deepEqual(events.lines, []);
    assert.deepEqual(events.ends, []);
  });

  test("a detached throw with a run in the store is logged with the run, and the child lives", () => {
    const events = installed();

    runStore.run(MARK, () => {
      events.uncaught(new TypeError("late callback"));
    });

    assert.equal(events.ends.length, 0);
    assert.match(events.lines[0] ?? "", /^horsies child: a callback threw in the run of task charge \(task [0-9a-f-]+, attempt 3\): TypeError: late callback/u);
  });

  test("a throw with no run ends the child with 1", () => {
    const events = installed();

    events.uncaught(new Error("module code failed"));

    assert.deepEqual(events.ends, [UNCAUGHT_EXIT_CODE]);
    assert.equal(UNCAUGHT_EXIT_CODE, 1);
    assert.match(events.lines[0] ?? "", /^horsies child: code outside every run threw: Error: module code failed/u);
  });

  test("an unhandled rejection is logged, and the child lives", () => {
    const events = installed();

    runStore.run(MARK, () => {
      events.rejection(new Error("nobody awaits"));
    });
    events.rejection("a plain text");

    assert.deepEqual(events.ends, []);
    assert.match(events.lines[0] ?? "", /a Promise that no body awaits rejected in the run of task charge/u);
    assert.match(events.lines[1] ?? "", /a Promise that no body awaits rejected: string: a plain text; the child lives$/u);
  });
});
