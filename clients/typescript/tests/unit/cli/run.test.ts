// `cli.run`: the mode of a process, the command table, and the exit rule (the wait for the core log records, then
// the end of the process with the command's exit code).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  type CliDeps,
  COMMAND_NAMES,
  type CommandTable,
  EXIT_LOG_POLL_MS,
  EXIT_LOG_WAIT_MS,
  processMode,
  type ProcessFacts,
  runCli,
  waitForLogRecords,
} from "../../../horsies/src/cli/run.ts";
import type { CommandExitCode } from "../../../horsies/src/cli/usage.ts";
import type { LogBacklog } from "../../../horsies/src/native/worker/addon.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { testApp } from "../worker/support.ts";

const ENTRY: readonly string[] = ["/usr/local/bin/node", "/srv/app/main.js"];

function main(...args: readonly string[]): ProcessFacts {
  return { thread: "main", argv: [...ENTRY, ...args] };
}

/** The end of a fake process: a throw that carries the exit code, as `process.exit` does not return. */
class Ended extends Error {
  readonly code: number;

  constructor(code: number) {
    super(`the process ended with ${String(code)}`);
    this.code = code;
  }
}

/** A table whose commands record their arguments and give `code`. */
function recordingTable(code: CommandExitCode): CommandTable & { readonly calls: string[][] } {
  const calls: string[][] = [];

  return {
    calls,
    worker: async (_app, args) => {
      calls.push(["worker", ...args]);

      return code;
    },
    check: async (_app, args) => {
      calls.push(["check", ...args]);

      return code;
    },
  };
}

function fakeDeps(facts: ProcessFacts, table: CommandTable, backlog: readonly LogBacklog[] = ["empty"]): CliDeps & {
  readonly errors: string[];
  readonly pauses: number[];
  readonly children: (readonly string[])[];
} {
  const errors: string[] = [];
  const pauses: number[] = [];
  const children: (readonly string[])[] = [];
  let reads = 0;

  return {
    errors,
    pauses,
    children,
    facts,
    table,
    child: async (_app, args) => {
      children.push(args);

      throw new Ended(0);
    },
    output: { writeOut: () => undefined, writeError: (line) => errors.push(line) },
    logBacklog: () => {
      reads += 1;

      return backlog[Math.min(reads - 1, backlog.length - 1)] ?? "empty";
    },
    pause: async (milliseconds) => {
      pauses.push(milliseconds);
    },
    exit: (code) => {
      throw new Ended(code);
    },
  };
}

async function endedCode(run: Promise<number>): Promise<number> {
  try {
    await run;
  } catch (cause) {
    if (cause instanceof Ended) {
      return cause.code;
    }

    throw cause;
  }

  return assert.fail("the process did not end");
}

describe("the mode of a process", () => {
  test("a worker thread", () => {
    assert.deepEqual(processMode({ thread: "worker", argv: [...ENTRY, "worker"] }), { kind: "workerThread" });
  });

  test("a command and its arguments", () => {
    assert.deepEqual(processMode(main("worker", "--profile", "io")), {
      kind: "command",
      command: "worker",
      args: ["--profile", "io"],
    });
    assert.deepEqual(processMode(main("check")), { kind: "command", command: "check", args: [] });
  });

  test("the child flag with a descriptor and a pid is child mode, with every argument after the entry", () => {
    assert.deepEqual(processMode(main("--horsies-child", "3", "4242")), {
      kind: "child",
      args: ["--horsies-child", "3", "4242"],
    });
  });

  test("the child flag with bad arguments is child mode too: the core reads them and the child ends with 73", () => {
    for (const args of [["--horsies-child"], ["--horsies-child", "3", "4242", "extra"], ["--horsies-child", "x", "y"]]) {
      assert.deepEqual(processMode(main(...args)), { kind: "child", args });
    }
  });

  test("no command and an unknown command are usage errors", () => {
    assert.equal(processMode(main()).kind, "usageError");
    assert.equal(processMode(main("serve")).kind, "usageError");
  });

  test("the command names", () => {
    assert.deepEqual(COMMAND_NAMES, ["worker", "check"]);
  });
});

describe("cli.run", () => {
  test("a worker thread is a defect", async () => {
    const table = recordingTable(0);

    await assert.rejects(runCli(testApp(), fakeDeps({ thread: "worker", argv: [...ENTRY] }, table)), ClientDefect);
    assert.equal(table.calls.length, 0);
  });

  test("child mode runs the child with its arguments, and no command runs", async () => {
    const table = recordingTable(0);
    const deps = fakeDeps(main("--horsies-child", "3", "4242"), table);

    assert.equal(await endedCode(runCli(testApp(), deps)), 0);
    assert.deepEqual(deps.children, [["--horsies-child", "3", "4242"]]);
    assert.equal(table.calls.length, 0);
  });

  test("the command runs with its arguments and the process ends with its code", async () => {
    const table = recordingTable(1);

    assert.equal(await endedCode(runCli(testApp(), fakeDeps(main("check", "--live"), table))), 1);
    assert.deepEqual(table.calls, [["check", "--live"]]);
  });

  test("an unknown command ends the process with 2 and the usage", async () => {
    const deps = fakeDeps(main("serve"), recordingTable(0));

    assert.equal(await endedCode(runCli(testApp(), deps)), 2);
    assert.ok(deps.errors[0]?.startsWith("usage: <entry> {worker,check}"));
    assert.ok(deps.errors[1]?.startsWith("horsies: error: unknown command"));
  });

  test("the end waits while core log records wait in the channel", async () => {
    const deps = fakeDeps(main("check"), recordingTable(0), ["waiting", "waiting", "empty"]);

    assert.equal(await endedCode(runCli(testApp(), deps)), 0);
    assert.deepEqual(deps.pauses, [EXIT_LOG_POLL_MS, EXIT_LOG_POLL_MS]);
  });

  test("the wait for the log records ends at its bound", async () => {
    const pauses: number[] = [];

    await waitForLogRecords({
      logBacklog: () => "waiting",
      pause: async (milliseconds) => {
        pauses.push(milliseconds);
      },
    });

    assert.equal(pauses.length, EXIT_LOG_WAIT_MS / EXIT_LOG_POLL_MS);
  });

  test("records that stay with no forwarder end the wait at once", async () => {
    const pauses: number[] = [];

    await waitForLogRecords({
      logBacklog: () => "noForwarder",
      pause: async (milliseconds) => {
        pauses.push(milliseconds);
      },
    });

    assert.deepEqual(pauses, []);
  });
});
