// The child command of a worker: each launch form, each flag that a child does not get, and each start that has no
// entry script to start again.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  childCommand,
  childCommandRefusedText,
  type ChildCommandBuilt,
  type ChildCommandRefused,
  keptFlags,
  type LaunchFacts,
} from "../../../horsies/src/worker/child-command.ts";

const NODE = "/usr/local/bin/node";
const BUN = "/usr/local/bin/bun";
const ENTRY = "/srv/app/dist/main.js";

function facts(parts: Partial<LaunchFacts>): LaunchFacts {
  return { execPath: NODE, execArgv: [], argv: [NODE, ENTRY, "worker"], packaging: "script", ...parts };
}

function built(launch: LaunchFacts): ChildCommandBuilt {
  const outcome = childCommand(launch);

  if (!outcome.ok) {
    assert.fail(`the command is refused: ${childCommandRefusedText(outcome.error)}`);
  }

  return outcome.value;
}

function refused(launch: LaunchFacts): ChildCommandRefused {
  const outcome = childCommand(launch);

  if (outcome.ok) {
    assert.fail(`the command is built: ${outcome.value.command.args.join(" ")}`);
  }

  return outcome.error;
}

describe("the child command", () => {
  test("node with a script: the runtime, the script and the child flag", () => {
    assert.deepEqual(built(facts({})), {
      command: { program: NODE, args: [ENTRY, "--horsies-child"] },
      dropped: [],
    });
  });

  test("node with a loader: each loader flag is kept, in order", () => {
    const command = built(facts({ execArgv: ["--import", "tsx"], argv: [NODE, "/srv/app/main.ts", "worker"] })).command;

    assert.deepEqual(command.args, ["--import", "tsx", "/srv/app/main.ts", "--horsies-child"]);
  });

  test("the inner node of tsx: the loader flags that tsx gave it are kept", () => {
    const execArgv = ["--require", "/srv/node_modules/tsx/dist/preflight.cjs", "--import", "file:///srv/node_modules/tsx/dist/loader.mjs"];

    assert.deepEqual(built(facts({ execArgv })).command.args, [...execArgv, ENTRY, "--horsies-child"]);
  });

  test("a heap limit is kept: each child gets the same limit", () => {
    assert.deepEqual(built(facts({ execArgv: ["--max-old-space-size=4096"] })).command.args, [
      "--max-old-space-size=4096",
      ENTRY,
      "--horsies-child",
    ]);
  });

  test("bun with a script and the flags that bun reports", () => {
    assert.deepEqual(built(facts({ execPath: BUN, execArgv: ["--smol"], argv: [BUN, ENTRY, "worker"] })).command, {
      program: BUN,
      args: ["--smol", ENTRY, "--horsies-child"],
    });
  });

  test("each file watcher and inspector flag is dropped, with its value", () => {
    const cases: readonly (readonly [readonly string[], readonly string[]])[] = [
      [["--watch"], ["--watch"]],
      [["--watch-path", "src"], ["--watch-path src"]],
      [["--watch-path=src"], ["--watch-path=src"]],
      [["--watch-preserve-output"], ["--watch-preserve-output"]],
      [["--hot"], ["--hot"]],
      [["--inspect"], ["--inspect"]],
      [["--inspect=0.0.0.0:9229"], ["--inspect=0.0.0.0:9229"]],
      [["--inspect-brk"], ["--inspect-brk"]],
      [["--inspect-wait"], ["--inspect-wait"]],
      [["--inspect-port", "9230"], ["--inspect-port 9230"]],
      [["--inspect-port=9230"], ["--inspect-port=9230"]],
      [["--inspect-publish-uid", "http"], ["--inspect-publish-uid http"]],
    ];

    for (const [execArgv, dropped] of cases) {
      assert.deepEqual(keptFlags(["--enable-source-maps", ...execArgv]), { kept: ["--enable-source-maps"], dropped }, execArgv.join(" "));
    }
  });

  test("the dropped flags are reported with the built command", () => {
    assert.deepEqual(built(facts({ execArgv: ["--watch", "--import", "tsx"] })), {
      command: { program: NODE, args: ["--import", "tsx", ENTRY, "--horsies-child"] },
      dropped: ["--watch"],
    });
  });

  test("code from the command line has no entry script", () => {
    for (const execArgv of [["-e", "1"], ["--eval", "1"], ["--eval=1"], ["-p", "1"], ["--print", "1"]]) {
      assert.deepEqual(refused(facts({ execArgv, argv: [NODE, "worker"] })), { kind: "evaluatedCode" }, execArgv.join(" "));
    }
  });

  test("a REPL has no entry script", () => {
    assert.deepEqual(refused(facts({ argv: [NODE] })), { kind: "noEntryScript" });
  });

  test("a script from stdin cannot be read again", () => {
    assert.deepEqual(refused(facts({ argv: [NODE, "-", "worker"] })), { kind: "stdinScript" });
  });

  test("an entry that is not an absolute path is refused", () => {
    assert.deepEqual(refused(facts({ argv: [NODE, "[eval]", "worker"] })), { kind: "entryNotAbsolute", entry: "[eval]" });
  });

  test("a compiled executable is refused", () => {
    assert.deepEqual(refused(facts({ packaging: "bunCompiled" })), { kind: "compiledExecutable", packaging: "bunCompiled" });
    assert.deepEqual(refused(facts({ packaging: "nodeSingleExecutable" })), {
      kind: "compiledExecutable",
      packaging: "nodeSingleExecutable",
    });
  });

  test("each refusal has its text", () => {
    const refusals: readonly ChildCommandRefused[] = [
      { kind: "evaluatedCode" },
      { kind: "noEntryScript" },
      { kind: "stdinScript" },
      { kind: "entryNotAbsolute", entry: "[eval]" },
      { kind: "compiledExecutable", packaging: "bunCompiled" },
    ];

    assert.equal(new Set(refusals.map(childCommandRefusedText)).size, refusals.length);
  });
});
