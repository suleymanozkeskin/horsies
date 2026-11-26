// The command that starts each child of a worker: the runtime of this process (Node or Bun), its flags, the entry
// script, and the child mode flag. The core appends the socket descriptor number and the worker pid.
//
//   program = process.execPath
//   args    = [...kept(process.execArgv), entry, "--horsies-child"]
//
// - Each flag of the runtime is kept, so each child has the same loaders and limits, except a closed list of flags
//   that must not reach a child: a child must not restart itself on a file change, and must not bind the debugger
//   port of the worker. The environment and the working directory reach the children unchanged.
// - A start with no entry script (code from `-e`, a REPL, a script from stdin) and a compiled executable are
//   refused: nothing starts.

import { isAbsolute } from "node:path";

import { CHILD_MODE_FLAG } from "../app/suppression.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";

/** How this process was packaged: a script that a runtime runs, or a compiled executable with the code inside. */
export type Packaging = "script" | "nodeSingleExecutable" | "bunCompiled";

/** The facts of this process that the child command reads. */
export type LaunchFacts = {
  /** The runtime binary (`process.execPath`). */
  readonly execPath: string;
  /** The flags of the runtime (`process.execArgv`). */
  readonly execArgv: readonly string[];
  /** The arguments of the process (`process.argv`): the runtime, the entry script, then the command. */
  readonly argv: readonly string[];
  readonly packaging: Packaging;
};

/** The program of a child and its first arguments. */
export type ChildCommand = { readonly program: string; readonly args: readonly string[] };

/** A child command and the runtime flags that it leaves out. */
export type ChildCommandBuilt = { readonly command: ChildCommand; readonly dropped: readonly string[] };

/** Why no child command can be built. Nothing starts. */
export type ChildCommandRefused =
  | { readonly kind: "evaluatedCode" }
  | { readonly kind: "noEntryScript" }
  | { readonly kind: "stdinScript" }
  | { readonly kind: "entryNotAbsolute"; readonly entry: string }
  | { readonly kind: "compiledExecutable"; readonly packaging: Exclude<Packaging, "script"> };

/** A runtime flag that a child must not get, and whether it takes a value in the next argument. */
type DroppedFlag = { readonly name: string; readonly value: "inline" | "inlineOrNext" };

/**
 * The flags that a child must not get: the file watchers of Node and Bun (a child must not restart itself on a file
 * change) and the inspector flags (every child would bind the debugger port of the worker).
 */
const DROPPED_FLAGS: readonly DroppedFlag[] = [
  { name: "--watch", value: "inline" },
  { name: "--watch-path", value: "inlineOrNext" },
  { name: "--watch-preserve-output", value: "inline" },
  { name: "--hot", value: "inline" },
  { name: "--inspect", value: "inline" },
  { name: "--inspect-brk", value: "inline" },
  { name: "--inspect-wait", value: "inline" },
  { name: "--inspect-port", value: "inlineOrNext" },
  { name: "--inspect-publish-uid", value: "inlineOrNext" },
];

/** The flags with which the runtime runs code from the command line, with no entry script. */
const EVALUATE_FLAGS: readonly string[] = ["-e", "--eval", "-p", "--print"];

/** The entry of a script that the runtime reads from stdin. */
const STDIN_ENTRY = "-";

/** The facts of this process. Effects: reads `process` and, on Node, whether it is a single executable application. */
export function processLaunchFacts(): LaunchFacts {
  return {
    execPath: process.execPath,
    execArgv: [...process.execArgv],
    argv: [...process.argv],
    packaging: processPackaging(),
  };
}

/** The roots under which a Bun compiled executable keeps its files. */
const BUN_COMPILED_ROOTS: readonly string[] = ["/$bunfs/", "B:\\~BUN\\", "B:/~BUN/"];

function processPackaging(): Packaging {
  const entry = process.argv[1] ?? "";

  if (process.versions["bun"] !== undefined) {
    return BUN_COMPILED_ROOTS.some((root) => entry.startsWith(root)) ? "bunCompiled" : "script";
  }

  return process.getBuiltinModule("node:sea").isSea() ? "nodeSingleExecutable" : "script";
}

/** The child command of `facts`, or why there is none. Pure. */
export function childCommand(facts: LaunchFacts): Result<ChildCommandBuilt, ChildCommandRefused> {
  if (facts.packaging !== "script") {
    return err({ kind: "compiledExecutable", packaging: facts.packaging });
  }

  if (facts.execArgv.some((flag) => EVALUATE_FLAGS.some((name) => flag === name || flag.startsWith(`${name}=`)))) {
    return err({ kind: "evaluatedCode" });
  }

  const entry = facts.argv[1];

  if (entry === undefined) {
    return err({ kind: "noEntryScript" });
  }

  if (entry === STDIN_ENTRY) {
    return err({ kind: "stdinScript" });
  }

  if (!isAbsolute(entry)) {
    return err({ kind: "entryNotAbsolute", entry });
  }

  const flags = keptFlags(facts.execArgv);

  return ok({
    command: { program: facts.execPath, args: [...flags.kept, entry, CHILD_MODE_FLAG] },
    dropped: flags.dropped,
  });
}

/** The flags of `execArgv` that a child gets, in order, and the flags left out, each with its value. Pure. */
export function keptFlags(execArgv: readonly string[]): { readonly kept: readonly string[]; readonly dropped: readonly string[] } {
  const kept: string[] = [];
  const dropped: string[] = [];
  let index = 0;

  while (index < execArgv.length) {
    const flag = execArgv[index] ?? "";
    const match = DROPPED_FLAGS.find((candidate) => flag === candidate.name || flag.startsWith(`${candidate.name}=`));
    const next = execArgv[index + 1];
    const takesNext = match?.value === "inlineOrNext" && flag === match.name && next !== undefined;

    if (match === undefined) {
      kept.push(flag);
    } else {
      dropped.push(takesNext ? `${flag} ${next}` : flag);
    }

    index += takesNext ? 2 : 1;
  }

  return { kept, dropped };
}

/** The text of a refusal. Pure. */
export function childCommandRefusedText(refused: ChildCommandRefused): string {
  switch (refused.kind) {
    case "evaluatedCode":
      return "the process runs code from the command line (-e, --eval, -p, --print), so a child has no entry script to start again";
    case "noEntryScript":
      return "the process has no entry script (a REPL), so a child has nothing to start again";
    case "stdinScript":
      return "the entry script came from stdin, so a child cannot read it again";
    case "entryNotAbsolute":
      return `the entry script ${JSON.stringify(refused.entry)} is not an absolute path`;
    case "compiledExecutable":
      return `a ${refused.packaging === "bunCompiled" ? "Bun compiled executable" : "Node single executable application"} is not a supported worker entry; run the entry script with the runtime`;
    default:
      return unreachable(refused, "childCommandRefusedText");
  }
}
