// The command `horsies get-docs`: its flags, its output lines and its exit codes, as Python's `horsies get-docs`
// (`--output`, `--ref main`) gives them; and the bounded run of an external tool.

import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ParseArgsOptionsConfig } from "node:util";

import type { CommandExitCode } from "../cli/usage.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { parseToolFlags, reportUsageError, switchFlagOf, textFlagOf, TOOL_FAILED, TOOL_PASSED, type ToolOutput } from "./command-line.ts";
import {
  DEFAULT_OUTPUT_DIR,
  docsErrorText,
  fetchDocs,
  type FetchDeps,
  MAX_STDERR_QUOTE_BYTES,
  packageVersionOf,
  type RefRequest,
  resolveDocsRef,
  type ToolCall,
  type ToolFailure,
  TOOL_RUN_LIMITS,
  type ToolRunLimits,
} from "./docs.ts";
import { attempt, type IoCause, ioCauseOf } from "./files.ts";

/** The usage line of `horsies get-docs`. */
export const GET_DOCS_USAGE = "horsies get-docs [-h] [--output OUTPUT] [--ref {main}]";

/** The help of `horsies get-docs`. */
export const GET_DOCS_HELP: readonly string[] = [
  `usage: ${GET_DOCS_USAGE}`,
  "",
  "Fetch the docs of this release for AI agents: the shared pages and the TypeScript pages.",
  "",
  "options:",
  "  -h, --help       show this help message and exit",
  `  --output OUTPUT  Output directory (default: ${DEFAULT_OUTPUT_DIR})`,
  "  --ref {main}     Download the main branch docs instead of the release tag that matches the installed version",
  "                   (required for a pre-release version)",
];

const GET_DOCS_FLAGS: ParseArgsOptionsConfig = {
  output: { type: "string" },
  ref: { type: "string" },
  help: { type: "boolean", short: "h" },
};

/** What `get-docs` does that has effects outside its arguments. */
export interface GetDocsDeps extends FetchDeps {
  readonly output: ToolOutput;
  /** The folder of the installed package, which holds its `package.json`. */
  readonly packageFolder: string;
  /** Make a new empty folder for the checkout. */
  makeWorkspace(): Result<string, IoCause>;
  /** Remove the checkout folder. */
  removeWorkspace(folder: string): Result<void, IoCause>;
}

/** The `--ref` request of a flag value: `main`, no `--ref`, or a usage error. Pure. */
function refRequestOf(value: { readonly kind: "given"; readonly text: string } | { readonly kind: "notGiven" }): RefRequest | "refused" {
  switch (value.kind) {
    case "notGiven":
      return "notGiven";
    case "given":
      return value.text === "main" ? "main" : "refused";
  }
}

/**
 * Run `horsies get-docs` with the arguments after `get-docs`. Exit codes: 0 with the status lines and the file count on
 * stderr; 1 with `error: <message>` on stderr; 2 for arguments that do not parse. `--help` prints the help and gives
 * 0. Effects: reads the package version; runs git, or curl and tar; writes the output folder.
 */
export function runGetDocs(args: readonly string[], deps: GetDocsDeps): CommandExitCode {
  const flags = parseToolFlags(args, GET_DOCS_FLAGS);

  if (!flags.ok) {
    return reportUsageError(deps.output, "get-docs", GET_DOCS_USAGE, flags.error);
  }

  if (switchFlagOf(flags.value, "help")) {
    for (const line of GET_DOCS_HELP) {
      deps.output.writeOut(line);
    }

    return TOOL_PASSED;
  }

  const request = refRequestOf(textFlagOf(flags.value, "ref"));

  if (request === "refused") {
    return reportUsageError(deps.output, "get-docs", GET_DOCS_USAGE, {
      kind: "usageError",
      message: "argument --ref: invalid choice (choose from main)",
    });
  }

  const outputFlag = textFlagOf(flags.value, "output");
  const output = outputFlag.kind === "given" ? outputFlag.text : DEFAULT_OUTPUT_DIR;
  const fetched = fetchIntoOutput(deps, request, output);

  if (!fetched.ok) {
    deps.output.writeError(`error: ${fetched.error}`);

    return TOOL_FAILED;
  }

  deps.output.writeError(`Done. ${fetched.value} files written to ${output}/`);

  return TOOL_PASSED;
}

/** The version, the ref and the fetch, in a workspace that is removed at the end. Errors: the message of the failure. */
function fetchIntoOutput(deps: GetDocsDeps, request: RefRequest, output: string): Result<number, string> {
  const version = packageVersionOf(deps.packageFolder);
  const reference = version.ok ? resolveDocsRef(version.value, request) : version;

  if (!reference.ok) {
    return err(docsErrorText(reference.error));
  }

  const workspace = deps.makeWorkspace();

  if (!workspace.ok) {
    return err(`create a temporary folder failed for ${tmpdir()}: ${workspace.error.message}`);
  }

  const fetched = fetchDocs(deps, reference.value, workspace.value, output);
  const removed = deps.removeWorkspace(workspace.value);

  if (!removed.ok) {
    deps.writeStatus(`The temporary folder ${workspace.value} was not removed (${removed.error.message}). Remove it by hand.`);
  }

  return fetched.ok ? fetched : err(docsErrorText(fetched.error));
}

/** The start of the stderr of a failed run, as text, trimmed. Pure. */
function stderrQuote(stderr: Buffer): string {
  return stderr.subarray(0, MAX_STDERR_QUOTE_BYTES).toString("utf8").trim();
}

/**
 * Run `call` with stdin and stdout closed and stderr kept, within `limits`; a run past the time bound, or one that
 * writes more stderr than the bound, is killed. Effects: runs the tool; blocks the thread until it ends.
 */
export function runToolBounded(call: ToolCall, limits: ToolRunLimits = TOOL_RUN_LIMITS): Result<undefined, ToolFailure> {
  const run = spawnSync(call.command, [...call.args], {
    cwd: call.cwd,
    stdio: ["ignore", "ignore", "pipe"],
    timeout: limits.timeMs,
    killSignal: "SIGKILL",
    maxBuffer: limits.stderrBytes,
  });

  if (run.error !== undefined) {
    const cause = ioCauseOf(run.error);

    switch (cause.code) {
      case "ETIMEDOUT":
        return err({ kind: "timedOut", boundMs: limits.timeMs });
      case "ENOBUFS":
        return err({ kind: "stderrTooLarge", boundBytes: limits.stderrBytes });
      default:
        return err({ kind: "notStarted", cause });
    }
  }

  if (run.signal !== null) {
    return err({ kind: "signalled", signal: run.signal });
  }

  if (run.status === null) {
    // Node gives a status or a signal for every run that started.
    throw new ClientDefect({ subject: "runToolBounded", rule: "a run with no error and no signal has an exit status" });
  }

  return run.status === 0 ? ok(undefined) : err({ kind: "exited", status: run.status, stderr: stderrQuote(run.stderr) });
}

/** The deps of `get-docs` in this process. */
export function processGetDocsDeps(output: ToolOutput, packageFolder: string): GetDocsDeps {
  return {
    output,
    packageFolder,
    runTool: (call) => runToolBounded(call),
    writeStatus: (line) => {
      output.writeError(line);
    },
    makeWorkspace: () => attempt(() => mkdtempSync(join(tmpdir(), "horsies-docs-"))),
    removeWorkspace: (folder) => attempt(() => rmSync(folder, { recursive: true, force: true })),
  };
}
