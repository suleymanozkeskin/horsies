// `horsies get-docs`: fetch the docs of this release for AI agents, as the Python client's `horsies get-docs` and the
// Rust client's shipped binary do.
//
// The docs are those of the release tag `v<package version>`; `--ref main` fetches the `main` branch instead, which
// can differ from this version. Two strategies, in order: a git sparse checkout, then the tarball of the ref through
// `curl` and `tar`. Each external tool runs at most `TOOL_RUN_BOUND_MS`. `docs-tree.ts` keeps the pages that a
// TypeScript user reads and replaces the output folder only after the new copy is complete.

import { mkdirSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

import { isPlainObject, isString } from "../codec/values.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { assembleOutput, type TreeCopyError, treeCopyErrorText } from "./docs-tree.ts";
import { attempt, type BoundedReadError, boundedReadText, type IoCause, readBounded } from "./files.ts";

export const REPO_URL = "https://github.com/suleymanozkeskin/horsies";
export const DEFAULT_OUTPUT_DIR = ".horsies-docs";
export const DOCS_REPO_PATH = "website/src/content/docs";
export const LLMS_REPO_PATH = "website/public/typescript/llms.txt";
export const MAIN_BRANCH = "main";

/** The longest that one git, curl or tar run takes before it is killed, in milliseconds. */
export const TOOL_RUN_BOUND_MS = 60_000;

/** The most stderr bytes of a tool run that the run keeps; a run that writes more is killed. */
export const MAX_TOOL_STDERR_BYTES: number = 1024 * 1024;

/** The bounds of one tool run: its wall time and the stderr that it can write. */
export type ToolRunLimits = { readonly timeMs: number; readonly stderrBytes: number };

/** The bounds of the tool runs of `get-docs`. */
export const TOOL_RUN_LIMITS: ToolRunLimits = { timeMs: TOOL_RUN_BOUND_MS, stderrBytes: MAX_TOOL_STDERR_BYTES };

/** The most stderr bytes of a failed tool run that an error quotes. */
export const MAX_STDERR_QUOTE_BYTES = 4096;

/** The largest `package.json` of the package that the version read takes, in bytes. */
export const MAX_MANIFEST_BYTES: number = 1024 * 1024;

/** The git ref of the docs. */
export type DocsRef = { readonly kind: "releaseTag"; readonly tag: string } | { readonly kind: "mainBranch" };

/** The branch that `--ref` names, or no `--ref`. */
export type RefRequest = "main" | "notGiven";

/** One run of an external tool. */
export type ToolCall = { readonly name: string; readonly command: string; readonly args: readonly string[]; readonly cwd: string };

/** Why a tool run did not end with exit status 0. */
export type ToolFailure =
  | { readonly kind: "notStarted"; readonly cause: IoCause }
  | { readonly kind: "exited"; readonly status: number; readonly stderr: string }
  | { readonly kind: "timedOut"; readonly boundMs: number }
  | { readonly kind: "stderrTooLarge"; readonly boundBytes: number }
  | { readonly kind: "signalled"; readonly signal: string };

/** Why the version of the package cannot be read. */
export type VersionUnreadable =
  | { readonly kind: "read"; readonly cause: BoundedReadError }
  | { readonly kind: "notJson"; readonly message: string }
  | { readonly kind: "noVersion" };

/** Why `get-docs` wrote nothing, or did not finish. */
export type DocsError =
  | { readonly kind: "packageVersionUnreadable"; readonly path: string; readonly reason: VersionUnreadable }
  | { readonly kind: "versionHasNoTag"; readonly version: string }
  | { readonly kind: "toolFailed"; readonly tool: string; readonly failure: ToolFailure }
  | { readonly kind: "tarballLayout"; readonly url: string }
  | { readonly kind: "workspace"; readonly path: string; readonly operation: "create the unpack folder" | "read the unpack folder"; readonly cause: IoCause }
  | { readonly kind: "treeCopy"; readonly error: TreeCopyError };

/** The name of a ref for git. Pure. */
export function gitNameOf(reference: DocsRef): string {
  switch (reference.kind) {
    case "releaseTag":
      return reference.tag;
    case "mainBranch":
      return MAIN_BRANCH;
  }
}

/** The tarball URL of a ref. Pure. */
export function tarballUrlOf(reference: DocsRef): string {
  switch (reference.kind) {
    case "releaseTag":
      return `${REPO_URL}/archive/refs/tags/${reference.tag}.tar.gz`;
    case "mainBranch":
      return `${REPO_URL}/archive/refs/heads/${MAIN_BRANCH}.tar.gz`;
  }
}

/** A version of three dot-separated runs of digits: a release version. */
const RELEASE_VERSION = /^\d+\.\d+\.\d+$/u;

/**
 * The ref to download, as Python's `resolve_docs_ref`: `--ref main` selects `main` for any version; else a version
 * `N.N.N` selects the tag `vN.N.N`. The tag is not checked here: a release with no published tag fails in the
 * download. Pure. Errors: `versionHasNoTag` for any other version (`0.9.0-rc.1`).
 */
export function resolveDocsRef(version: string, request: RefRequest): Result<DocsRef, DocsError> {
  switch (request) {
    case "main":
      return ok({ kind: "mainBranch" });
    case "notGiven":
      return RELEASE_VERSION.test(version) ? ok({ kind: "releaseTag", tag: `v${version}` }) : err({ kind: "versionHasNoTag", version });
  }
}

/** The version of the package in `packageFolder`, from its `package.json`. Effects: reads the file. */
export function packageVersionOf(packageFolder: string): Result<string, DocsError> {
  const path = join(packageFolder, "package.json");
  const bytes = readBounded(path, MAX_MANIFEST_BYTES);

  if (!bytes.ok) {
    return err({ kind: "packageVersionUnreadable", path, reason: { kind: "read", cause: bytes.error } });
  }

  let document: unknown;

  try {
    document = JSON.parse(bytes.value.toString("utf8"));
  } catch (cause) {
    if (!(cause instanceof SyntaxError)) {
      throw cause;
    }

    return err({ kind: "packageVersionUnreadable", path, reason: { kind: "notJson", message: cause.message } });
  }

  const version = isPlainObject(document) ? document["version"] : undefined;

  return isString(version) ? ok(version) : err({ kind: "packageVersionUnreadable", path, reason: { kind: "noVersion" } });
}

function versionUnreadableText(reason: VersionUnreadable): string {
  switch (reason.kind) {
    case "read":
      return boundedReadText(reason.cause);
    case "notJson":
      return `it is not JSON: ${reason.message}`;
    case "noVersion":
      return "it has no text version";
  }
}

/** The text of a failed tool run, for a message. Pure. */
export function toolFailureText(tool: string, failure: ToolFailure): string {
  switch (failure.kind) {
    case "notStarted":
      return `${tool} failed: ${failure.cause.message}`;
    case "exited":
      return `${tool} failed: exit status ${failure.status}: ${failure.stderr}`;
    case "timedOut":
      return `${tool} failed: timed out after ${failure.boundMs / 1000}s`;
    case "stderrTooLarge":
      return `${tool} failed: it wrote more than ${failure.boundBytes} bytes to stderr`;
    case "signalled":
      return `${tool} failed: ended by the signal ${failure.signal}`;
  }
}

/** The message of a failed `get-docs`, as Python's and the Rust tool's messages. Pure. */
export function docsErrorText(error: DocsError): string {
  switch (error.kind) {
    case "packageVersionUnreadable":
      return `the version of the horsies package cannot be read from ${error.path}: ${versionUnreadableText(error.reason)}`;
    case "versionHasNoTag":
      return (
        `installed version ${JSON.stringify(error.version)} is not a release version, so no release tag matches it. ` +
        `Run get-docs with --ref ${MAIN_BRANCH} to fetch the ${MAIN_BRANCH} branch docs, which can differ from this version.`
      );
    case "toolFailed":
      return toolFailureText(error.tool, error.failure);
    case "tarballLayout":
      return `unexpected tarball layout from ${error.url}: the archive does not hold exactly one top-level folder`;
    case "workspace":
      return `${error.operation} failed for ${error.path}: ${error.cause.message}`;
    case "treeCopy":
      return treeCopyErrorText(error.error);
  }
}

/** What a fetch does that has effects outside the output folder. */
export interface FetchDeps {
  /** Run one external tool and wait for its end, at most `TOOL_RUN_BOUND_MS`. */
  runTool(call: ToolCall): Result<undefined, ToolFailure>;
  /** Write one status line to stderr. */
  writeStatus(line: string): void;
}

function runStep(deps: FetchDeps, call: ToolCall): Result<undefined, DocsError> {
  const ran = deps.runTool(call);

  return ran.ok ? ran : err({ kind: "toolFailed", tool: call.name, failure: ran.error });
}

/** A sparse checkout of the docs paths of `reference` under `workspace`. Gives the checkout root. Effects: runs git. */
export function sparseCheckout(deps: FetchDeps, reference: DocsRef, workspace: string): Result<string, DocsError> {
  const clone = join(workspace, "repo");
  const ref = gitNameOf(reference);

  const steps: readonly ToolCall[] = [
    {
      name: "git clone",
      command: "git",
      args: ["clone", "--filter=blob:none", "--no-checkout", "--depth=1", "--branch", ref, `${REPO_URL}.git`, clone],
      cwd: workspace,
    },
    { name: "git sparse-checkout init", command: "git", args: ["sparse-checkout", "init", "--cone"], cwd: clone },
    { name: "git sparse-checkout set", command: "git", args: ["sparse-checkout", "set", DOCS_REPO_PATH, LLMS_REPO_PATH], cwd: clone },
    { name: "git checkout", command: "git", args: ["checkout", ref], cwd: clone },
  ];

  for (const step of steps) {
    const ran = runStep(deps, step);

    if (!ran.ok) {
      return ran;
    }
  }

  return ok(clone);
}

/**
 * The tarball of `reference`, downloaded with `curl` and unpacked with `tar` under `workspace`. Gives the one
 * top-level folder of the archive. Effects: runs curl and tar; writes the workspace.
 */
export function tarballCheckout(deps: FetchDeps, reference: DocsRef, workspace: string): Result<string, DocsError> {
  const archive = join(workspace, "repo.tar.gz");
  const unpacked = join(workspace, "unpacked");
  const url = tarballUrlOf(reference);
  const maxTime = String(TOOL_RUN_BOUND_MS / 1000);

  const download = runStep(deps, {
    name: "curl",
    command: "curl",
    args: ["--fail", "--silent", "--show-error", "--location", "--max-time", maxTime, "--output", archive, url],
    cwd: workspace,
  });

  if (!download.ok) {
    return download;
  }

  const created = attempt(() => mkdirSync(unpacked));

  if (!created.ok) {
    return err({ kind: "workspace", path: unpacked, operation: "create the unpack folder", cause: created.error });
  }

  const extracted = runStep(deps, { name: "tar", command: "tar", args: ["-xzf", archive, "-C", unpacked], cwd: workspace });

  if (!extracted.ok) {
    return extracted;
  }

  const tops = attempt(() => readdirSync(unpacked).map((name) => join(unpacked, name)));

  if (!tops.ok) {
    return err({ kind: "workspace", path: unpacked, operation: "read the unpack folder", cause: tops.error });
  }

  const [top, ...others] = tops.value;

  if (top === undefined || others.length > 0) {
    return err({ kind: "tarballLayout", url });
  }

  const stats = attempt(() => statSync(top, { throwIfNoEntry: false }));

  if (!stats.ok) {
    return err({ kind: "workspace", path: top, operation: "read the unpack folder", cause: stats.error });
  }

  return stats.value?.isDirectory() === true ? ok(top) : err({ kind: "tarballLayout", url });
}

/**
 * Fetch the docs of `reference` into `output` through the checkout in `workspace` (an empty folder that the caller
 * owns and removes): a sparse checkout, then the tarball when it fails. Gives the file count. Effects: runs the tools;
 * writes the workspace and the output folder. Until the copy is complete, an existing `output` is kept.
 */
export function fetchDocs(deps: FetchDeps, reference: DocsRef, workspace: string, output: string): Result<number, DocsError> {
  deps.writeStatus(`Fetching docs of ${gitNameOf(reference)} via git sparse checkout...`);

  const sparse = sparseCheckout(deps, reference, workspace);

  if (!sparse.ok) {
    deps.writeStatus(`Git sparse checkout failed (${docsErrorText(sparse.error)}), falling back to tarball...`);
  }

  const checkout = sparse.ok ? sparse : tarballCheckout(deps, reference, workspace);

  if (!checkout.ok) {
    return checkout;
  }

  const copied = assembleOutput(join(checkout.value, DOCS_REPO_PATH), join(checkout.value, LLMS_REPO_PATH), output);

  return copied.ok ? copied : err({ kind: "treeCopy", error: copied.error });
}
