// The copy of a fetched docs tree into the output folder of `horsies get-docs`: only the pages that a TypeScript user
// reads are kept, every page under `typescript/` (the API of the client) and every page with `section: shared`
// (behaviour and guarantees of the core). The Python and Rust API pages and the site pages are dropped. The output
// folder is replaced only after the new copy is complete.

import { copyFileSync, mkdirSync, readdirSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { basename, dirname, join, relative, sep } from "node:path";

import { err, ok, type Result } from "../wire/result.ts";
import { attempt, type IoCause, readBounded } from "./files.ts";

/** The folder of the TypeScript API pages in the docs tree. */
export const TYPESCRIPT_PAGES_FOLDER = "typescript";

/** The deepest folder nesting of the docs tree that the copy walks. */
export const MAX_DOCS_DEPTH = 8;

/** The most files of the docs tree that the copy reads. */
export const MAX_DOCS_FILES = 2000;

/** The largest docs page that the copy reads, in bytes. */
export const MAX_PAGE_BYTES: number = 2 * 1024 * 1024;

/** The most frontmatter lines that the section read looks at. */
export const MAX_FRONTMATTER_LINES = 64;

/** The section that a page declares in its frontmatter. `notDeclared`: no `section:` line (a client API page). */
export type PageSection = "shared" | "python" | "site" | "notDeclared";

/** The bound of the docs tree that a copy passed. */
export type TreeBound =
  | { readonly kind: "depth"; readonly folders: number }
  | { readonly kind: "fileCount"; readonly files: number }
  | { readonly kind: "pageSize"; readonly bytes: number };

/** The step of the copy whose system call failed. */
export type TreeOperation = "read the folder" | "read" | "write" | "copy" | "create the folder" | "remove the old folder" | "move the new copy into place";

/** Why the copy wrote no output folder. A failure before the final swap leaves the old output folder as it was. */
export type TreeCopyError =
  | { readonly kind: "sourceMissing"; readonly path: string }
  | { readonly kind: "treeBoundExceeded"; readonly path: string; readonly bound: TreeBound }
  | { readonly kind: "io"; readonly path: string; readonly operation: TreeOperation; readonly cause: IoCause }
  | { readonly kind: "noFiles" };

/** The `section:` of the frontmatter of `text`: the lines between a first line `---` and the next `---`. Pure. */
export function pageSectionOf(text: string): PageSection {
  const lines = text.split("\n").map((line) => line.trimEnd());

  if (lines[0] !== "---") {
    return "notDeclared";
  }

  for (const line of lines.slice(1, 1 + MAX_FRONTMATTER_LINES)) {
    if (line === "---") {
      return "notDeclared";
    }

    if (line.startsWith("section:")) {
      const value = line.slice("section:".length).trim();

      return value === "shared" || value === "python" || value === "site" ? value : "notDeclared";
    }
  }

  return "notDeclared";
}

/** Whether a TypeScript user reads the page at `relativePath` (a path inside the docs tree). Pure. */
export function isKeptPage(relativePath: string, text: string): boolean {
  if (relativePath.split(sep)[0] === TYPESCRIPT_PAGES_FOLDER) {
    return true;
  }

  switch (pageSectionOf(text)) {
    case "shared":
      return true;
    case "python":
    case "site":
    case "notDeclared":
      return false;
  }
}

/** The text of a passed tree bound, for a message. Pure. */
export function treeBoundText(bound: TreeBound): string {
  switch (bound.kind) {
    case "depth":
      return `nests deeper than ${bound.folders} folders`;
    case "fileCount":
      return `holds more than ${bound.files} files`;
    case "pageSize":
      return `is larger than ${bound.bytes} bytes`;
  }
}

/** The text of a failed copy, for a message. Pure. */
export function treeCopyErrorText(error: TreeCopyError): string {
  switch (error.kind) {
    case "sourceMissing":
      return `docs source directory not found: ${error.path}`;
    case "treeBoundExceeded":
      return `the docs tree at ${error.path} ${treeBoundText(error.bound)}`;
    case "io":
      return `${error.operation} failed for ${error.path}: ${error.cause.message}`;
    case "noFiles":
      return "no files were copied to the output directory";
  }
}

/** `<output>.tmp` beside `output`. Pure. */
export function stagingPathOf(output: string): string {
  return join(dirname(output), `${basename(output)}.tmp`);
}

/** One walk of the docs tree: the files seen and the files copied. */
type TreeWalk = { readonly root: string; readonly target: string; seen: number; copied: number };

function ioError(path: string, operation: TreeOperation, cause: IoCause): TreeCopyError {
  return { kind: "io", path, operation, cause };
}

function isDirectory(path: string): Result<boolean, TreeCopyError> {
  const stats = attempt(() => statSync(path, { throwIfNoEntry: false }));

  return stats.ok ? ok(stats.value?.isDirectory() === true) : err(ioError(path, "read the folder", stats.error));
}

function removeIfPresent(path: string): Result<undefined, TreeCopyError> {
  const removed = attempt(() => rmSync(path, { recursive: true, force: true }));

  return removed.ok ? ok(undefined) : err(ioError(path, "remove the old folder", removed.error));
}

/**
 * Copy the kept pages of `docs` and the file `llms` (when it is a file) into `output`: first into `<output>.tmp`,
 * then the old `output` is removed and the new copy takes its name. Gives the file count. Effects: reads the docs
 * tree; writes beside `output`.
 */
export function assembleOutput(docs: string, llms: string, output: string): Result<number, TreeCopyError> {
  const isFolder = isDirectory(docs);

  if (!isFolder.ok || !isFolder.value) {
    return isFolder.ok ? err({ kind: "sourceMissing", path: docs }) : isFolder;
  }

  const staging = stagingPathOf(output);
  const cleared = removeIfPresent(staging);
  const walk: TreeWalk = { root: docs, target: staging, seen: 0, copied: 0 };
  const walked = cleared.ok ? copyKeptPages(walk, docs, 0) : cleared;

  if (!walked.ok) {
    return walked;
  }

  const llmsCopied = copyLlmsFile(llms, staging);

  if (!llmsCopied.ok) {
    return llmsCopied;
  }

  const count = walk.copied + (llmsCopied.value ? 1 : 0);

  if (count === 0) {
    return err({ kind: "noFiles" });
  }

  const removed = removeIfPresent(output);

  if (!removed.ok) {
    return removed;
  }

  const moved = attempt(() => renameSync(staging, output));

  return moved.ok ? ok(count) : err(ioError(staging, "move the new copy into place", moved.error));
}

/** Copy `llms` to `<staging>/llms.txt` when it is a file. Gives whether it was copied. */
function copyLlmsFile(llms: string, staging: string): Result<boolean, TreeCopyError> {
  const stats = attempt(() => statSync(llms, { throwIfNoEntry: false }));

  if (!stats.ok) {
    return err(ioError(llms, "copy", stats.error));
  }

  if (stats.value === undefined || !stats.value.isFile()) {
    return ok(false);
  }

  const created = attempt(() => mkdirSync(staging, { recursive: true }));

  if (!created.ok) {
    return err(ioError(staging, "create the folder", created.error));
  }

  const copied = attempt(() => copyFileSync(llms, join(staging, "llms.txt")));

  return copied.ok ? ok(true) : err(ioError(llms, "copy", copied.error));
}

/**
 * Copy the kept pages under `folder` into the same relative paths under the walk's target, at most `MAX_DOCS_DEPTH`
 * folders deep and at most `MAX_DOCS_FILES` files read. Effects: reads the tree; writes the target.
 */
function copyKeptPages(walk: TreeWalk, folder: string, depth: number): Result<undefined, TreeCopyError> {
  if (depth > MAX_DOCS_DEPTH) {
    return err({ kind: "treeBoundExceeded", path: folder, bound: { kind: "depth", folders: MAX_DOCS_DEPTH } });
  }

  const names = attempt(() => readdirSync(folder));

  if (!names.ok) {
    return err(ioError(folder, "read the folder", names.error));
  }

  for (const name of names.value.toSorted()) {
    const path = join(folder, name);
    const nested = isDirectory(path);
    const step = !nested.ok ? nested : nested.value ? copyKeptPages(walk, path, depth + 1) : copyOnePage(walk, path);

    if (!step.ok) {
      return step;
    }
  }

  return ok(undefined);
}

/** Read the file at `path` and copy it into the walk's target when it is a kept page. */
function copyOnePage(walk: TreeWalk, path: string): Result<undefined, TreeCopyError> {
  walk.seen += 1;

  if (walk.seen > MAX_DOCS_FILES) {
    return err({ kind: "treeBoundExceeded", path, bound: { kind: "fileCount", files: MAX_DOCS_FILES } });
  }

  const bytes = readBounded(path, MAX_PAGE_BYTES);

  if (!bytes.ok) {
    switch (bytes.error.kind) {
      case "tooLarge":
        return err({ kind: "treeBoundExceeded", path, bound: { kind: "pageSize", bytes: MAX_PAGE_BYTES } });
      case "io":
        return err(ioError(path, "read", bytes.error.cause));
    }
  }

  const relativePath = relative(walk.root, path);

  if (!isKeptPage(relativePath, bytes.value.toString("utf8"))) {
    return ok(undefined);
  }

  const destination = join(walk.target, relativePath);
  const created = attempt(() => mkdirSync(dirname(destination), { recursive: true }));

  if (!created.ok) {
    return err(ioError(dirname(destination), "create the folder", created.error));
  }

  const written = attempt(() => writeFileSync(destination, bytes.value));

  if (!written.ok) {
    return err(ioError(destination, "write", written.error));
  }

  walk.copied += 1;

  return ok(undefined);
}
