// The pages of a docs root: every `.md` and `.mdx` file below the root folder, in path order, read within bounds.

import { type Dirent, lstatSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

import type { Result } from "../../horsies/src/wire/result.ts";
import { type PageKind, pageKindOf } from "./extract.ts";

/** A docs page: its path from the repository folder, its kind and its text. */
export type Page = { readonly path: string; readonly kind: PageKind; readonly text: string };

/** A docs root: no folder (the docs pages are held on the docs branch until the release), or its pages. */
export type RootRead =
  | { readonly state: "absent"; readonly root: string }
  | { readonly state: "read"; readonly root: string; readonly pages: readonly Page[] };

/** The bounds of the walk of one root. */
export type WalkBounds = { readonly folderDepth: number; readonly pages: number; readonly pageBytes: number };

/** Far above the docs of the clients: the Rust pages are 3 folders deep, 23 pages, at most about 40 KiB each. */
export const WALK_BOUNDS: WalkBounds = { folderDepth: 8, pages: 2_000, pageBytes: 1_048_576 };

/** Why the walk of a root stops. Paths are from the repository folder. */
export type WalkFailed =
  | { readonly kind: "ReadFailed"; readonly path: string; readonly cause: string }
  | { readonly kind: "NotAFolder"; readonly path: string }
  | { readonly kind: "TooDeep"; readonly path: string; readonly bound: number }
  | { readonly kind: "TooManyPages"; readonly root: string; readonly bound: number }
  | { readonly kind: "PageTooLarge"; readonly path: string; readonly bytes: number; readonly bound: number };

/** The value of `action`, or `ReadFailed` for the error that it throws (an error of the file system). */
export function fileSystemCall<T>(path: string, action: () => T): Result<T, WalkFailed> {
  try {
    return { ok: true, value: action() };
  } catch (error) {
    return { ok: false, error: { kind: "ReadFailed", path, cause: String(error) } };
  }
}

type Walk = { readonly repositoryDir: string; readonly root: string; readonly bounds: WalkBounds; readonly pages: Page[] };

function readPage(walk: Walk, path: string, kind: PageKind): Result<Page, WalkFailed> {
  const absolute = join(walk.repositoryDir, path);
  const status = fileSystemCall(path, () => lstatSync(absolute));

  if (!status.ok) {
    return status;
  }

  if (status.value.size > walk.bounds.pageBytes) {
    return { ok: false, error: { kind: "PageTooLarge", path, bytes: status.value.size, bound: walk.bounds.pageBytes } };
  }

  const text = fileSystemCall(path, () => readFileSync(absolute, "utf8"));

  return text.ok ? { ok: true, value: { path, kind, text: text.value } } : text;
}

type Entry = { readonly kind: "folder" } | { readonly kind: "page"; readonly pageKind: PageKind } | { readonly kind: "other" };

function entryOf(entry: Dirent): Entry {
  const pageKind = pageKindOf(entry.name);

  if (entry.isDirectory()) {
    return { kind: "folder" };
  }

  return entry.isFile() && pageKind !== "notAPage" ? { kind: "page", pageKind } : { kind: "other" };
}

/** Adds the page at `path` to `walk.pages`. */
function addPage(walk: Walk, path: string, kind: PageKind): Result<"added", WalkFailed> {
  if (walk.pages.length >= walk.bounds.pages) {
    return { ok: false, error: { kind: "TooManyPages", root: walk.root, bound: walk.bounds.pages } };
  }

  const page = readPage(walk, path, kind);

  if (!page.ok) {
    return page;
  }

  walk.pages.push(page.value);

  return { ok: true, value: "added" };
}

/** Adds the pages of the folder `folder` (a path from the repository folder) and of its folders to `walk.pages`. */
function walkFolder(walk: Walk, folder: string, depth: number): Result<"walked", WalkFailed> {
  if (depth > walk.bounds.folderDepth) {
    return { ok: false, error: { kind: "TooDeep", path: folder, bound: walk.bounds.folderDepth } };
  }

  const entries = fileSystemCall(folder, () => readdirSync(join(walk.repositoryDir, folder), { withFileTypes: true }));

  if (!entries.ok) {
    return entries;
  }

  for (const dirent of entries.value.toSorted((first, second) => (first.name < second.name ? -1 : 1))) {
    const path = `${folder}/${dirent.name}`;
    const entry = entryOf(dirent);
    let step: Result<"walked" | "added" | "skipped", WalkFailed>;

    switch (entry.kind) {
      case "folder":
        step = walkFolder(walk, path, depth + 1);
        break;
      case "page":
        step = addPage(walk, path, entry.pageKind);
        break;
      case "other":
        step = { ok: true, value: "skipped" };
        break;
    }

    if (!step.ok) {
      return step;
    }
  }

  return { ok: true, value: "walked" };
}

/**
 * The pages below `root` (a folder path from `repositoryDir`), in path order, or `absent` when no file or folder has
 * that path. Reads the file system only.
 */
export function readRoot(repositoryDir: string, root: string, bounds: WalkBounds = WALK_BOUNDS): Result<RootRead, WalkFailed> {
  const status = fileSystemCall(root, () => lstatSync(join(repositoryDir, root), { throwIfNoEntry: false }));

  if (!status.ok) {
    return status;
  }

  if (status.value === undefined) {
    return { ok: true, value: { state: "absent", root } };
  }

  if (!status.value.isDirectory()) {
    return { ok: false, error: { kind: "NotAFolder", path: root } };
  }

  const walk: Walk = { repositoryDir, root, bounds, pages: [] };
  const walked = walkFolder(walk, root, 0);

  return walked.ok ? { ok: true, value: { state: "read", root, pages: walk.pages } } : walked;
}

/** The text of a walk failure, for a message. */
export function walkFailedText(failure: WalkFailed): string {
  switch (failure.kind) {
    case "ReadFailed":
      return `${failure.path}: ${failure.cause}`;
    case "NotAFolder":
      return `${failure.path}: not a folder`;
    case "TooDeep":
      return `${failure.path}: more than ${failure.bound} folders below its docs root`;
    case "TooManyPages":
      return `${failure.root}: more than ${failure.bound} pages`;
    case "PageTooLarge":
      return `${failure.path}: ${failure.bytes} bytes, more than the bound of ${failure.bound}`;
  }
}
