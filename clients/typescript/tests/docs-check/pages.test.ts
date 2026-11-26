import { afterAll, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { fileSystemCall, readRoot, WALK_BOUNDS, walkFailedText } from "./pages.ts";

const repository = realpathSync(mkdtempSync(join(tmpdir(), "horsies-docs-pages-test-")));

afterAll(() => {
  rmSync(repository, { recursive: true, force: true });
});

/** Writes `files` (paths from the repository folder) with their texts. */
function writeFiles(files: Readonly<Record<string, string>>): void {
  for (const [path, text] of Object.entries(files)) {
    mkdirSync(join(repository, path, ".."), { recursive: true });
    writeFileSync(join(repository, path), text);
  }
}

describe("readRoot", () => {
  test("absent: no file or folder has the root's path", () => {
    expect(readRoot(repository, "docs/typescript")).toEqual({ ok: true, value: { state: "absent", root: "docs/typescript" } });
  });

  test("read: the .md and .mdx pages of every folder, in path order, with their kinds; other files are no pages", () => {
    writeFiles({
      "read/b.md": "B",
      "read/a/z.mdx": "Z",
      "read/a/notes.txt": "not a page",
      "read/a.md": "A",
    });

    expect(readRoot(repository, "read")).toEqual({
      ok: true,
      value: {
        state: "read",
        root: "read",
        pages: [
          { path: "read/a/z.mdx", kind: "mdx", text: "Z" },
          { path: "read/a.md", kind: "md", text: "A" },
          { path: "read/b.md", kind: "md", text: "B" },
        ],
      },
    });
  });

  test("NotAFolder: the root is a file", () => {
    writeFiles({ "file-root.md": "A" });

    expect(readRoot(repository, "file-root.md")).toEqual({ ok: false, error: { kind: "NotAFolder", path: "file-root.md" } });
  });

  test("TooDeep: a folder deeper than the bound", () => {
    writeFiles({ "deep/one/two/page.md": "A" });

    expect(readRoot(repository, "deep", { ...WALK_BOUNDS, folderDepth: 1 })).toEqual({
      ok: false,
      error: { kind: "TooDeep", path: "deep/one/two", bound: 1 },
    });
  });

  test("TooManyPages: more pages than the bound", () => {
    writeFiles({ "many/a.md": "A", "many/b.md": "B", "many/c.md": "C" });

    expect(readRoot(repository, "many", { ...WALK_BOUNDS, pages: 2 })).toEqual({
      ok: false,
      error: { kind: "TooManyPages", root: "many", bound: 2 },
    });
  });

  test("PageTooLarge: a page of more bytes than the bound", () => {
    writeFiles({ "large/page.md": "12345" });

    expect(readRoot(repository, "large", { ...WALK_BOUNDS, pageBytes: 4 })).toEqual({
      ok: false,
      error: { kind: "PageTooLarge", path: "large/page.md", bytes: 5, bound: 4 },
    });
  });
});

describe("fileSystemCall", () => {
  test("ReadFailed: the error that a file system call throws, with its path", () => {
    const failed = fileSystemCall("docs/page.md", () => {
      throw new Error("EIO: i/o error");
    });

    expect(failed).toEqual({ ok: false, error: { kind: "ReadFailed", path: "docs/page.md", cause: "Error: EIO: i/o error" } });
    expect(!failed.ok && walkFailedText(failed.error)).toBe("docs/page.md: Error: EIO: i/o error");
  });
});
