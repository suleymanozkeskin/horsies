// `get-docs`: the ref, the package version, the kept pages, the copy into the output folder and the two fetch
// strategies. The fetch runs with a stand-in for git, curl and tar: no network.

import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { after, describe, test } from "node:test";

import { assembleOutput, isKeptPage, MAX_DOCS_DEPTH, pageSectionOf, stagingPathOf } from "../../../horsies/src/tools/docs-tree.ts";
import {
  DOCS_REPO_PATH,
  docsErrorText,
  type FetchDeps,
  fetchDocs,
  LLMS_REPO_PATH,
  packageVersionOf,
  resolveDocsRef,
  tarballUrlOf,
  type ToolCall,
} from "../../../horsies/src/tools/docs.ts";
import { err, ok } from "../../../horsies/src/wire/result.ts";

const folders: string[] = [];

function newFolder(): string {
  const folder = mkdtempSync(join(tmpdir(), "horsies-docs-test-"));

  folders.push(folder);

  return folder;
}

after(() => {
  for (const folder of folders) {
    rmSync(folder, { recursive: true, force: true });
  }
});

function writeFile(path: string, text: string): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text);
}

/** A docs tree of a checkout under `root`: a TypeScript page, a shared page, a Python page, a Rust page, llms.txt. */
function writeCheckout(root: string): void {
  const docs = join(root, DOCS_REPO_PATH);

  writeFile(join(docs, "typescript/tasks/errors.md"), "---\ntitle: E\n---\n");
  writeFile(join(docs, "tasks/retry-policy.md"), "---\nsection: shared\n---\n");
  writeFile(join(docs, "tasks/defining-tasks.md"), "---\nsection: python\n---\n");
  writeFile(join(docs, "rust/tasks/errors.md"), "---\ntitle: E\n---\n");
  writeFile(join(root, LLMS_REPO_PATH), "typescript pages");
}

describe("resolveDocsRef", () => {
  test("a release version asks for its tag", () => {
    assert.deepEqual(resolveDocsRef("0.8.1", "notGiven"), { ok: true, value: { kind: "releaseTag", tag: "v0.8.1" } });
    assert.equal(tarballUrlOf({ kind: "releaseTag", tag: "v0.8.1" }), "https://github.com/suleymanozkeskin/horsies/archive/refs/tags/v0.8.1.tar.gz");
  });

  test("a pre-release version has no tag unless main is asked for", () => {
    for (const version of ["0.9.0-rc.1", "0.8", "0.8.0.1", "v0.8.0"]) {
      const refused = resolveDocsRef(version, "notGiven");

      assert.ok(!refused.ok && refused.error.kind === "versionHasNoTag", version);
      assert.match(docsErrorText(refused.error), /is not a release version.*Run get-docs with --ref main/u);
      assert.deepEqual(resolveDocsRef(version, "main"), { ok: true, value: { kind: "mainBranch" } });
    }
  });
});

describe("packageVersionOf", () => {
  test("the version of the package's own package.json", () => {
    const version = packageVersionOf(join(import.meta.dirname, "../../../horsies"));

    assert.ok(version.ok);
    assert.match(version.value, /^\d+\.\d+\.\d+/u);
  });

  test("a missing manifest, a manifest that is not JSON, and one with no version", () => {
    const folder = newFolder();

    const missing = packageVersionOf(folder);

    assert.ok(!missing.ok);
    assert.match(docsErrorText(missing.error), /cannot be read from .*package\.json: ENOENT/u);
    writeFileSync(join(folder, "package.json"), "{");

    const notJson = packageVersionOf(folder);

    assert.ok(!notJson.ok);
    assert.match(docsErrorText(notJson.error), /it is not JSON/u);
    writeFileSync(join(folder, "package.json"), '{"name":"horsies"}');

    const noVersion = packageVersionOf(folder);

    assert.ok(!noVersion.ok);
    assert.match(docsErrorText(noVersion.error), /has no text version/u);
  });
});

describe("the kept pages", () => {
  test("the section is read from the frontmatter only", () => {
    assert.equal(pageSectionOf("---\ntitle: A\nsection: shared\n---\n# A\n"), "shared");
    assert.equal(pageSectionOf("---\nsection: python\n---\n"), "python");
    assert.equal(pageSectionOf("---\nsection: site\n---\n"), "site");
    assert.equal(pageSectionOf("---\ntitle: A\n---\nsection: shared\n"), "notDeclared");
    assert.equal(pageSectionOf("# no frontmatter\nsection: shared\n"), "notDeclared");
  });

  test("TypeScript pages and shared pages are kept; Python, Rust and site pages are not", () => {
    assert.ok(isKeptPage("typescript/tasks/errors.md", "---\ntitle: E\n---\n"));
    assert.ok(isKeptPage("concepts/architecture.md", "---\nsection: shared\n---\n"));
    assert.ok(!isKeptPage("tasks/defining-tasks.md", "---\nsection: python\n---\n"));
    assert.ok(!isKeptPage("rust/tasks/errors.md", "---\ntitle: E\n---\n"));
    assert.ok(!isKeptPage("changelog.md", "---\nsection: site\n---\n"));
    assert.ok(!isKeptPage("typescripts.md", "---\nsection: python\n---\n"));
  });
});

describe("assembleOutput", () => {
  test("the output holds the kept pages and the TypeScript llms file, and an old output is replaced", () => {
    const root = newFolder();
    const output = join(newFolder(), ".horsies-docs");

    writeCheckout(root);
    writeFile(join(output, "stale.md"), "old");

    assert.deepEqual(assembleOutput(join(root, DOCS_REPO_PATH), join(root, LLMS_REPO_PATH), output), { ok: true, value: 3 });
    assert.ok(existsSync(join(output, "typescript/tasks/errors.md")));
    assert.ok(existsSync(join(output, "tasks/retry-policy.md")));
    assert.ok(!existsSync(join(output, "tasks/defining-tasks.md")));
    assert.ok(!existsSync(join(output, "rust/tasks/errors.md")));
    assert.ok(!existsSync(join(output, "stale.md")));
    assert.equal(readFileSync(join(output, "llms.txt"), "utf8"), "typescript pages");
    assert.ok(!existsSync(stagingPathOf(output)));
  });

  test("a missing docs tree keeps the old output", () => {
    const folder = newFolder();
    const output = join(folder, ".horsies-docs");

    writeFile(join(output, "kept.md"), "old");

    const refused = assembleOutput(join(folder, "missing"), join(folder, "llms.txt"), output);

    assert.deepEqual(refused, { ok: false, error: { kind: "sourceMissing", path: join(folder, "missing") } });
    assert.ok(existsSync(join(output, "kept.md")));
  });

  test("a tree with no kept page and no llms file writes nothing", () => {
    const folder = newFolder();

    writeFile(join(folder, "docs/tasks/defining-tasks.md"), "---\nsection: python\n---\n");

    const refused = assembleOutput(join(folder, "docs"), join(folder, "llms.txt"), join(folder, "out"));

    assert.deepEqual(refused, { ok: false, error: { kind: "noFiles" } });
    assert.ok(!existsSync(join(folder, "out")));
  });

  test("a tree that nests deeper than the bound is refused and keeps the old output", () => {
    const folder = newFolder();
    const output = join(folder, "out");
    const deep = join(folder, "docs", ...Array.from({ length: MAX_DOCS_DEPTH + 1 }, (_, index) => `level-${index}`));

    writeFile(join(deep, "page.md"), "---\nsection: shared\n---\n");
    writeFile(join(output, "kept.md"), "old");

    const refused = assembleOutput(join(folder, "docs"), join(folder, "llms.txt"), output);

    assert.ok(!refused.ok && refused.error.kind === "treeBoundExceeded");
    assert.match(docsErrorText({ kind: "treeCopy", error: refused.error }), /nests deeper than 8 folders/u);
    assert.ok(existsSync(join(output, "kept.md")));
  });
});

/** A stand-in for git, curl and tar: `answer` gives each run its result; each call is recorded. */
function fakeTools(answer: (call: ToolCall) => ReturnType<FetchDeps["runTool"]>): FetchDeps & { calls: ToolCall[]; status: string[] } {
  const calls: ToolCall[] = [];
  const status: string[] = [];

  return {
    calls,
    status,
    runTool: (call) => {
      calls.push(call);

      return answer(call);
    },
    writeStatus: (line) => {
      status.push(line);
    },
  };
}

describe("fetchDocs", () => {
  const reference = { kind: "releaseTag", tag: "v0.8.1" } as const;

  test("a sparse checkout that succeeds gives the kept pages", () => {
    const workspace = newFolder();
    const output = join(newFolder(), "docs");

    const tools = fakeTools((call) => {
      if (call.name === "git checkout") {
        writeCheckout(call.cwd);
      }

      return ok(undefined);
    });

    assert.deepEqual(fetchDocs(tools, reference, workspace, output), { ok: true, value: 3 });
    assert.deepEqual(
      tools.calls.map((call) => call.name),
      ["git clone", "git sparse-checkout init", "git sparse-checkout set", "git checkout"],
    );
    assert.ok(tools.calls[0]?.args.includes("v0.8.1"));
    assert.deepEqual(tools.status, ["Fetching docs of v0.8.1 via git sparse checkout..."]);
  });

  test("a failed git run falls back to the tarball through curl and tar", () => {
    const workspace = newFolder();
    const output = join(newFolder(), "docs");

    const tools = fakeTools((call) => {
      switch (call.name) {
        case "git clone":
          return err({ kind: "exited", status: 128, stderr: "fatal: no network" });
        case "tar":
          writeCheckout(join(call.args[3] ?? "", "horsies-0.8.1"));

          return ok(undefined);
        default:
          return ok(undefined);
      }
    });

    assert.deepEqual(fetchDocs(tools, reference, workspace, output), { ok: true, value: 3 });
    assert.deepEqual(
      tools.calls.map((call) => call.name),
      ["git clone", "curl", "tar"],
    );
    assert.ok(tools.calls[1]?.args.includes("https://github.com/suleymanozkeskin/horsies/archive/refs/tags/v0.8.1.tar.gz"));
    assert.equal(tools.status[1], "Git sparse checkout failed (git clone failed: exit status 128: fatal: no network), falling back to tarball...");
  });

  test("a failed download is the error of the fetch", () => {
    const tools = fakeTools(() => err({ kind: "timedOut", boundMs: 60_000 }));
    const fetched = fetchDocs(tools, reference, newFolder(), join(newFolder(), "docs"));

    assert.ok(!fetched.ok);
    assert.equal(docsErrorText(fetched.error), "curl failed: timed out after 60s");
  });

  test("an archive without exactly one top-level folder is refused", () => {
    const tools = fakeTools((call) => {
      if (call.name === "git clone") {
        return err({ kind: "notStarted", cause: { code: "ENOENT", message: "spawnSync git ENOENT" } });
      }

      if (call.name === "tar") {
        writeFile(join(call.args[3] ?? "", "a", "x"), "x");
        writeFile(join(call.args[3] ?? "", "b", "x"), "x");
      }

      return ok(undefined);
    });

    const fetched = fetchDocs(tools, reference, newFolder(), join(newFolder(), "docs"));

    assert.deepEqual(fetched, { ok: false, error: { kind: "tarballLayout", url: tarballUrlOf(reference) } });
    assert.ok(!fetched.ok);
    assert.match(docsErrorText(fetched.error), /does not hold exactly one top-level folder/u);
  });
});
