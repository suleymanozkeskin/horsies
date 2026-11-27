// The staging of the platform packages with `napi create-npm-dirs`: the folders of the list, a missing addon file,
// and a list that differs from the napi targets. Needs the workspace install (the napi CLI of the bridge).

import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, test } from "node:test";

import { binaryFileOf } from "./content.ts";
import { readHorsiesManifest } from "./manifest.ts";
import { readPlatformList } from "./platforms.ts";
import type { ReleasePlatform } from "./platforms.ts";
import { stageFailedText, stageLint, stagePlatforms } from "./stage.ts";

/** The committed list and manifest. */
function committed(): { readonly rows: readonly ReleasePlatform[]; readonly manifest: ReturnType<typeof readHorsiesManifest> } {
  const rows = readPlatformList();

  assert.ok(rows.ok);

  return { rows: rows.value, manifest: readHorsiesManifest() };
}

/** Run `check` with a new stage folder and a new addons folder, removed at the end. */
function withFolders(check: (stageDir: string, addonsDir: string) => void): void {
  const root = mkdtempSync(join(tmpdir(), "horsies-stage-test-"));

  try {
    mkdirSync(join(root, "addons"));
    check(join(root, "stage"), join(root, "addons"));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

describe("stagePlatforms", () => {
  test("writes one package folder per row, with its addon file, license and manifest", () => {
    const { rows, manifest } = committed();

    assert.ok(manifest.ok);

    withFolders((stageDir, addonsDir) => {
      for (const row of rows) {
        writeFileSync(join(addonsDir, binaryFileOf(row)), "stand-in");
      }

      const staged = stagePlatforms(stageDir, manifest.value, rows, addonsDir);

      assert.ok(staged.ok, staged.ok ? "" : stageFailedText(staged.error));
      assert.deepEqual(staged.value.map(([row]) => row.platform), rows.map((row) => row.platform));

      for (const [row, folder] of staged.value) {
        assert.ok(existsSync(join(folder, binaryFileOf(row))));
        assert.ok(existsSync(join(folder, "LICENSE")));
        assert.equal(JSON.parse(readFileSync(join(folder, "package.json"), "utf8")).version, manifest.value.version);
      }
    });
  });

  test("AddonMissing: a row whose addon file is not in the addons folder", () => {
    const { rows, manifest } = committed();

    assert.ok(manifest.ok);

    withFolders((stageDir, addonsDir) => {
      const staged = stagePlatforms(stageDir, manifest.value, rows, addonsDir);

      assert.deepEqual(staged.ok ? "staged" : staged.error, { kind: "AddonMissing", file: binaryFileOf(rows[0] ?? assert.fail("no row")) });
    });
  });

  test("PlatformDirsDiffer: a list with fewer platforms than the napi targets", () => {
    const { rows, manifest } = committed();

    assert.ok(manifest.ok);

    withFolders((stageDir, addonsDir) => {
      const staged = stagePlatforms(stageDir, manifest.value, rows.slice(0, 1), addonsDir);

      assert.equal(staged.ok ? "staged" : staged.error.kind, "PlatformDirsDiffer");
      assert.match(staged.ok ? "" : stageFailedText(staged.error), /napi.targets of bridge\/package.json give the platforms/u);
    });
  });
});

/** Run `check` with a new stage folder and a new lint package folder (an `eslint-plugin` stand-in), removed at the end. */
function withLintFolder(files: Readonly<Record<string, string>>, check: (stageDir: string, lintDir: string) => void): void {
  const root = mkdtempSync(join(tmpdir(), "horsies-stage-lint-test-"));

  try {
    const lintDir = join(root, "eslint-plugin");

    mkdirSync(join(lintDir, "src"), { recursive: true });

    for (const [file, text] of Object.entries(files)) {
      writeFileSync(join(lintDir, file), text);
    }

    check(join(root, "stage"), `${lintDir}/`);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

describe("stageLint", () => {
  test("staged: the committed lint package, built, with its manifest without private, the license and the README", () => {
    withFolders((stageDir) => {
      const staged = stageLint(stageDir);

      assert.ok(staged.ok, staged.ok ? "" : stageFailedText(staged.error));
      assert.equal(staged.value.kind, "staged");

      const folder = staged.value.kind === "staged" ? staged.value.folder : assert.fail("not staged");

      assert.ok(existsSync(join(folder, "dist", "index.js")));
      assert.ok(existsSync(join(folder, "dist", "index.d.ts")));
      assert.ok(existsSync(join(folder, "LICENSE")));
      assert.ok(existsSync(join(folder, "README.md")));
      assert.equal(JSON.parse(readFileSync(join(folder, "package.json"), "utf8")).private, undefined);
    });
  });

  test("absent: a folder with no package.json", () => {
    withFolders((stageDir) => {
      assert.deepEqual(stageLint(stageDir, join(stageDir, "no-such-folder", "")), { ok: true, value: { kind: "absent" } });
    });
  });

  test("LintManifestNotAnObject: a package.json that holds an array", () => {
    withLintFolder({ "package.json": "[]" }, (stageDir, lintDir) => {
      assert.deepEqual(stageLint(stageDir, lintDir), { ok: false, error: { kind: "LintManifestNotAnObject" } });
    });
  });

  test("BuildFailed: a folder with no tsconfig.build.json", () => {
    withLintFolder({ "package.json": '{"name":"@horsies/eslint-plugin"}' }, (stageDir, lintDir) => {
      const staged = stageLint(stageDir, lintDir);

      assert.equal(staged.ok ? "staged" : staged.error.kind, "BuildFailed");
      assert.match(staged.ok ? "" : stageFailedText(staged.error), /the build of @horsies\/eslint-plugin failed/u);
    });
  });
});
