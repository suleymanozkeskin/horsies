// The checks of an unpacked platform tarball that read its files: a published build passes, a build with a test-only
// export does not. The rows here are not glibc rows, so no readelf runs.

import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, test } from "node:test";

import { lintProblems, platformProblems } from "./checks.ts";
import { runCommand } from "./command.ts";
import { LINT_PACKAGE_NAME, platformExpectationOf } from "./content.ts";
import { readHorsiesManifest } from "./manifest.ts";
import { npmPack } from "./npm-pack.ts";
import type { PackedTarball } from "./npm-pack.ts";
import type { ReleasePlatform } from "./platforms.ts";
import { stageLint } from "./stage.ts";

const MACOS_ROW: ReleasePlatform = {
  name: "macos-arm64",
  release: "publish",
  runner: "macos-15",
  rustTarget: "aarch64-apple-darwin",
  cLibrary: "libsystem",
  platform: "darwin-arm64",
  cpu: "arm64",
  build: { kind: "runner" },
  install: { kind: "runner" },
};

const EXPECTED = platformExpectationOf(MACOS_ROW, "0.8.1");

const TARBALL: PackedTarball = {
  name: EXPECTED.packageName,
  version: "0.8.1",
  filename: "horsies-native-darwin-arm64-0.8.1.tgz",
  integrity: "sha512-abc",
  shasum: "0123",
  files: ["package.json", "README.md", "LICENSE", EXPECTED.binary],
};

/** Run `check` with an unpacked package folder that holds `binaryText` as the addon file. */
function withPackage(binaryText: string, check: (packageDir: string) => void): void {
  const packageDir = mkdtempSync(join(tmpdir(), "horsies-checks-test-"));

  try {
    writeFileSync(join(packageDir, EXPECTED.binary), binaryText);

    writeFileSync(
      join(packageDir, "package.json"),
      JSON.stringify({
        name: EXPECTED.packageName,
        version: "0.8.1",
        os: ["darwin"],
        cpu: ["arm64"],
        main: EXPECTED.binary,
        files: [EXPECTED.binary],
        publishConfig: { access: "public" },
      }),
    );

    check(packageDir);
  } finally {
    rmSync(packageDir, { recursive: true, force: true });
  }
}

describe("platformProblems", () => {
  test("a published build passes", () => {
    withPackage("coreState buildCoreHandle", (packageDir) => {
      assert.deepEqual(platformProblems(TARBALL, packageDir, EXPECTED), []);
    });
  });

  test("a build with the test-only export fails", () => {
    withPackage("coreState probeCallback", (packageDir) => {
      assert.deepEqual(platformProblems(TARBALL, packageDir, EXPECTED), [
        "@horsies/native-darwin-arm64: the addon holds the test-only export probeCallback; it was built with test-probes",
      ]);
    });
  });
});

/** Run `check` with the committed lint package built, packed and unpacked, in folders that are removed at the end. */
function withPackedLint(check: (tarball: PackedTarball, packageDir: string, version: string) => void): void {
  const root = mkdtempSync(join(tmpdir(), "horsies-lint-pack-test-"));

  try {
    const manifest = readHorsiesManifest();
    const staged = stageLint(join(root, "stage"));

    assert.ok(manifest.ok);
    assert.ok(staged.ok && staged.value.kind === "staged");

    const packed = npmPack(staged.value.folder, root);

    assert.ok(packed.ok);

    const unpackDir = join(root, "unpacked");

    mkdirSync(unpackDir);

    const untar = runCommand("tar", ["-xzf", join(root, packed.value.filename), "-C", unpackDir], unpackDir);

    assert.ok(untar.ok && untar.value.code === 0);

    check(packed.value, join(unpackDir, "package"), manifest.value.version);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

describe("lintProblems", () => {
  test("the committed lint package, packed, passes at the version of horsies", () => {
    withPackedLint((tarball, packageDir, version) => {
      assert.deepEqual(lintProblems(tarball, packageDir, version), []);
    });
  });

  test("a packed file that the exports map names and the tarball lacks fails", () => {
    withPackedLint((tarball, packageDir, version) => {
      const files = tarball.files.filter((file) => file !== "dist/index.js");

      assert.deepEqual(lintProblems({ ...tarball, files }, packageDir, version), [`${LINT_PACKAGE_NAME} does not hold dist/index.js`]);
    });
  });

  test("a version that differs from the version of horsies fails", () => {
    withPackedLint((tarball, packageDir) => {
      assert.deepEqual(lintProblems(tarball, packageDir, "9.9.9"), [
        `${LINT_PACKAGE_NAME}: the packed version is "${JSON.parse(readFileSync(join(packageDir, "package.json"), "utf8")).version}", not 9.9.9`,
      ]);
    });
  });
});
