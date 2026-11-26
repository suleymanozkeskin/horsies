// The checks of an unpacked platform tarball that read its files: a published build passes, a build with a test-only
// export does not. The rows here are not glibc rows, so no readelf runs.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, test } from "node:test";

import { platformProblems } from "./checks.ts";
import { platformExpectationOf } from "./content.ts";
import type { PackedTarball } from "./npm-pack.ts";
import type { ReleasePlatform } from "./platforms.ts";

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
