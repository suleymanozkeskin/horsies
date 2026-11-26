// The content checks of the tarballs: one case per problem of each check.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  exportTargets,
  horsiesFileProblems,
  horsiesManifestProblems,
  platformExpectationOf,
  platformFileProblems,
  platformManifestProblems,
  sameJson,
  SKILL_FILES,
} from "./content.ts";
import type { ReleasePlatform } from "./platforms.ts";

const EXPORTS = {
  ".": { types: "./dist/index.d.ts", default: "./dist/index.js" },
  "./zod": { types: "./dist/zod.d.ts", default: "./dist/zod.js" },
  "./package.json": "./package.json",
};

const PACKED = ["package.json", "LICENSE", "dist/index.d.ts", "dist/index.js", "dist/zod.d.ts", "dist/zod.js"];

const GLIBC_ROW: ReleasePlatform = {
  name: "linux-x86_64",
  release: "publish",
  runner: "ubuntu-latest",
  rustTarget: "x86_64-unknown-linux-gnu",
  cLibrary: "glibc",
  platform: "linux-x64-gnu",
  cpu: "x64",
  build: { kind: "glibc-2.17-toolchain" },
  install: { kind: "container", nodeVariant: "trixie-slim", bunVariant: "debian" },
};

describe("exportTargets", () => {
  test("names every target of the map, without ./", () => {
    assert.deepEqual(exportTargets(EXPORTS), ["dist/index.d.ts", "dist/index.js", "dist/zod.d.ts", "dist/zod.js", "package.json"]);
  });
});

describe("horsiesFileProblems", () => {
  const targets = exportTargets(EXPORTS);

  test("passes the packed files of the entry points", () => {
    assert.deepEqual(horsiesFileProblems(PACKED, targets, { kind: "notWritten" }), []);
  });

  test("a target of the exports map that is not packed", () => {
    assert.deepEqual(horsiesFileProblems(PACKED.filter((file) => file !== "dist/zod.d.ts"), targets, { kind: "notWritten" }), [
      "the exports map names dist/zod.d.ts, which is not packed",
    ]);
  });

  test("an addon file, a source file, the dev-only entry and build info", () => {
    const files = [...PACKED, "horsies.node", "src/index.ts", "dist/native/dev-addon.js", "tsconfig.tsbuildinfo"];

    assert.deepEqual(horsiesFileProblems(files, targets, { kind: "notWritten" }), [
      "holds the addon file horsies.node",
      "holds the source file src/index.ts",
      "holds the dev-only entry dist/native/dev-addon.js",
      "holds the build info tsconfig.tsbuildinfo",
    ]);
  });

  test("a skill file when the source has no skill folder", () => {
    assert.deepEqual(horsiesFileProblems([...PACKED, ".agents/skills/horsies/SKILL.md"], targets, { kind: "notWritten" }), [
      "holds the skill file SKILL.md, and the source has no skill folder",
    ]);
  });

  test("the five skill files of the source, packed", () => {
    const skills = SKILL_FILES.map((name) => `.agents/skills/horsies/${name}`);

    assert.deepEqual(horsiesFileProblems([...PACKED, ...skills], targets, { kind: "written", files: SKILL_FILES }), []);
  });

  test("a skill file of the source that is missing or not packed", () => {
    const written = { kind: "written", files: ["SKILL.md", "tasks.md", "workflows.md", "configs.md"] } as const;

    assert.deepEqual(horsiesFileProblems([...PACKED, ".agents/skills/horsies/SKILL.md"], targets, written), [
      "the skill folder of the source has no monitoring.md",
      "the skill file tasks.md is not packed",
      "the skill file workflows.md is not packed",
      "the skill file configs.md is not packed",
    ]);
  });
});

describe("sameJson", () => {
  test("ignores the order of object members, not of array elements", () => {
    assert.ok(sameJson({ a: 1, b: [1, 2] }, { b: [1, 2], a: 1 }));
    assert.ok(!sameJson([1, 2], [2, 1]));
    assert.ok(!sameJson({ a: 1 }, undefined));
  });
});

describe("horsiesManifestProblems", () => {
  const optional = { "@horsies/native-linux-x64-gnu": "0.8.1" };
  const valid = { version: "0.8.1", optionalDependencies: optional, publishConfig: { access: "public" } };

  test("passes the version, the optional dependencies and public access", () => {
    assert.deepEqual(horsiesManifestProblems(valid, "0.8.1", optional), []);
  });

  test("another version, private, other optional dependencies, no public access", () => {
    const manifest = { version: "0.8.0", private: true, optionalDependencies: {} };

    assert.deepEqual(horsiesManifestProblems(manifest, "0.8.1", optional), [
      'the packed version is "0.8.0", not 0.8.1',
      "the packed manifest has the field private, so npm refuses to publish it",
      'the optional dependencies are {}, not {"@horsies/native-linux-x64-gnu":"0.8.1"}',
      "publishConfig.access is not public: a scoped package publishes as restricted",
    ]);
  });
});

describe("platformExpectationOf", () => {
  test("a glibc row is linux with libc glibc", () => {
    assert.deepEqual(platformExpectationOf(GLIBC_ROW, "0.8.1"), {
      packageName: "@horsies/native-linux-x64-gnu",
      version: "0.8.1",
      os: "linux",
      cpu: "x64",
      libc: { kind: "field", value: "glibc" },
      glibcFloor: { kind: "floor", version: [2, 17] },
      binary: "horsies.linux-x64-gnu.node",
    });
  });

  test("a manylinux build has the floor of its image", () => {
    const row: ReleasePlatform = {
      ...GLIBC_ROW,
      build: { kind: "manylinux-container", image: "quay.io/pypa/manylinux_2_28_aarch64:2026.11.21-1", glibcFloor: [2, 28] },
    };

    assert.deepEqual(platformExpectationOf(row, "0.8.1").glibcFloor, { kind: "floor", version: [2, 28] });
  });

  test("a musl row has libc musl; a macOS row is darwin with no libc field", () => {
    assert.deepEqual(platformExpectationOf({ ...GLIBC_ROW, cLibrary: "musl", platform: "linux-x64-musl", build: { kind: "alpine-container", nodeVariant: "alpine3.23" } }, "0.8.1").libc, {
      kind: "field",
      value: "musl",
    });

    const macos = platformExpectationOf({ ...GLIBC_ROW, cLibrary: "libsystem", platform: "darwin-arm64", cpu: "arm64", build: { kind: "runner" } }, "0.8.1");

    assert.equal(macos.os, "darwin");
    assert.deepEqual(macos.libc, { kind: "noField" });
    assert.deepEqual(macos.glibcFloor, { kind: "noFloor" });
  });
});

describe("platformFileProblems and platformManifestProblems", () => {
  const expected = platformExpectationOf(GLIBC_ROW, "0.8.1");

  const manifest = {
    name: "@horsies/native-linux-x64-gnu",
    version: "0.8.1",
    os: ["linux"],
    cpu: ["x64"],
    libc: ["glibc"],
    main: "horsies.linux-x64-gnu.node",
    files: ["horsies.linux-x64-gnu.node"],
    publishConfig: { access: "public" },
  };

  test("pass the four files and the manifest of the platform", () => {
    assert.deepEqual(platformFileProblems(["package.json", "README.md", "LICENSE", "horsies.linux-x64-gnu.node"], expected), []);
    assert.deepEqual(platformManifestProblems(manifest, expected), []);
  });

  test("a missing binary and an extra file", () => {
    assert.deepEqual(platformFileProblems(["package.json", "README.md", "LICENSE", "index.js"], expected), [
      "@horsies/native-linux-x64-gnu does not hold horsies.linux-x64-gnu.node",
      "@horsies/native-linux-x64-gnu holds index.js, which it must not",
    ]);
  });

  test("a manifest field of another platform", () => {
    assert.deepEqual(platformManifestProblems({ ...manifest, libc: ["musl"] }, expected), [
      '@horsies/native-linux-x64-gnu: libc is ["musl"], not ["glibc"]',
    ]);
  });

  test("a libc field on a platform that has none", () => {
    const macos = platformExpectationOf({ ...GLIBC_ROW, cLibrary: "libsystem", platform: "darwin-arm64", cpu: "arm64", build: { kind: "runner" } }, "0.8.1");

    const darwinManifest = {
      ...manifest,
      name: "@horsies/native-darwin-arm64",
      os: ["darwin"],
      cpu: ["arm64"],
      main: macos.binary,
      files: [macos.binary],
    };

    assert.deepEqual(platformManifestProblems(darwinManifest, macos), ['@horsies/native-darwin-arm64: libc is ["glibc"], not undefined']);
  });
});
