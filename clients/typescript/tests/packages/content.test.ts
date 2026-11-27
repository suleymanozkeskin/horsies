// The content checks of the tarballs: one case per problem of each check.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  binFileProblems,
  entryPointSpecifiers,
  exportTargets,
  horsiesFileProblems,
  horsiesManifestProblems,
  LINT_PACKAGE_NAME,
  lintFileProblems,
  lintManifestProblems,
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

const PACKED_SKILLS = SKILL_FILES.map((name) => `.agents/skills/horsies/${name}`);
const FOUND = { kind: "found", files: SKILL_FILES } as const;

describe("entryPointSpecifiers", () => {
  test("names the import specifier of each entry point, and not package.json", () => {
    const exportsMap = { ...EXPORTS, "./rerun/pg": { types: "./dist/rerun/pg.d.ts", default: "./dist/rerun/pg.js" } };

    assert.deepEqual(entryPointSpecifiers(exportsMap), ["horsies", "horsies/zod", "horsies/rerun/pg"]);
  });

  test("names none for a manifest with no exports map", () => {
    assert.deepEqual(entryPointSpecifiers(undefined), []);
  });
});

describe("horsiesFileProblems", () => {
  const targets = exportTargets(EXPORTS);

  test("passes the packed files of the entry points and the five skill files", () => {
    assert.deepEqual(horsiesFileProblems([...PACKED, ...PACKED_SKILLS], targets, FOUND), []);
  });

  test("a target of the exports map that is not packed", () => {
    const files = [...PACKED.filter((file) => file !== "dist/zod.d.ts"), ...PACKED_SKILLS];

    assert.deepEqual(horsiesFileProblems(files, targets, FOUND), ["the exports map names dist/zod.d.ts, which is not packed"]);
  });

  test("an addon file, a source file, the dev-only entry and build info", () => {
    const files = [...PACKED, ...PACKED_SKILLS, "horsies.node", "src/index.ts", "dist/native/dev-addon.js", "tsconfig.tsbuildinfo"];

    assert.deepEqual(horsiesFileProblems(files, targets, FOUND), [
      "holds the addon file horsies.node",
      "holds the source file src/index.ts",
      "holds the dev-only entry dist/native/dev-addon.js",
      "holds the build info tsconfig.tsbuildinfo",
    ]);
  });

  test("a source with no skill folder fails, also when the tarball holds the skill files", () => {
    assert.deepEqual(horsiesFileProblems([...PACKED, ...PACKED_SKILLS], targets, { kind: "missing" }), [
      "the source has no skill folder .agents/skills/horsies",
    ]);
  });

  test("a source with no skill folder and a tarball with none: every skill file is not packed", () => {
    assert.deepEqual(horsiesFileProblems(PACKED, targets, { kind: "missing" }), [
      "the source has no skill folder .agents/skills/horsies",
      ...SKILL_FILES.map((name) => `the skill file ${name} is not packed`),
    ]);
  });

  test("a skill file of the source that is missing, and skill files that are not packed", () => {
    const found = { kind: "found", files: ["SKILL.md", "tasks.md", "workflows.md", "configs.md"] } as const;

    assert.deepEqual(horsiesFileProblems([...PACKED, ".agents/skills/horsies/SKILL.md"], targets, found), [
      "the skill folder of the source has no monitoring.md",
      "the skill file configs.md is not packed",
      "the skill file monitoring.md is not packed",
      "the skill file tasks.md is not packed",
      "the skill file workflows.md is not packed",
    ]);
  });

  test("a file of the skill folder besides the five that is not packed", () => {
    const found = { kind: "found", files: [...SKILL_FILES, "extra.md"] } as const;

    assert.deepEqual(horsiesFileProblems([...PACKED, ...PACKED_SKILLS], targets, found), ["the skill file extra.md is not packed"]);
  });
});

describe("binFileProblems", () => {
  const bin = { horsies: "./dist/tools/bin.js" };

  test("passes a bin horsies whose file is packed", () => {
    assert.deepEqual(binFileProblems([...PACKED, "dist/tools/bin.js"], bin), []);
  });

  test("a bin file that is not packed", () => {
    assert.deepEqual(binFileProblems(PACKED, bin), ["the bin horsies names dist/tools/bin.js, which is not packed"]);
  });

  test("a manifest with no bin horsies", () => {
    assert.deepEqual(binFileProblems(PACKED, undefined), ["the manifest has no bin horsies"]);
    assert.deepEqual(binFileProblems(PACKED, { other: "./dist/tools/bin.js" }), ["the manifest has no bin horsies"]);
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

describe("lintFileProblems", () => {
  const targets = exportTargets({ ".": { types: "./dist/index.d.ts", default: "./dist/index.js" }, "./package.json": "./package.json" });
  const PACKED_LINT = ["package.json", "README.md", "LICENSE", "dist/index.js", "dist/index.d.ts"];

  test("passes the packed files", () => {
    assert.deepEqual(lintFileProblems(PACKED_LINT, targets), []);
  });

  test("a target of the exports map, the README or the license that is not packed", () => {
    assert.deepEqual(lintFileProblems(["package.json"], targets), [
      `${LINT_PACKAGE_NAME} does not hold README.md`,
      `${LINT_PACKAGE_NAME} does not hold LICENSE`,
      `${LINT_PACKAGE_NAME} does not hold dist/index.d.ts`,
      `${LINT_PACKAGE_NAME} does not hold dist/index.js`,
    ]);
  });

  test("a source file, a test file and build info", () => {
    const files = [...PACKED_LINT, "src/index.ts", "tests/rule.test.ts", "tsconfig.tsbuildinfo"];

    assert.deepEqual(lintFileProblems(files, targets), [
      `${LINT_PACKAGE_NAME} holds the source file src/index.ts`,
      `${LINT_PACKAGE_NAME} holds the source file tests/rule.test.ts`,
      `${LINT_PACKAGE_NAME} holds the build info tsconfig.tsbuildinfo`,
    ]);
  });
});

describe("lintManifestProblems", () => {
  const GOOD = { name: LINT_PACKAGE_NAME, version: "0.8.1", publishConfig: { access: "public" } };

  test("passes a publishable manifest at the version of the release", () => {
    assert.deepEqual(lintManifestProblems(GOOD, "0.8.1"), []);
  });

  test("a wrong name, a wrong version, the field private and a restricted access", () => {
    const manifest = { name: "eslint-plugin", version: "0.8.0", private: true, publishConfig: { access: "restricted" } };

    assert.deepEqual(lintManifestProblems(manifest, "0.8.1"), [
      `${LINT_PACKAGE_NAME}: the packed name is "eslint-plugin"`,
      `${LINT_PACKAGE_NAME}: the packed version is "0.8.0", not 0.8.1`,
      `${LINT_PACKAGE_NAME}: the packed manifest has the field private, so npm refuses to publish it`,
      `${LINT_PACKAGE_NAME}: publishConfig.access is not public: a scoped package publishes as restricted`,
    ]);
  });
});
