// The matrices of the package workflow: one case per build form and install form, and the runtime versions.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { buildEntryOf, installEntriesOf, runtimeVersionsOf, webPageOf } from "./matrix.ts";
import type { ReleasePlatform } from "./platforms.ts";

const VERSIONS = { node: "26.13.1", bun: "1.4.4" };

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

const MUSL_ROW: ReleasePlatform = {
  ...GLIBC_ROW,
  name: "linux-musl-x86_64",
  release: "hold",
  rustTarget: "x86_64-unknown-linux-musl",
  cLibrary: "musl",
  platform: "linux-x64-musl",
  build: { kind: "alpine-container", nodeVariant: "alpine3.23" },
  install: { kind: "container", nodeVariant: "alpine3.23", bunVariant: "alpine" },
};

const MACOS_ROW: ReleasePlatform = {
  ...GLIBC_ROW,
  name: "macos-arm64",
  runner: "macos-15",
  rustTarget: "aarch64-apple-darwin",
  cLibrary: "libsystem",
  platform: "darwin-arm64",
  cpu: "arm64",
  build: { kind: "runner" },
  install: { kind: "runner" },
};

describe("buildEntryOf", () => {
  test("a glibc row builds with the toolchain and checks in its images", () => {
    assert.deepEqual(buildEntryOf(GLIBC_ROW, VERSIONS), {
      name: "linux-x86_64",
      runner: "ubuntu-latest",
      rust_target: "x86_64-unknown-linux-gnu",
      platform: "linux-x64-gnu",
      build: "glibc-2.17-toolchain",
      check: "container",
      node_image: "node:26.13.1-trixie-slim",
      bun_image: "oven/bun:1.4.4-debian",
    });
  });

  test("an Alpine row names its build image", () => {
    assert.equal(buildEntryOf(MUSL_ROW, VERSIONS).build_image, "node:26.13.1-alpine3.23");
  });

  test("a manylinux row names its manylinux image", () => {
    const row: ReleasePlatform = {
      ...GLIBC_ROW,
      build: { kind: "manylinux-container", image: "quay.io/pypa/manylinux_2_28_aarch64:2026.11.21-1", glibcFloor: [2, 28] },
    };

    assert.equal(buildEntryOf(row, VERSIONS).build_image, "quay.io/pypa/manylinux_2_28_aarch64:2026.11.21-1");
    assert.equal(buildEntryOf(row, VERSIONS).build, "manylinux-container");
  });

  test("a runner row names no image", () => {
    assert.deepEqual(buildEntryOf(MACOS_ROW, VERSIONS), {
      name: "macos-arm64",
      runner: "macos-15",
      rust_target: "aarch64-apple-darwin",
      platform: "darwin-arm64",
      build: "runner",
      check: "runner",
    });
  });
});

describe("installEntriesOf", () => {
  test("a container row gives npm on the Node image and Bun on the Bun image", () => {
    assert.deepEqual(
      installEntriesOf(MUSL_ROW, VERSIONS).map((entry) => [entry.installer, entry.runtime, entry.image, entry.release, entry.package]),
      [
        ["npm", "node", "node:26.13.1-alpine3.23", "hold", "@horsies/native-linux-x64-musl"],
        ["bun", "bun", "oven/bun:1.4.4-alpine", "hold", "@horsies/native-linux-x64-musl"],
      ],
    );
  });

  test("a runner row gives npm and Bun on the runner, with no image", () => {
    assert.deepEqual(
      installEntriesOf(MACOS_ROW, VERSIONS).map((entry) => [entry.installer, entry.place, "image" in entry]),
      [
        ["npm", "runner", false],
        ["bun", "runner", false],
      ],
    );
  });
});

describe("webPageOf", () => {
  test("run: the Linux x86_64 glibc row", () => {
    assert.equal(webPageOf(GLIBC_ROW), "run");
    assert.deepEqual(installEntriesOf(GLIBC_ROW, VERSIONS).map((entry) => entry.web_page), ["run", "run"]);
  });

  test("skip: the musl row and the macOS row", () => {
    assert.equal(webPageOf(MUSL_ROW), "skip");
    assert.equal(webPageOf(MACOS_ROW), "skip");
  });
});

describe("runtimeVersionsOf", () => {
  test("reads both exact versions", () => {
    assert.deepEqual(runtimeVersionsOf(["--node-version", "26.13.1", "--bun-version", "1.4.4"]), VERSIONS);
  });

  test("refuses a missing version or one that is not exact", () => {
    assert.ok("refused" in runtimeVersionsOf(["--node-version", "26.13.1"]));
    assert.ok("refused" in runtimeVersionsOf(["--node-version", "26", "--bun-version", "1.4.4"]));
  });

  test("refuses an unknown argument", () => {
    assert.ok("refused" in runtimeVersionsOf(["--node", "26.13.1"]));
  });
});

describe("the selection list of the install entries", () => {
  test("each entry of a row carries the packages that the caller passes", () => {
    const entries = installEntriesOf(MUSL_ROW, VERSIONS, "@horsies/native-a,@horsies/native-b");

    assert.deepEqual(entries.map((entry) => entry.all_packages), ["@horsies/native-a,@horsies/native-b", "@horsies/native-a,@horsies/native-b"]);
  });
});
