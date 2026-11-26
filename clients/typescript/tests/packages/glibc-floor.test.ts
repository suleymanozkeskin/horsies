// The glibc floor: the version needs of readelf's output and the verdict against the floor.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { compareVersions, NAPI_CROSS_GLIBC_FLOOR, glibcFloorProblems, glibcNeedsOf } from "./glibc-floor.ts";

/** The version needs section of `readelf --version-info --wide` for a file that needs `versions`. */
function readelfOutput(versions: readonly string[]): string {
  return [
    "Version needs section '.gnu.version_r' contains 1 entry:",
    " Addr: 0x0000000000001234  Offset: 0x001234  Link: 6 (.dynstr)",
    `  000000: Version: 1  File: libc.so.6  Cnt: ${versions.length}`,
    ...versions.map((version, index) => `  0x${(16 * (index + 1)).toString(16).padStart(4, "0")}:   Name: GLIBC_${version}  Flags: none  Version: ${index + 2}`),
  ].join("\n");
}

describe("glibcNeedsOf", () => {
  test("reads the numbered versions and the names that are not numbers", () => {
    assert.deepEqual(glibcNeedsOf(readelfOutput(["2.2.5", "2.17", "PRIVATE"])), {
      numbered: [
        [2, 2, 5],
        [2, 17],
      ],
      unnumbered: ["GLIBC_PRIVATE"],
    });
  });
});

describe("compareVersions", () => {
  test("compares part by part; a missing part is 0", () => {
    assert.equal(compareVersions([2, 17], [2, 17, 0]), 0);
    assert.equal(compareVersions([2, 2, 5], [2, 17]), -1);
    assert.equal(compareVersions([2, 18], [2, 17]), 1);
  });
});

describe("glibcFloorProblems", () => {
  test("passes needs at or below the floor", () => {
    assert.deepEqual(glibcFloorProblems(glibcNeedsOf(readelfOutput(["2.2.5", "2.14", "2.17"])), NAPI_CROSS_GLIBC_FLOOR), []);
  });

  test("a need above the floor", () => {
    assert.deepEqual(glibcFloorProblems(glibcNeedsOf(readelfOutput(["2.17", "2.34"])), NAPI_CROSS_GLIBC_FLOOR), [
      "needs GLIBC_2.34, above the floor 2.17",
    ]);
  });

  test("a need that is not a numbered version", () => {
    assert.deepEqual(glibcFloorProblems(glibcNeedsOf(readelfOutput(["2.17", "PRIVATE"])), NAPI_CROSS_GLIBC_FLOOR), [
      "needs GLIBC_PRIVATE, which is not a numbered glibc version",
    ]);
  });

  test("a manylinux_2_28 floor passes 2.28 and refuses 2.29", () => {
    assert.deepEqual(glibcFloorProblems(glibcNeedsOf(readelfOutput(["2.17", "2.28"])), [2, 28]), []);
    assert.deepEqual(glibcFloorProblems(glibcNeedsOf(readelfOutput(["2.29"])), [2, 28]), ["needs GLIBC_2.29, above the floor 2.28"]);
  });

  test("a file that needs no glibc version", () => {
    assert.deepEqual(glibcFloorProblems(glibcNeedsOf(""), NAPI_CROSS_GLIBC_FLOOR), ["needs no GLIBC_ version: it is not a glibc build"]);
  });
});
