// The glibc floor: the version needs of readelf's output and the verdict against the floor.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { compareVersions, GLIBC_FLOOR, glibcFloorProblems, glibcNeedsOf } from "./glibc-floor.ts";

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
    assert.deepEqual(glibcFloorProblems(glibcNeedsOf(readelfOutput(["2.2.5", "2.14", "2.17"])), GLIBC_FLOOR), []);
  });

  test("a need above the floor", () => {
    assert.deepEqual(glibcFloorProblems(glibcNeedsOf(readelfOutput(["2.17", "2.34"])), GLIBC_FLOOR), [
      "needs GLIBC_2.34, above the floor 2.17",
    ]);
  });

  test("a need that is not a numbered version", () => {
    assert.deepEqual(glibcFloorProblems(glibcNeedsOf(readelfOutput(["2.17", "PRIVATE"])), GLIBC_FLOOR), [
      "needs GLIBC_PRIVATE, which is not a numbered glibc version",
    ]);
  });

  test("a file that needs no glibc version", () => {
    assert.deepEqual(glibcFloorProblems(glibcNeedsOf(""), GLIBC_FLOOR), ["needs no GLIBC_ version: it is not a glibc build"]);
  });
});
