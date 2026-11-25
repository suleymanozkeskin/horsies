// Every path to the codec reader, and the check that runs in front of it.
//
// The reader takes a `JsonText` only, and it does not check the core's strict read rule again. Two functions make a
// `JsonText`: `jsonTextCheckedByCore` (a path on which the core's strict check ran) and `writtenJsonText` (the codec
// writer). This test lists each module of the package that names one of them. A new module that names one fails this
// test until its row is added here, with the check that runs before the reader on that path; that module's own tests
// prove the check.

import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { SOURCE_DIR } from "./support/files.ts";

type ReaderPath = {
  readonly module: string;
  readonly maker: "jsonTextCheckedByCore" | "writtenJsonText";
  readonly checkInFront: string;
};

/** The module that defines the two makers. */
const DEFINING_MODULE = "codec/json-text.ts";

const MAKERS = ["jsonTextCheckedByCore", "writtenJsonText"] as const;

const READER_PATHS: readonly ReaderPath[] = [];

function sourceModules(): readonly string[] {
  return readdirSync(SOURCE_DIR, { recursive: true, encoding: "utf8" })
    .filter((name) => name.endsWith(".ts"))
    .map((name) => name.split("\\").join("/"))
    .toSorted();
}

function pathKey(module: string, maker: string): string {
  return `${module} ${maker}`;
}

describe("paths to the codec reader", () => {
  test("each module that makes a JsonText is listed with the check in front of the reader", () => {
    const found: string[] = [];

    for (const module of sourceModules()) {
      if (module === DEFINING_MODULE) {
        continue;
      }

      const text = readFileSync(`${SOURCE_DIR}${module}`, "utf8");

      for (const maker of MAKERS) {
        if (new RegExp(`\\b${maker}\\b`).test(text)) {
          found.push(pathKey(module, maker));
        }
      }
    }

    const listed = READER_PATHS.map((path) => pathKey(path.module, path.maker)).toSorted();

    assert.deepEqual(found.toSorted(), listed);
  });

  test("each listed path names its check", () => {
    for (const path of READER_PATHS) {
      assert.notEqual(path.checkInFront.length, 0, path.module);
    }
  });
});
