import { describe, expect, test } from "bun:test";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  BUILTIN_CODE_KEY,
  DATA_REFUSED_KEY,
  DATA_REFUSED_SERIALIZATION_CAUSE,
  DATA_REFUSED_STRICT_JSON_CAUSE,
  DATA_REFUSED_VALUE_VALIDATION_CAUSE,
  ENGINE_KEY_PREFIXES,
  ERROR_DATA_DEPTH_LIMIT,
  MAX_VALUE_JSON_DEPTH,
  SUMMARY_OUTPUT_DEPTH_LIMIT,
  TASK_RESULT_ENVELOPE_KEY,
} from "../../../horsies/src/_catalog/codec_bounds.ts";
import { WorkflowStatus } from "../../../horsies/src/_catalog/status.ts";
import {
  BUILT_IN_TASK_CODES as CATALOG_BUILT_IN_TASK_CODES,
  RESERVED_NON_TASK_CODES as CATALOG_RESERVED_NON_TASK_CODES,
} from "../../../horsies/src/_catalog/task_code.ts";
import { isEngineKey } from "../../../horsies/src/codec/engine-keys.ts";
import { errorDataText } from "../../../horsies/src/codec/error-data.ts";
import { RESERVED_NON_TASK_CODES } from "../../../horsies/src/contract/public.ts";
import { BUILT_IN_TASK_CODES, WORKFLOW_STATUSES } from "../../../horsies/src/wire/public.ts";

const SOURCE_DIR = fileURLToPath(new URL("../../../horsies/src/", import.meta.url));

/** The most files read under one source folder. */
const MAX_SOURCE_FILES = 512;

/** The `.ts` files of the source folder `folder` (not recursive). */
function sourceFiles(folder: string): readonly string[] {
  const names = readdirSync(join(SOURCE_DIR, folder)).filter((name) => name.endsWith(".ts"));

  expect(names.length).toBeLessThanOrEqual(MAX_SOURCE_FILES);

  return names.map((name) => join(SOURCE_DIR, folder, name));
}

/** The lines of `file` that are code: a line comment and a doc comment line are left out. */
function codeLines(file: string): readonly string[] {
  return readFileSync(file, "utf8")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => !line.startsWith("//") && !line.startsWith("*") && !line.startsWith("/*"));
}

describe("the codec bounds and spellings of the generated catalog", () => {
  test("the codec holds no hand-written copy: no bounds file, and no text of the catalog in src/codec or src/wire", () => {
    expect(existsSync(join(SOURCE_DIR, "codec", "bounds.ts"))).toBe(false);

    const texts = [
      BUILTIN_CODE_KEY,
      TASK_RESULT_ENVELOPE_KEY,
      DATA_REFUSED_KEY,
      DATA_REFUSED_STRICT_JSON_CAUSE,
      DATA_REFUSED_VALUE_VALIDATION_CAUSE,
      DATA_REFUSED_SERIALIZATION_CAUSE,
      ...ENGINE_KEY_PREFIXES,
    ];

    const found = [...sourceFiles("codec"), ...sourceFiles("wire")].flatMap((file) =>
      codeLines(file)
        .filter((line) => texts.some((text) => line.includes(JSON.stringify(text))))
        .map((line) => `${file}: ${line}`),
    );

    expect(found).toEqual([]);
  });

  test("the depth limits of error data and of a summary output follow the value depth bound", () => {
    expect(ERROR_DATA_DEPTH_LIMIT).toBe(MAX_VALUE_JSON_DEPTH - 1);
    expect(SUMMARY_OUTPUT_DEPTH_LIMIT).toBe(MAX_VALUE_JSON_DEPTH - 2);
  });

  test("the engine key check takes the generated prefixes and tag key", () => {
    for (const prefix of ENGINE_KEY_PREFIXES) {
      expect(isEngineKey(`${prefix}x`)).toBe(true);
    }

    expect(isEngineKey(BUILTIN_CODE_KEY)).toBe(true);
    expect(isEngineKey("h_x")).toBe(false);
  });

  test("replaced error data has the generated key and causes", () => {
    const nonFinite = errorDataText({ value: Number.NaN });
    const tooLong = errorDataText({ count: 10n ** 4301n });

    expect(nonFinite.state === "replaced" ? String(nonFinite.text) : "stored").toBe(
      JSON.stringify({ [DATA_REFUSED_KEY]: DATA_REFUSED_STRICT_JSON_CAUSE }),
    );
    expect(tooLong.state === "replaced" ? String(tooLong.text) : "stored").toBe(
      JSON.stringify({ [DATA_REFUSED_KEY]: DATA_REFUSED_SERIALIZATION_CAUSE }),
    );
    expect(nonFinite).toMatchObject({
      state: "replaced",
      cause: DATA_REFUSED_STRICT_JSON_CAUSE,
    });
    expect(tooLong).toMatchObject({
      state: "replaced",
      cause: DATA_REFUSED_SERIALIZATION_CAUSE,
    });
  });

  test("the public code and status lists are the lists of the generated catalog", () => {
    expect(BUILT_IN_TASK_CODES).toBe(CATALOG_BUILT_IN_TASK_CODES);
    expect(RESERVED_NON_TASK_CODES).toBe(CATALOG_RESERVED_NON_TASK_CODES);
    expect(WORKFLOW_STATUSES).toEqual(Object.values(WorkflowStatus));
    expect(Object.isFrozen(WORKFLOW_STATUSES)).toBe(true);
  });
});
