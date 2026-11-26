import { describe, expect, test } from "bun:test";

import { compareReport, extractorConfig, reportFileName } from "./report.ts";

const entryPoint = { specifier: "horsies/zod", typesFile: "dist/zod.d.ts", reportName: "horsies-zod" };

describe("compareReport", () => {
  test("same: the generated text is the committed text", () => {
    expect(compareReport("a\nb\n", "a\nb\n")).toEqual({ state: "same" });
  });

  test("notCommitted: no committed file", () => {
    expect(compareReport(undefined, "a\n")).toEqual({ state: "notCommitted" });
  });

  test("changed: the removed and the added lines, each counted as often as it occurs", () => {
    expect(compareReport("a\nb\nb\nc\n", "a\nb\nd\nc\nd\n")).toEqual({
      state: "changed",
      removed: ["b"],
      added: ["d", "d"],
    });
  });

  test("changed: only the order of lines differs", () => {
    expect(compareReport("a\nb\n", "b\na\n")).toEqual({ state: "changed", removed: [], added: [] });
  });
});

describe("the API Extractor config of an entry point", () => {
  test("reads the entry point's declarations and writes its report file", () => {
    const config = extractorConfig({ folder: "/work/package", entryPoints: [entryPoint] }, entryPoint, "/work/reports");

    expect(config).toMatchObject({
      projectFolder: "/work/package",
      mainEntryPointFilePath: "<projectFolder>/dist/zod.d.ts",
      apiReport: { enabled: true, reportFileName: "horsies-zod", reportFolder: "/work/reports" },
    });
    expect(reportFileName(entryPoint)).toBe("horsies-zod.api.md");
  });

  test("a message other than a missing release tag, a forgotten export or a doc comment is an error", () => {
    expect(extractorConfig({ folder: "/p", entryPoints: [] }, entryPoint, "/r")).toMatchObject({
      messages: {
        compilerMessageReporting: { default: { logLevel: "error" } },
        extractorMessageReporting: {
          default: { logLevel: "error" },
          "ae-forgotten-export": { logLevel: "none", addToApiReportFile: true },
        },
      },
    });
  });
});
