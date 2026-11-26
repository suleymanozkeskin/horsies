import { afterAll, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { BuiltPackage } from "./package.ts";

import { apiReport, compareReport, extractorConfig, reportFileName } from "./report.ts";

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

  test("a forgotten export is a warning that the gate reads; any other message is an error", () => {
    expect(extractorConfig({ folder: "/p", entryPoints: [] }, entryPoint, "/r")).toMatchObject({
      messages: {
        compilerMessageReporting: { default: { logLevel: "error" } },
        extractorMessageReporting: {
          default: { logLevel: "error" },
          "ae-forgotten-export": { logLevel: "warning", addToApiReportFile: false },
        },
      },
    });
  });
});

const work = realpathSync(mkdtempSync(join(tmpdir(), "horsies-report-test-")));

afterAll(() => {
  rmSync(work, { recursive: true, force: true });
});

/** A package in `work/<name>` with `dist/index.d.ts` and `dist/hidden.d.ts`. */
function declarationPackage(name: string, index: string): BuiltPackage {
  const folder = join(work, name);

  mkdirSync(join(folder, "dist"), { recursive: true });
  writeFileSync(join(folder, "package.json"), JSON.stringify({ name: "probe", version: "0.0.0" }));
  writeFileSync(join(folder, "dist", "hidden.d.ts"), "export type Hidden = { readonly id: string };\n");
  writeFileSync(join(folder, "dist", "index.d.ts"), index);

  return { folder, entryPoints: [{ specifier: "probe", typesFile: "dist/index.d.ts", reportName: "probe" }] };
}

describe("apiReport", () => {
  test("the report of an entry point whose surface names only exported types", () => {
    const built = declarationPackage("exported", 'export type { Hidden } from "./hidden.ts";\n');
    const entry = built.entryPoints[0];
    const report = entry === undefined ? undefined : apiReport(built, entry, built.folder);

    expect(report?.ok && report.value.forgotten).toEqual([]);
    expect(report?.ok && report.value.text).toContain("export type Hidden = {");
  });

  test("each type that a public declaration names and the entry point does not export", () => {
    const index = 'import type { Hidden } from "./hidden.ts";\nexport declare function find(id: string): Hidden;\n';
    const built = declarationPackage("forgotten", index);
    const entry = built.entryPoints[0];
    const report = entry === undefined ? undefined : apiReport(built, entry, built.folder);

    expect(report?.ok && report.value.forgotten.map(({ file, line }) => `${file}:${line}`)).toEqual(["dist/index.d.ts:2"]);
    expect(report?.ok && report.value.forgotten[0]?.text).toContain('"Hidden"');
  });

  test("ExtractorErrors: an error-level message of API Extractor (an internal name without an underscore)", () => {
    const built = declarationPackage("refused", "/** @internal */\nexport declare const count: number;\n");
    const entry = built.entryPoints[0];
    const report = entry === undefined ? undefined : apiReport(built, entry, built.folder);

    expect(report?.ok === false && report.error.kind).toBe("ExtractorErrors");
  });

  test("ExtractorThrew: a package folder without package.json", () => {
    const built = declarationPackage("no-manifest", "export declare const count: number;\n");
    const entry = built.entryPoints[0];

    rmSync(join(built.folder, "package.json"));

    const report = entry === undefined ? undefined : apiReport(built, entry, built.folder);

    expect(report?.ok === false && report.error.kind).toBe("ExtractorThrew");
  });
});
