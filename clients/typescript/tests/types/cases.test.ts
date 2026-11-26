import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { caseVerdict, diagnosticsOf, expectationOf, headerRefusedText } from "./cases.ts";

describe("expectationOf", () => {
  test("one list holds in both lanes", () => {
    expect(expectationOf("// expect: TS2322 at 14\nconst a = 1;\n")).toEqual({
      ok: true,
      value: { current: [{ code: "TS2322", line: 14 }], floor: [{ code: "TS2322", line: 14 }] },
    });
  });

  test("several diagnostics, and a floor list of their own", () => {
    expect(expectationOf("// expect: TS2322 at 14, TS2353 at 15; floor: TS2345 at 14\n")).toEqual({
      ok: true,
      value: {
        current: [
          { code: "TS2322", line: 14 },
          { code: "TS2353", line: 15 },
        ],
        floor: [{ code: "TS2345", line: 14 }],
      },
    });
  });

  test("NoHeader: line 1 is not an expect line", () => {
    const refused = expectationOf("// A case.\n// expect: TS2322 at 14\n");

    expect(refused).toEqual({ ok: false, error: { kind: "NoHeader" } });
    expect(!refused.ok && headerRefusedText(refused.error)).toContain("line 1");
  });

  test("BadItem: an item that is not TSnnnn at <line>", () => {
    for (const [header, item] of [
      ["// expect: TS2322", "TS2322"],
      ["// expect: TS2322 at 0", "TS2322 at 0"],
      ["// expect: ", ""],
      ["// expect: TS2322 at 1; floor: 2345 at 1", "2345 at 1"],
    ] as const) {
      expect(expectationOf(`${header}\n`)).toEqual({ ok: false, error: { kind: "BadItem", item } });
    }
  });

  test("every committed compile-fail case has a valid header", () => {
    const folder = fileURLToPath(new URL("fail/", import.meta.url));

    for (const name of readdirSync(folder).filter((file) => file.endsWith(".ts"))) {
      expect(expectationOf(readFileSync(`${folder}${name}`, "utf8")).ok).toBe(true);
    }
  });
});

describe("diagnosticsOf", () => {
  test("one diagnostic per error line, with its file resolved; message continuation lines are skipped", () => {
    const output = [
      "tests/types/fail/a.ts(6,24): error TS2555: Expected at least 1 arguments, but got 0.",
      "tests/types/fail/b.ts(10,14): error TS2375: Type '{ note: undefined; }' is not assignable.",
      "  Types of property 'note' are incompatible.",
      "Found 2 errors in 2 files",
    ].join("\n");

    expect(diagnosticsOf(output, (file) => `/w/${file}`)).toEqual({
      located: [
        { file: "/w/tests/types/fail/a.ts", line: 6, code: "TS2555" },
        { file: "/w/tests/types/fail/b.ts", line: 10, code: "TS2375" },
      ],
      unlocated: [],
    });
  });

  test("an error that names no place is unlocated", () => {
    const output = "tsconfig.json: error TS5011: The common source directory\nerror TS6053: File 'x.ts' not found.";

    expect(diagnosticsOf(output, (file) => file).unlocated).toEqual([
      "tsconfig.json: error TS5011: The common source directory",
      "error TS6053: File 'x.ts' not found.",
    ]);
  });
});

describe("caseVerdict", () => {
  const expected = [{ code: "TS2322", line: 14 }];

  test("passed: exactly the expected codes at the expected lines", () => {
    expect(caseVerdict(expected, [{ code: "TS2322", line: 14 }])).toEqual({ state: "passed" });
    expect(caseVerdict([], [])).toEqual({ state: "passed" });
  });

  test("failed: another code, another line, a missing or an extra diagnostic", () => {
    for (const actual of [
      [{ code: "TS2345", line: 14 }],
      [{ code: "TS2322", line: 15 }],
      [],
      [
        { code: "TS2322", line: 14 },
        { code: "TS2322", line: 20 },
      ],
    ]) {
      expect(caseVerdict(expected, actual).state).toBe("failed");
    }
  });

  test("a compile-pass case fails on any diagnostic", () => {
    expect(caseVerdict([], [{ code: "TS2322", line: 1 }])).toEqual({
      state: "failed",
      expected: [],
      actual: ["TS2322 at 1"],
    });
  });
});
