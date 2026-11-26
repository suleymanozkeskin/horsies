// The rules of the type tests: the expectation header of a compile-fail case, the diagnostics that a compiler prints,
// and the verdict of a case in a lane.
//
// A compile-fail case (`fail/<area>-*.ts`) names its expected diagnostics on line 1:
//
//   // expect: TS2322 at 14
//   // expect: TS2322 at 14, TS2353 at 15
//   // expect: TS2322 at 14; floor: TS2345 at 14
//
// The list before `; floor:` holds in the current lane (TypeScript 7: `tsc` and `bun check`); the list after it holds
// in the floor lane (TypeScript 6.0). Without `; floor:`, one list holds in both lanes. A case passes in a lane when the
// compiler gives exactly the expected codes at the expected lines in its file, no more and no fewer. Message texts
// are not compared: they differ between compiler versions. `@ts-expect-error` alone is no compile-fail case: it passes
// on any error.

import type { Result } from "../../horsies/src/wire/result.ts";

/** The compiler lanes: TypeScript 7 (`tsc` and `bun check`), and TypeScript 6.0, the oldest supported compiler. */
export type Lane = "current" | "floor";

/** A diagnostic that a case expects: its code (`TS2322`) and its line (1-based). */
export type ExpectedDiagnostic = { readonly code: string; readonly line: number };

/** The expected diagnostics of a compile-fail case in each lane. */
export type CaseExpectation = Readonly<Record<Lane, readonly ExpectedDiagnostic[]>>;

export type HeaderRefused = { readonly kind: "NoHeader" } | { readonly kind: "BadItem"; readonly item: string };

const HEADER_PREFIX = "// expect: ";
const FLOOR_SEPARATOR = "; floor: ";
const ITEM = /^(TS\d+) at ([1-9]\d*)$/;

/** The items of a list (`TS2322 at 14, TS2353 at 15`); an empty list is one empty item, refused. */
function itemsOf(list: string): Result<readonly ExpectedDiagnostic[], HeaderRefused> {
  const items: ExpectedDiagnostic[] = [];

  for (const item of list.split(", ")) {
    const match = ITEM.exec(item);
    const code = match?.[1];
    const line = match?.[2];

    if (code === undefined || line === undefined) {
      return { ok: false, error: { kind: "BadItem", item } };
    }

    items.push({ code, line: Number(line) });
  }

  return { ok: true, value: items };
}

/** The expectation of a compile-fail case from line 1 of its source. Pure. */
export function expectationOf(source: string): Result<CaseExpectation, HeaderRefused> {
  const firstLine = source.split("\n", 1)[0] ?? "";

  if (!firstLine.startsWith(HEADER_PREFIX)) {
    return { ok: false, error: { kind: "NoHeader" } };
  }

  const [currentList = "", floorList] = firstLine.slice(HEADER_PREFIX.length).trimEnd().split(FLOOR_SEPARATOR, 2);
  const current = itemsOf(currentList);

  if (!current.ok) {
    return current;
  }

  const floor = floorList === undefined ? current : itemsOf(floorList);

  if (!floor.ok) {
    return floor;
  }

  return { ok: true, value: { current: current.value, floor: floor.value } };
}

/** The text of a refused header, for a message. */
export function headerRefusedText(refusal: HeaderRefused): string {
  switch (refusal.kind) {
    case "NoHeader":
      return `line 1 is not "${HEADER_PREFIX}TSnnnn at <line>"`;
    case "BadItem":
      return `the item "${refusal.item}" is not "TSnnnn at <line>"`;
  }
}

/** A diagnostic that a compiler printed: the file (absolute), the line (1-based), the code. */
export type Diagnostic = { readonly file: string; readonly line: number; readonly code: string };

/** The diagnostics of a compiler's output, and each error line that names no place (a config or a global error). */
export type CompilerReport = { readonly located: readonly Diagnostic[]; readonly unlocated: readonly string[] };

const LOCATED = /^(.+?)\((\d+),(\d+)\): error (TS\d+): /;
const UNLOCATED = /^error (TS\d+): |^(.+?): error (TS\d+): /;

/**
 * The diagnostics of the output of `tsc --pretty false` or `bun check --no-pretty`: one line per diagnostic, its file
 * relative to `cwd` (resolved with `resolveFile`). Continuation lines of a message are skipped. Pure.
 */
export function diagnosticsOf(output: string, resolveFile: (file: string) => string): CompilerReport {
  const located: Diagnostic[] = [];
  const unlocated: string[] = [];

  for (const line of output.split("\n")) {
    const match = LOCATED.exec(line);
    const [, file, lineText, , code] = match ?? [];

    if (file !== undefined && lineText !== undefined && code !== undefined) {
      located.push({ file: resolveFile(file), line: Number(lineText), code });
      continue;
    }

    if (UNLOCATED.test(line)) {
      unlocated.push(line);
    }
  }

  return { located, unlocated };
}

function diagnosticKey(diagnostic: ExpectedDiagnostic): string {
  return `${diagnostic.code} at ${diagnostic.line}`;
}

/** The verdict of a case in one compiler run: `passed`, or the expected and the actual diagnostics. */
export type CaseVerdict =
  | { readonly state: "passed" }
  | { readonly state: "failed"; readonly expected: readonly string[]; readonly actual: readonly string[] };

/**
 * The verdict of a case: its diagnostics (those of its file) must be exactly `expected`, as a list of codes at lines
 * in any order. A compile-pass case expects none. Pure.
 */
export function caseVerdict(expected: readonly ExpectedDiagnostic[], actual: readonly ExpectedDiagnostic[]): CaseVerdict {
  const expectedKeys = expected.map(diagnosticKey).toSorted();
  const actualKeys = actual.map(diagnosticKey).toSorted();

  const same =
    expectedKeys.length === actualKeys.length && expectedKeys.every((key, index) => key === actualKeys[index]);

  return same ? { state: "passed" } : { state: "failed", expected: expectedKeys, actual: actualKeys };
}
