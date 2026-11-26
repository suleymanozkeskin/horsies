// The glibc floor of a Linux glibc addon: the highest `GLIBC_` symbol version that the file needs is at most 2.17,
// the glibc of the napi cross toolchain. A file built against a newer glibc refuses to load on an older system.

import type { Result } from "../../horsies/src/wire/result.ts";
import { runTool } from "../public-api/tools.ts";
import type { ToolFailed } from "../public-api/tools.ts";

/** The highest glibc version that a published addon may need. */
export const GLIBC_FLOOR: readonly number[] = [2, 17];

/** The glibc versions that a file needs: the numbered ones, and each `GLIBC_` name that is not a number. */
export type GlibcNeeds = {
  readonly numbered: readonly (readonly number[])[];
  readonly unnumbered: readonly string[];
};

/** A version need line of `readelf --version-info`: `Name: GLIBC_2.17  Flags: none  Version: 3`. */
const GLIBC_NEED = /Name: GLIBC_(\S+)/gu;

/** A numbered glibc version: `2.17`, `2.2.5`. */
const NUMBERED_VERSION = /^\d+(?:\.\d+)*$/u;

/** The glibc versions that the output of `readelf --version-info --wide` names. Pure. */
export function glibcNeedsOf(readelfOutput: string): GlibcNeeds {
  const numbered: (readonly number[])[] = [];
  const unnumbered: string[] = [];

  for (const match of readelfOutput.matchAll(GLIBC_NEED)) {
    const version = match[1] ?? "";

    if (NUMBERED_VERSION.test(version)) {
      numbered.push(version.split(".").map(Number));
    } else {
      unnumbered.push(`GLIBC_${version}`);
    }
  }

  return { numbered, unnumbered };
}

/** -1, 0 or 1 as version `a` is below, equal to or above version `b`; a missing part counts as 0. Pure. */
export function compareVersions(a: readonly number[], b: readonly number[]): -1 | 0 | 1 {
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    const left = a[index] ?? 0;
    const right = b[index] ?? 0;

    if (left !== right) {
      return left < right ? -1 : 1;
    }
  }

  return 0;
}

/** The problems of a file's glibc needs against `floor`: none when the highest need is at most the floor. Pure. */
export function glibcFloorProblems(needs: GlibcNeeds, floor: readonly number[]): readonly string[] {
  const floorText = floor.join(".");
  const problems = needs.unnumbered.map((name) => `needs ${name}, which is not a numbered glibc version`);

  if (needs.numbered.length === 0) {
    return [...problems, "needs no GLIBC_ version: it is not a glibc build"];
  }

  const highest = needs.numbered.reduce((high, version) => (compareVersions(version, high) > 0 ? version : high));

  return compareVersions(highest, floor) > 0
    ? [...problems, `needs GLIBC_${highest.join(".")}, above the floor ${floorText}`]
    : problems;
}

/** Why the version needs of a file could not be read. */
export type ReadelfFailed =
  | { readonly kind: "ToolFailed"; readonly failure: ToolFailed }
  | { readonly kind: "ReadelfRefused"; readonly code: number; readonly output: string };

/** The output of `readelf --version-info --wide` for `file`. Effects: runs readelf (binutils). */
export function readelfVersionInfo(file: string): Result<string, ReadelfFailed> {
  const run = runTool("readelf", ["--version-info", "--wide", file], process.cwd());

  if (!run.ok) {
    return { ok: false, error: { kind: "ToolFailed", failure: run.error } };
  }

  return run.value.code === 0
    ? { ok: true, value: run.value.output }
    : { ok: false, error: { kind: "ReadelfRefused", code: run.value.code, output: run.value.output } };
}
