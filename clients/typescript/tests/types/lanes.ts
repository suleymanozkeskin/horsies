// The compilers of the two lanes and one compile of a project: the current lane (TypeScript 7: `tsc` and `bun check`)
// and the floor lane (TypeScript 6.0, the oldest compiler that the published types support).

import { resolve } from "node:path";

import type { Result } from "../../horsies/src/wire/result.ts";
import { CURRENT_COMPILER, FLOOR_COMPILER, runTool, type ToolFailed } from "../public-api/tools.ts";
import { type Diagnostic, diagnosticsOf, type Lane } from "./cases.ts";

/** A compiler of a lane. */
export type CompilerName = "tsc 7" | "bun check" | "tsc 6.0";

export type LaneCompiler = { readonly name: CompilerName; readonly lane: Lane };

/** Each compiler that the type tests and the consumer project run, in this order. */
export const COMPILERS: readonly LaneCompiler[] = [
  { name: "tsc 7", lane: "current" },
  { name: "bun check", lane: "current" },
  { name: "tsc 6.0", lane: "floor" },
];

function commandOf(compiler: CompilerName, project: string): { readonly command: string; readonly args: string[] } {
  switch (compiler) {
    case "tsc 7":
      return { command: CURRENT_COMPILER, args: ["--project", project, "--noEmit", "--pretty", "false"] };
    case "bun check":
      return { command: "bun", args: ["check", "--project", project, "--no-pretty", "--all"] };
    case "tsc 6.0":
      return { command: FLOOR_COMPILER, args: ["--project", project, "--noEmit", "--pretty", "false"] };
  }
}

export type CompileFailed =
  | { readonly kind: "NotRun"; readonly failure: ToolFailed }
  | { readonly kind: "UnlocatedErrors"; readonly lines: readonly string[] }
  | { readonly kind: "FailedWithoutDiagnostics"; readonly code: number; readonly output: string };

/**
 * The diagnostics of one compile of `project` (a `tsconfig.json`) with `compiler`, run in `cwd`; each file is
 * absolute. An error that names no place (a config error) fails the compile.
 */
export function compile(compiler: CompilerName, project: string, cwd: string): Result<readonly Diagnostic[], CompileFailed> {
  const { command, args } = commandOf(compiler, project);
  const run = runTool(command, args, cwd);

  if (!run.ok) {
    return { ok: false, error: { kind: "NotRun", failure: run.error } };
  }

  const report = diagnosticsOf(run.value.output, (file) => resolve(cwd, file));

  if (report.unlocated.length > 0) {
    return { ok: false, error: { kind: "UnlocatedErrors", lines: report.unlocated } };
  }

  if (run.value.code !== 0 && report.located.length === 0) {
    return { ok: false, error: { kind: "FailedWithoutDiagnostics", code: run.value.code, output: run.value.output } };
  }

  return { ok: true, value: report.located };
}

/** The text of a failed compile, for a message. */
export function compileFailedText(compiler: CompilerName, failure: CompileFailed): string {
  switch (failure.kind) {
    case "NotRun":
      return `${compiler}: did not run (${failure.failure.kind}): ${failure.failure.tool}`;
    case "UnlocatedErrors":
      return `${compiler}: errors without a place:\n${failure.lines.join("\n")}`;
    case "FailedWithoutDiagnostics":
      return `${compiler}: exit code ${failure.code} with no diagnostic:\n${failure.output}`;
  }
}
