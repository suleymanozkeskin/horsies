// The type tests and the consumer project, with each compiler of the two lanes (`lanes.ts`). Run from
// `clients/typescript/`:
//
//   bun tests/types/run.ts
//
// - Compile-pass cases (`pass/*.ts`): no diagnostic.
// - Compile-fail cases (`fail/*.ts`): exactly the diagnostics that line 1 names (`cases.ts`).
// - The consumer project (`consumer/`): it imports each entry point from the built package; no diagnostic.
//
// Each case is a module (it imports the package), so one program per compiler holds every case, and the diagnostics of
// a file are its own. Exit status: 0 when every case and the consumer pass; 1 when one fails; 2 when a tool fails.
// Needs `bun`, and the floor compiler installed in `ci/typescript-floor/`. The work files are in a new folder of the
// system's temporary folder, removed at the end.

import { cpSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { type BuiltPackage, buildPackage } from "../public-api/package.ts";
import { TESTS_DIR, WORKSPACE_DIR } from "../public-api/tools.ts";
import { type CaseExpectation, caseVerdict, type Diagnostic, expectationOf, headerRefusedText } from "./cases.ts";
import { compile, compileFailedText, COMPILERS, type LaneCompiler } from "./lanes.ts";

const TYPES_DIR: string = fileURLToPath(new URL("./", import.meta.url));

type TypeCase =
  | { readonly kind: "pass"; readonly file: string }
  | { readonly kind: "fail"; readonly file: string; readonly expectation: CaseExpectation };

/** The exit status of a run. */
type Verdict = "passed" | "refused" | "toolFailed";

const EXIT_STATUS: Readonly<Record<Verdict, number>> = { passed: 0, refused: 1, toolFailed: 2 };

function worse(first: Verdict, second: Verdict): Verdict {
  return EXIT_STATUS[first] >= EXIT_STATUS[second] ? first : second;
}

function caseFilesOf(folder: "pass" | "fail"): readonly string[] {
  return readdirSync(join(TYPES_DIR, folder))
    .filter((name) => name.endsWith(".ts"))
    .toSorted()
    .map((name) => join(TYPES_DIR, folder, name));
}

/** The cases, or the header problems of the compile-fail cases (each one is printed). */
function typeCases(): readonly TypeCase[] | "refused" {
  const problems: string[] = [];
  const cases: TypeCase[] = caseFilesOf("pass").map((file) => ({ kind: "pass", file }));

  for (const file of caseFilesOf("fail")) {
    const expectation = expectationOf(readFileSync(file, "utf8"));

    if (expectation.ok) {
      cases.push({ kind: "fail", file, expectation: expectation.value });
    } else {
      problems.push(`${file.slice(TYPES_DIR.length)}: ${headerRefusedText(expectation.error)}`);
    }
  }

  for (const problem of problems) {
    console.error(problem);
  }

  return problems.length === 0 ? cases : "refused";
}

/** A `tsconfig.json` in `workFolder` with the options of the tests project and the case files only. */
function casesProject(workFolder: string, cases: readonly TypeCase[]): string {
  const project = join(workFolder, "cases.tsconfig.json");

  const config = {
    extends: join(TESTS_DIR, "tsconfig.json"),
    compilerOptions: { typeRoots: [join(WORKSPACE_DIR, "node_modules", "@types")], noEmit: true },
    files: cases.map((typeCase) => typeCase.file),
    include: [],
  };

  writeFileSync(project, JSON.stringify(config, null, 2));

  return project;
}

/** The problems of one compile of the cases: each case whose diagnostics differ, each diagnostic outside a case. */
function caseProblems(compiler: LaneCompiler, cases: readonly TypeCase[], diagnostics: readonly Diagnostic[]): readonly string[] {
  const caseFiles = new Set(cases.map((typeCase) => typeCase.file));

  const outside = diagnostics
    .filter((diagnostic) => !caseFiles.has(diagnostic.file))
    .map((diagnostic) => `${compiler.name}: ${diagnostic.code} outside a case: ${diagnostic.file}:${diagnostic.line}`);

  const differing = cases.flatMap((typeCase) => {
    const expected = typeCase.kind === "pass" ? [] : typeCase.expectation[compiler.lane];
    const verdict = caseVerdict(expected, diagnostics.filter((diagnostic) => diagnostic.file === typeCase.file));

    switch (verdict.state) {
      case "passed":
        return [];
      case "failed":
        return [
          `${compiler.name}: ${typeCase.file.slice(TYPES_DIR.length)}: expected [${verdict.expected.join(", ")}], ` +
            `got [${verdict.actual.join(", ")}]`,
        ];
    }
  });

  return [...outside, ...differing];
}

/** Compiles `project` with each compiler and judges its diagnostics with `problemsOf`; prints each problem. */
function runLanes(
  label: string,
  project: string,
  problemsOf: (compiler: LaneCompiler, diagnostics: readonly Diagnostic[]) => readonly string[],
): Verdict {
  let verdict: Verdict = "passed";

  for (const compiler of COMPILERS) {
    const compiled = compile(compiler.name, project, WORKSPACE_DIR);

    if (!compiled.ok) {
      console.error(compileFailedText(compiler.name, compiled.error));
      verdict = worse(verdict, "toolFailed");
      continue;
    }

    const problems = problemsOf(compiler, compiled.value);

    for (const problem of problems) {
      console.error(problem);
    }

    console.log(`${compiler.name} (${compiler.lane} lane): ${label}: ${problems.length === 0 ? "passed" : "failed"}`);
    verdict = worse(verdict, problems.length === 0 ? "passed" : "refused");
  }

  return verdict;
}

/** The consumer project in `workFolder`, with the built package and zod in its `node_modules`. */
function consumerProject(workFolder: string, builtPackage: BuiltPackage): string {
  const folder = join(workFolder, "consumer");

  cpSync(join(TYPES_DIR, "consumer"), folder, { recursive: true });
  writeFileSync(join(folder, "package.json"), JSON.stringify({ private: true, type: "module" }));
  mkdirSync(join(folder, "node_modules"));
  symlinkSync(builtPackage.folder, join(folder, "node_modules", "horsies"), "dir");
  symlinkSync(join(TESTS_DIR, "node_modules", "zod"), join(folder, "node_modules", "zod"), "dir");

  return join(folder, "tsconfig.json");
}

function consumerVerdict(workFolder: string): Verdict {
  const packageFolder = join(workFolder, "package");

  mkdirSync(packageFolder);

  const builtPackage = buildPackage(packageFolder);

  if (!builtPackage.ok) {
    console.error(`the package declarations were not built: ${builtPackage.error.kind}`);

    return "toolFailed";
  }

  const project = consumerProject(workFolder, builtPackage.value);

  return runLanes("the consumer project", project, (compiler, diagnostics) =>
    diagnostics.map((diagnostic) => `${compiler.name}: consumer: ${diagnostic.code} at ${diagnostic.file}:${diagnostic.line}`),
  );
}

function run(workFolder: string): Verdict {
  const cases = typeCases();

  if (cases === "refused") {
    return "refused";
  }

  const casesVerdict = runLanes(`${cases.length} type test cases`, casesProject(workFolder, cases), (compiler, diagnostics) =>
    caseProblems(compiler, cases, diagnostics),
  );

  return worse(casesVerdict, consumerVerdict(workFolder));
}

function main(): Verdict {
  const workFolder = realpathSync(mkdtempSync(join(tmpdir(), "horsies-type-tests-")));

  try {
    return run(workFolder);
  } finally {
    rmSync(workFolder, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  process.exitCode = EXIT_STATUS[main()];
}
