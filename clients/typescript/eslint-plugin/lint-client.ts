// The rules of README.md (`horsies/no-dropped-result`, `@typescript-eslint/switch-exhaustiveness-check`) on the
// client's own code: every file of the projects `horsies/tsconfig.json` (the package source), `tests/tsconfig.json`
// (the tests), `examples/tsconfig.json` and `examples/showcase/tsconfig.json` (the examples), with one change for the boundary parsers (`BOUNDARY_PARSERS`). Run on Node from any folder, after
// `bun install --frozen-lockfile` in `clients/typescript/` (ci/unit.sh):
//
//   node eslint-plugin/lint-client.ts
//
// Exit status: 0 when no file gets a report; 1 when a file gets a report; 2 when a file does not lint (a fatal
// message of ESLint, as for a parse error) or a project's file list does not read.

import { fileURLToPath } from "node:url";

import { ESLint, type Linter } from "eslint";
import ts from "typescript";

import { lintConfig } from "./lint-config.ts";

const WORKSPACE_DIR = fileURLToPath(new URL("../", import.meta.url));

/** The projects of the client's own code, relative to the workspace folder. */
const CLIENT_PROJECTS: readonly string[] = [
  "horsies/tsconfig.json",
  "tests/tsconfig.json",
  "examples/tsconfig.json",
  "examples/showcase/tsconfig.json",
];

/**
 * The boundary parsers of the client. Each one switches on a value of a set that the client does not own (a JSON
 * value, a `typeof` result, the platform and CPU of the host, the definition type of a zod schema, a TypeScript syntax
 * kind), names the members that it reads, and takes the others in `default`: a refusal, or the next reader, which gets
 * the narrowed rest. In these files a `default` makes a switch exhaustive. `oxlint.config.ts` has overrides for some
 * of the same files, for the same reason.
 */
const BOUNDARY_PARSERS: readonly string[] = [
  "horsies/src/_catalog/monitoring_models.ts",
  "horsies/src/native/load.ts",
  "horsies/src/worker/manifest-text.ts",
  "horsies/src/zod/definition.ts",
  "tests/packages/platforms.ts",
  "tests/public-api/surface.ts",
  "tests/unit/generated/support/wire-text.ts",
];

/** The config of README.md for the client's projects, and the rule option of the boundary parsers. Pure. */
function clientConfig(): Linter.Config[] {
  return [
    ...lintConfig({ kind: "projects", rootDir: WORKSPACE_DIR, tsconfigFiles: CLIENT_PROJECTS }),
    {
      files: [...BOUNDARY_PARSERS],
      rules: { "@typescript-eslint/switch-exhaustiveness-check": ["error", { considerDefaultExhaustiveForUnions: true }] },
    },
  ];
}

/** The file list of a project, or the text of the diagnostics that stop its read. */
type ProjectRead = { readonly kind: "read"; readonly files: readonly string[] } | { readonly kind: "refused"; readonly text: string };

/** The outcome of the lint run. */
type Verdict = { readonly kind: "clean" } | { readonly kind: "reported" } | { readonly kind: "notLinted"; readonly text: string };

function diagnosticText(diagnostic: ts.Diagnostic): string {
  return ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n");
}

/** The files of the project `tsconfigFile`, as the compiler reads it. Effects: reads the config and its folders. */
function projectFiles(tsconfigFile: string): ProjectRead {
  const unrecoverable: ts.Diagnostic[] = [];

  const host: ts.ParseConfigFileHost = {
    ...ts.sys,
    onUnRecoverableConfigFileDiagnostic: (diagnostic) => {
      unrecoverable.push(diagnostic);
    },
  };

  const parsed = ts.getParsedCommandLineOfConfigFile(`${WORKSPACE_DIR}${tsconfigFile}`, undefined, host);
  const diagnostics = [...unrecoverable, ...(parsed?.errors ?? [])];

  if (parsed === undefined || diagnostics.length > 0) {
    return { kind: "refused", text: `${tsconfigFile}: ${diagnostics.map(diagnosticText).join("; ")}` };
  }

  return { kind: "read", files: parsed.fileNames };
}

/** The verdict of the lint results: a fatal message first, then any report. Pure. */
function verdictOf(results: readonly ESLint.LintResult[]): Verdict {
  const fatal = results.flatMap((result) => result.messages.filter((message) => message.fatal === true).map((message) => `${result.filePath}: ${message.message}`));

  if (fatal.length > 0) {
    return { kind: "notLinted", text: fatal.join("\n") };
  }

  return results.some((result) => result.messages.length > 0) ? { kind: "reported" } : { kind: "clean" };
}

/** Lint `files` with the rules, in the client's projects. Effects: reads the files; writes the reports to stdout. */
async function lintClient(files: readonly string[]): Promise<Verdict> {
  const eslint = new ESLint({ cwd: WORKSPACE_DIR, overrideConfigFile: true, overrideConfig: clientConfig() });
  const results = await eslint.lintFiles([...files]);
  const formatter = await eslint.loadFormatter("stylish");

  process.stdout.write(await formatter.format(results));

  return verdictOf(results);
}

async function main(): Promise<number> {
  const reads = CLIENT_PROJECTS.map(projectFiles);
  const refusals = reads.flatMap((read) => (read.kind === "refused" ? [read.text] : []));

  if (refusals.length > 0) {
    process.stderr.write(`${refusals.join("\n")}\n`);

    return 2;
  }

  const files = reads.flatMap((read) => (read.kind === "read" ? read.files : []));
  const verdict = await lintClient(files);

  switch (verdict.kind) {
    case "clean":
      process.stdout.write(`${files.length} files, no report\n`);

      return 0;
    case "reported":
      return 1;
    case "notLinted":
      process.stderr.write(`${verdict.text}\n`);

      return 2;
  }
}

process.exitCode = await main();
