// The compiled-program check of the packed packages: a new project installs `horsies` from the local registry with
// Bun, `bun build --compile` makes one file of a program that creates an app and pings PostgreSQL, the file is copied
// to a new folder, the project folder (and with it every `node_modules`) is removed, and the file runs there. Run by
// Bun:
//
//   bun tests/packages/compile-check.ts --registry <url> --version <version> --platform-package <name> --release <publish|hold>
//
// A `publish` row installs `horsies` only (Bun chooses the platform package from the optional dependencies); a `hold`
// row installs its platform package by name beside `horsies`. Needs `HORSIES_TEST_DATABASE_URL`, which the compiled
// program reads. Exit status: 0 when the compiled program pings the database and exits with 0; 1 when it does not;
// 2 when the arguments do not read, or the install or the build fails.

import { copyFileSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";

import { DATABASE_URL_VARIABLE } from "../runtime/native/manifest.ts";
import { commandFailedText, runCommand } from "./command.ts";

/** The file name of the compiled program. */
const PROGRAM_FILE = "horsies-compiled-ping";

/**
 * The program that the check compiles: it creates an app on the database that the variable names, pings it through
 * the public API, writes one JSON line, and exits with 0 only when the ping succeeds.
 */
const PROGRAM_SOURCE = `import { Horsies } from "horsies";

const created = Horsies.create({
  database: { databaseUrl: { kind: "value", value: process.env[${JSON.stringify(DATABASE_URL_VARIABLE)}] ?? "" } },
  tasks: [],
});

if (!created.ok) {
  console.log(JSON.stringify({ created: created.error.refusals.map((refusal) => refusal.message) }));
  process.exit(1);
}

const ping = await created.value.pingDatabase();

console.log(JSON.stringify(ping.ok ? { ping: "ok", latencyMs: ping.value.latencyMs } : { ping: ping.error }));
process.exit(ping.ok ? 0 : 1);
`;

/** The inputs of the check. */
type Inputs = {
  readonly registry: string;
  readonly version: string;
  readonly platformPackage: string;
  readonly release: "publish" | "hold";
};

/** The inputs from the arguments, or the text of the refusal. Pure. */
function inputsOf(args: readonly string[]): Inputs | { readonly refused: string } {
  const options = {
    registry: { type: "string" },
    version: { type: "string" },
    "platform-package": { type: "string" },
    release: { type: "string" },
  } as const;

  let values: { readonly [K in keyof typeof options]?: string | undefined };

  try {
    values = parseArgs({ args: [...args], options, strict: true }).values;
  } catch (cause) {
    return { refused: cause instanceof Error ? cause.message : String(cause) };
  }

  const { registry, version, release } = values;
  const platformPackage = values["platform-package"];

  if (registry === undefined || version === undefined || platformPackage === undefined) {
    return { refused: "give --registry, --version and --platform-package" };
  }

  if (release !== "publish" && release !== "hold") {
    return { refused: "give --release publish|hold" };
  }

  return { registry, version, platformPackage, release };
}

/**
 * Install the packages in `folder` with Bun, write the program, and compile it to `folder/PROGRAM_FILE`; the failure
 * text, or `compiled`. Effects: writes the folder; runs `bun add` and `bun build --compile`.
 */
function installAndCompile(inputs: Inputs, folder: string): "compiled" | { readonly failed: string } {
  const packages = [`horsies@${inputs.version}`, ...(inputs.release === "hold" ? [`${inputs.platformPackage}@${inputs.version}`] : [])];

  writeFileSync(join(folder, "package.json"), `${JSON.stringify({ name: "horsies-compile-check", private: true, type: "module" })}\n`);
  writeFileSync(join(folder, "bunfig.toml"), `[install]\nregistry = "${inputs.registry}"\n`);
  writeFileSync(join(folder, "program.ts"), PROGRAM_SOURCE);

  const install = runCommand(process.execPath, ["add", ...packages], folder);

  if (!install.ok || install.value.code !== 0) {
    return { failed: commandFailedText(`bun add ${packages.join(" ")}`, install) };
  }

  const build = runCommand(process.execPath, ["build", "--compile", "program.ts", "--outfile", PROGRAM_FILE], folder);

  if (!build.ok || build.value.code !== 0) {
    return { failed: commandFailedText("bun build --compile program.ts", build) };
  }

  console.log(`installed ${packages.join(" ")} and compiled ${PROGRAM_FILE} in ${folder}`);

  return "compiled";
}

/** The problems of the compiled program that runs in `runFolder`: it exits with 0. Effects: runs the program. */
function runProblems(runFolder: string): readonly string[] {
  const run = runCommand(join(runFolder, PROGRAM_FILE), [], runFolder);

  if (!run.ok || run.value.code !== 0) {
    return [commandFailedText(`${PROGRAM_FILE} in a folder with no node_modules`, run)];
  }

  console.log(`compiled program in ${runFolder}: ${run.value.stdout.trim()}`);

  return [];
}

/**
 * Install, compile, move the program, remove the project, and run the program; the exit status. Effects: as
 * `installAndCompile` and `runProblems`; removes both folders at the end.
 */
function main(args: readonly string[]): number {
  const inputs = inputsOf(args);

  if ("refused" in inputs) {
    console.error(inputs.refused);

    return 2;
  }

  const projectFolder = mkdtempSync(join(tmpdir(), "horsies-compile-check-"));
  const runFolder = mkdtempSync(join(tmpdir(), "horsies-compiled-run-"));

  try {
    const compiled = installAndCompile(inputs, projectFolder);

    if (compiled !== "compiled") {
      console.error(compiled.failed);

      return 2;
    }

    copyFileSync(join(projectFolder, PROGRAM_FILE), join(runFolder, PROGRAM_FILE));
    rmSync(projectFolder, { recursive: true, force: true });

    const problems = runProblems(runFolder);

    for (const problem of problems) {
      console.error(`FAIL (compiled program): ${problem}`);
    }

    return problems.length === 0 ? 0 : 1;
  } finally {
    rmSync(projectFolder, { recursive: true, force: true });
    rmSync(runFolder, { recursive: true, force: true });
  }
}

process.exitCode = main(process.argv.slice(2));
