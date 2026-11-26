// The install check of the packed packages: a new project installs `horsies` from the local registry, as a user does,
// and the installed package loads its addon and pings PostgreSQL. Run by the runtime of the installer:
//
//   node tests/packages/consumer-check.ts --installer npm --registry <url> --version <version> --platform-package <name> --release <publish|hold>
//   bun tests/packages/consumer-check.ts --installer bun ...
//
// A `publish` row installs `horsies` only: the package manager must choose the platform package from the optional
// dependencies. A `hold` row is not listed there, so its platform package is installed by name beside `horsies`. Needs
// `HORSIES_TEST_DATABASE_URL`. Exit status: 0 when every check passes; 1 when a check fails; 2 when the arguments do
// not read or the install fails.

import { mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

import { isPlainObject } from "../../horsies/src/codec/values.ts";
import { manifestJson, testDatabaseUrl } from "../runtime/native/manifest.ts";
import { coreStateProblem, testOnlyExportsIn } from "./addon-exports.ts";
import { commandFailedText, runCommand } from "./command.ts";
import type { Installer } from "./matrix.ts";

/** The modules of the installed package that the check calls, typed by their source modules. */
type LoadModule = typeof import("../../horsies/src/native/load.ts");

type ProcessCoreModule = typeof import("../../horsies/src/native/process-core/addon.ts");

type CoreHandleModule = typeof import("../../horsies/src/native/handle/core-handle.ts");

/** The inputs of the check. */
type Inputs = {
  readonly installer: Installer;
  readonly registry: string;
  readonly version: string;
  readonly platformPackage: string;
  readonly release: "publish" | "hold";
};

/** The inputs from the arguments, or the text of the refusal. Pure. */
function inputsOf(args: readonly string[]): Inputs | { readonly refused: string } {
  const options = {
    installer: { type: "string" },
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

  const { installer, registry, version, release } = values;
  const platformPackage = values["platform-package"];

  if ((installer !== "npm" && installer !== "bun") || (release !== "publish" && release !== "hold")) {
    return { refused: "give --installer npm|bun and --release publish|hold" };
  }

  if (registry === undefined || version === undefined || platformPackage === undefined) {
    return { refused: "give --registry, --version and --platform-package" };
  }

  return { installer, registry, version, platformPackage, release };
}

/** Write the project in `folder` and install the packages; the failure text, or `installed`. Effects: writes and installs. */
function install(inputs: Inputs, folder: string): "installed" | { readonly failed: string } {
  const packages = [`horsies@${inputs.version}`, ...(inputs.release === "hold" ? [`${inputs.platformPackage}@${inputs.version}`] : [])];

  writeFileSync(join(folder, "package.json"), `${JSON.stringify({ name: "horsies-install-check", private: true, type: "module" })}\n`);

  let run: ReturnType<typeof runCommand>;

  switch (inputs.installer) {
    case "npm":
      writeFileSync(join(folder, ".npmrc"), `registry=${inputs.registry}\n`);
      run = runCommand("npm", ["install", "--no-audit", "--no-fund", "--no-update-notifier", ...packages], folder);
      break;
    case "bun":
      writeFileSync(join(folder, "bunfig.toml"), `[install]\nregistry = "${inputs.registry}"\n`);
      run = runCommand(process.execPath, ["add", ...packages], folder);
      break;
  }

  if (!run.ok || run.value.code !== 0) {
    return { failed: commandFailedText(`${inputs.installer} install ${packages.join(" ")}`, run) };
  }

  console.log(`installed ${packages.join(" ")} with ${inputs.installer} in ${folder}`);

  return "installed";
}

/** The script that imports `horsies` as a user's module does, and tries one deep import. */
const IMPORT_SCRIPT = `(async () => {
  const entry = await import("horsies");
  let deep = "imported";
  try { await import("horsies/dist/native/load.js"); } catch (error) { deep = String(error && (error.code || error.name)); }
  console.log(JSON.stringify({ exports: Object.keys(entry).length, deep }));
})().catch((error) => { console.log(JSON.stringify({ failed: String(error) })); });`;

/** The problems of `import "horsies"` from the project: the entry has exports, a deep import is refused. Effects: runs the runtime. */
function importProblems(folder: string): readonly string[] {
  const run = runCommand(process.execPath, ["-e", IMPORT_SCRIPT], folder);

  if (!run.ok || run.value.code !== 0) {
    return [commandFailedText("import of horsies", run)];
  }

  const line = run.value.stdout.trim();

  console.log(`import of horsies from the project: ${line}`);

  // SAFETY: the script writes one JSON object.
  const answer = JSON.parse(line) as { readonly exports?: number; readonly deep?: string; readonly failed?: string };

  return [
    ...(answer.failed === undefined ? [] : [`import("horsies") failed: ${answer.failed}`]),
    ...((answer.exports ?? 0) > 0 ? [] : ["import(\"horsies\") gives no export"]),
    ...(answer.deep === "imported" ? ["the deep import horsies/dist/native/load.js resolved: the exports map must refuse it"] : []),
  ];
}

/** The platform packages that resolve from the installed `horsies`. Effects: resolves modules. */
function installedPlatformPackages(packageDir: string, candidates: readonly string[]): readonly string[] {
  const requireFromPackage = createRequire(join(packageDir, "package.json"));

  return candidates.filter((name) => {
    try {
      requireFromPackage.resolve(`${name}/package.json`);

      return true;
    } catch {
      return false;
    }
  });
}

/** The installed `horsies` folder, by its real path. Effects: resolves the module. */
function installedPackageDir(folder: string): string {
  return dirname(realpathSync(createRequire(join(folder, "package.json")).resolve("horsies/package.json")));
}

/** The platform package names that the installed `horsies` lists, and the row's own. Effects: reads the manifest. */
function candidatePackages(packageDir: string, platformPackage: string): readonly string[] {
  const manifest: unknown = createRequire(join(packageDir, "package.json"))("./package.json");
  const optional = isPlainObject(manifest) ? manifest["optionalDependencies"] : undefined;
  const listed = isPlainObject(optional) ? Object.keys(optional) : [];

  return [...new Set([...listed, platformPackage])].toSorted();
}

/**
 * The problems of the installed package: exactly the row's platform package is installed, the package loader loads it,
 * it has no test-only export, the core has not started, and a core handle pings the database. Effects: loads the
 * addon; connects to the database.
 */
async function packageProblems(packageDir: string, platformPackage: string): Promise<readonly string[]> {
  const installed = installedPlatformPackages(packageDir, candidatePackages(packageDir, platformPackage));

  if (installed.join(" ") !== platformPackage) {
    return [`the installed platform packages are [${installed.join(", ")}], not [${platformPackage}]`];
  }

  const module = (path: string): string => pathToFileURL(join(packageDir, "dist", path)).href;
  // SAFETY: each module is the build of the source module that names its type.
  const load = (await import(module("native/load.js"))) as LoadModule;
  // SAFETY: as above.
  const processCore = (await import(module("native/process-core/addon.js"))) as ProcessCoreModule;
  // SAFETY: as above.
  const coreHandle = (await import(module("native/handle/core-handle.js"))) as CoreHandleModule;
  const loaded = load.loadAddon();

  if (loaded.kind !== "loaded") {
    return [`the package loader did not load the addon: ${JSON.stringify(loaded)}`];
  }

  if (loaded.source.kind !== "platformPackage" || loaded.source.packageName !== platformPackage) {
    return [`the package loader loaded ${JSON.stringify(loaded.source)}, not ${platformPackage}`];
  }

  const state = coreStateProblem(processCore.processCoreView(loaded.exports).coreState());
  const handle = coreHandle.buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");
  const ping = await handle.pingDatabase();

  void (await handle.close());
  console.log(`loaded ${platformPackage}; ping: ${ping.kind === "done" ? `${ping.record.latencyMs} ms` : JSON.stringify(ping.error)}`);

  return [
    ...testOnlyExportsIn(loaded.exports).map((name) => `the addon exports the test-only ${name}`),
    ...(state === "none" ? [] : [state]),
    ...(ping.kind === "done" ? [] : [`the ping failed: ${JSON.stringify(ping.error)}`]),
  ];
}

/**
 * Install and check; the exit status. Effects: as `install`, `importProblems` and `packageProblems`; removes the
 * project folder at the end.
 */
async function main(args: readonly string[]): Promise<number> {
  const inputs = inputsOf(args);

  if ("refused" in inputs) {
    console.error(inputs.refused);

    return 2;
  }

  const folder = mkdtempSync(join(tmpdir(), "horsies-install-check-"));

  try {
    const installed = install(inputs, folder);

    if (installed !== "installed") {
      console.error(installed.failed);

      return 2;
    }

    const problems = [...importProblems(folder), ...(await packageProblems(installedPackageDir(folder), inputs.platformPackage))];

    for (const problem of problems) {
      console.error(`FAIL (${inputs.installer}): ${problem}`);
    }

    return problems.length === 0 ? 0 : 1;
  } finally {
    rmSync(folder, { recursive: true, force: true });
  }
}

// The process ends here: a core that started keeps no work that the check waits for.
process.exit(await main(process.argv.slice(2)));
