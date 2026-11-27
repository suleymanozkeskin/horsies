// Builds `horsies` and the platform package of the host for the end-to-end lane of the built package, and installs both
// into `clients/typescript/node_modules`, where `import "horsies"` and the package loader find them:
//
//   cd clients/typescript && bun tests/e2e/install-built-package.ts --platform linux-x64-gnu --addon <abs path of the .node file>
//
// `horsies` is staged as the release stages it (`tests/packages/stage.ts`: `dist/` from `tsconfig.build.json`, the packed
// manifest) and replaces the workspace link `node_modules/horsies`. The platform package holds the manifest fields that
// the release checks (`name`, `version`, `os`, `cpu`, `libc`, `main`) and the addon file of the lane. The addon of the
// lane is the dev build (cargo feature `test-probes`), because the end-to-end files use its test exports; the release
// packages hold a build without it. Exit status: 0 when both are installed; 2 when an input or a step fails.

import { copyFileSync, cpSync, existsSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import { parseArgs } from "node:util";

import { WORKSPACE_DIR } from "../public-api/tools.ts";
import { platformExpectationOf } from "../packages/content.ts";
import { manifestRefusedText, readHorsiesManifest } from "../packages/manifest.ts";
import { platformListRefusedText, platformPackageOf, readPlatformList } from "../packages/platforms.ts";
import { stageFailedText, stageHorsies } from "../packages/stage.ts";

/** The folder of the installed packages. */
const NODE_MODULES = `${WORKSPACE_DIR}node_modules`;

/** The peer dependencies of `horsies` that the end-to-end files use, and so the package resolves from the workspace. */
const PEER_PACKAGES: readonly string[] = ["zod", "@opentelemetry/api", "pg", "postgres"];

/** The folders that hold the installed peer packages, in order: the tests package (the copy the test files import), then the workspace. */
const PEER_FOLDERS: readonly string[] = [`${WORKSPACE_DIR}tests/node_modules`, `${WORKSPACE_DIR}horsies/node_modules`, NODE_MODULES];

/**
 * Link each peer package into the installed `horsies` (a package manager that installs `horsies` as a user's project
 * does gives it its peers). The tests package comes first, so the test files and the package share one copy of the
 * OpenTelemetry API. Effects: writes links.
 */
function linkPeers(): void {
  for (const name of PEER_PACKAGES) {
    const source = PEER_FOLDERS.map((folder) => join(folder, name)).find((candidate) => existsSync(candidate));

    if (source === undefined) {
      continue;
    }

    const link = join(NODE_MODULES, "horsies", "node_modules", name);

    mkdirSync(dirname(link), { recursive: true });
    symlinkSync(source, link, "dir");
  }
}

function main(args: readonly string[]): number {
  let values: { readonly platform?: string | undefined; readonly addon?: string | undefined };

  try {
    values = parseArgs({ args: [...args], options: { platform: { type: "string" }, addon: { type: "string" } }, strict: true }).values;
  } catch (cause) {
    console.error(cause instanceof Error ? cause.message : String(cause));

    return 2;
  }

  const { platform, addon } = values;

  if (platform === undefined || addon === undefined || !isAbsolute(addon) || !existsSync(addon)) {
    console.error("give --platform <name of a row> and --addon <absolute path of an existing .node file>");

    return 2;
  }

  const rows = readPlatformList();
  const manifest = readHorsiesManifest();

  if (!rows.ok) {
    console.error(platformListRefusedText(rows.error));

    return 2;
  }

  if (!manifest.ok) {
    console.error(manifestRefusedText(manifest.error));

    return 2;
  }

  const row = rows.value.find((candidate) => candidate.platform === platform);

  if (row === undefined) {
    console.error(`the platform list has no row for ${platform}`);

    return 2;
  }

  const stageDir = mkdtempSync(join(tmpdir(), "horsies-e2e-package-"));

  try {
    const staged = stageHorsies(stageDir, manifest.value, rows.value);

    if (!staged.ok) {
      console.error(stageFailedText(staged.error));

      return 2;
    }

    rmSync(join(NODE_MODULES, "horsies"), { recursive: true, force: true });
    cpSync(staged.value, join(NODE_MODULES, "horsies"), { recursive: true });
    linkPeers();

    const expected = platformExpectationOf(row, manifest.value.version);
    const platformDir = join(NODE_MODULES, platformPackageOf(row));

    rmSync(platformDir, { recursive: true, force: true });
    mkdirSync(platformDir, { recursive: true });
    copyFileSync(addon, join(platformDir, expected.binary));
    writeFileSync(
      join(platformDir, "package.json"),
      `${JSON.stringify({
        name: expected.packageName,
        version: expected.version,
        os: [expected.os],
        cpu: [expected.cpu],
        ...(expected.libc.kind === "field" ? { libc: [expected.libc.value] } : {}),
        main: expected.binary,
        files: [expected.binary],
      })}\n`,
    );
    console.log(`installed horsies and ${expected.packageName}@${expected.version} into ${NODE_MODULES}`);

    return 0;
  } finally {
    rmSync(stageDir, { recursive: true, force: true });
  }
}

process.exitCode = main(process.argv.slice(2));
