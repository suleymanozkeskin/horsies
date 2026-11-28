// The matrices of `typescript-packages.yml`, from the platform list. Run from `clients/typescript/`:
//
//   bun tests/packages/matrix.ts --node-version <version> --bun-version <version> >> "$GITHUB_OUTPUT"
//
// It writes three output lines: `version` (the version of `horsies`, the one version of the release), `build_matrix`
// (one entry per platform) and `install_matrix` (one entry per platform and installer). Exit status: 0 when the lines
// are written; 2 when the arguments, the platform list or the manifest do not read.

import { parseArgs } from "node:util";

import type { NativePlatform } from "../../horsies/src/native/addon-file.ts";
import { manifestRefusedText, readHorsiesManifest } from "./manifest.ts";
import { platformListRefusedText, platformPackageOf, readPlatformList } from "./platforms.ts";
import type { NpmBuild, NpmInstall, ReleasePlatform, ReleaseState } from "./platforms.ts";

/** The exact runtime versions of the workflow (its `env` block). */
export type RuntimeVersions = { readonly node: string; readonly bun: string };

/** One entry of the build matrix: a platform, its build, and where its load check runs. */
export type BuildEntry = {
  readonly name: string;
  readonly runner: string;
  readonly rust_target: string;
  readonly platform: NativePlatform;
  readonly build: NpmBuild["kind"];
  readonly check: NpmInstall["kind"];
  readonly build_image?: string;
  readonly node_image?: string;
  readonly bun_image?: string;
};

/** The package manager of an install check, and the runtime that loads the installed package. */
export type Installer = "npm" | "bun";

/** One entry of the install matrix: a platform and an installer. */
export type InstallEntry = {
  readonly name: string;
  readonly runner: string;
  readonly platform: NativePlatform;
  readonly package: string;
  readonly release: ReleaseState;
  readonly installer: Installer;
  readonly runtime: "node" | "bun";
  readonly place: NpmInstall["kind"];
  /** `run`: the install check serves the monitoring page and its script; `skip`: it does not. */
  readonly web_page: "run" | "skip";
  /** Every platform package of the list, comma separated: the selection check offers them all to the installer. */
  readonly all_packages: string;
  readonly image?: string;
};

/** The Node image of a variant, such as `node:26.13.1-alpine3.23`. Pure. */
export function nodeImage(versions: RuntimeVersions, variant: string): string {
  return `node:${versions.node}-${variant}`;
}

/** The Bun image of a variant, such as `oven/bun:1.4.4-alpine`. Pure. */
export function bunImage(versions: RuntimeVersions, variant: string): string {
  return `oven/bun:${versions.bun}-${variant}`;
}

/** The container image of a build: the Node image of an Alpine build, the manylinux image of a manylinux build. Pure. */
function buildImageOf(build: NpmBuild, versions: RuntimeVersions): { readonly build_image?: string } {
  switch (build.kind) {
    case "alpine-container":
      return { build_image: nodeImage(versions, build.nodeVariant) };
    case "manylinux-container":
      return { build_image: build.image };
    case "glibc-2.17-toolchain":
    case "runner":
      return {};
  }
}

/** The build entry of a row. Pure. */
export function buildEntryOf(row: ReleasePlatform, versions: RuntimeVersions): BuildEntry {
  const entry = { name: row.name, runner: row.runner, rust_target: row.rustTarget, platform: row.platform };
  const buildImage = buildImageOf(row.build, versions);

  const checkImages =
    row.install.kind === "container"
      ? { node_image: nodeImage(versions, row.install.nodeVariant), bun_image: bunImage(versions, row.install.bunVariant) }
      : {};

  return { ...entry, build: row.build.kind, check: row.install.kind, ...buildImage, ...checkImages };
}

/** Whether the install check of a row serves the monitoring page: the Linux x86_64 glibc row, as the wheel check does. Pure. */
export function webPageOf(row: ReleasePlatform): InstallEntry["web_page"] {
  return row.platform === "linux-x64-gnu" ? "run" : "skip";
}

/** The two install entries of a row: npm on Node, and Bun. Pure. */
export function installEntriesOf(
  row: ReleasePlatform,
  versions: RuntimeVersions,
  allPackages: string = platformPackageOf(row),
): readonly InstallEntry[] {
  const common = {
    all_packages: allPackages,
    name: row.name,
    runner: row.runner,
    platform: row.platform,
    package: platformPackageOf(row),
    release: row.release,
    place: row.install.kind,
    web_page: webPageOf(row),
  };

  switch (row.install.kind) {
    case "runner":
      return [
        { ...common, installer: "npm", runtime: "node" },
        { ...common, installer: "bun", runtime: "bun" },
      ];
    case "container":
      return [
        { ...common, installer: "npm", runtime: "node", image: nodeImage(versions, row.install.nodeVariant) },
        { ...common, installer: "bun", runtime: "bun", image: bunImage(versions, row.install.bunVariant) },
      ];
  }
}

/** A runtime version of the exact form `<major>.<minor>.<patch>`. */
const EXACT_VERSION = /^\d+\.\d+\.\d+$/u;

/** The runtime versions of the arguments, or the text of the refusal. Pure. */
export function runtimeVersionsOf(args: readonly string[]): RuntimeVersions | { readonly refused: string } {
  let values: { readonly "node-version"?: string | undefined; readonly "bun-version"?: string | undefined };

  try {
    values = parseArgs({
      args: [...args],
      options: { "node-version": { type: "string" }, "bun-version": { type: "string" } },
      strict: true,
    }).values;
  } catch (cause) {
    return { refused: cause instanceof Error ? cause.message : String(cause) };
  }

  const node = values["node-version"];
  const bun = values["bun-version"];

  if (node === undefined || bun === undefined || !EXACT_VERSION.test(node) || !EXACT_VERSION.test(bun)) {
    return { refused: "give --node-version and --bun-version, each as <major>.<minor>.<patch>" };
  }

  return { node, bun };
}

/** Write the output lines; the exit status. Effects: reads the platform list and the manifest; writes stdout and stderr. */
function main(args: readonly string[]): number {
  const versions = runtimeVersionsOf(args);

  if ("refused" in versions) {
    console.error(versions.refused);

    return 2;
  }

  const rows = readPlatformList();

  if (!rows.ok) {
    console.error(platformListRefusedText(rows.error));

    return 2;
  }

  const manifest = readHorsiesManifest();

  if (!manifest.ok) {
    console.error(manifestRefusedText(manifest.error));

    return 2;
  }

  const allPackages = rows.value.map((row) => platformPackageOf(row)).join(",");
  const build = rows.value.map((row) => buildEntryOf(row, versions));
  const install = rows.value.flatMap((row) => installEntriesOf(row, versions, allPackages));

  console.log(`version=${manifest.value.version}`);
  console.log(`build_matrix=${JSON.stringify({ include: build })}`);
  console.log(`install_matrix=${JSON.stringify({ include: install })}`);

  return 0;
}

if (import.meta.main) {
  process.exitCode = main(process.argv.slice(2));
}
