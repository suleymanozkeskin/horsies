// The zod releases that the adapter reads, and the check of the release that built a schema.
//
// The main entry recognises a zod schema by its Standard Schema marker (`~standard.vendor === "zod"`). zod 3 has the
// same marker, and a zod 4 release outside the supported range may hold definition parts that this adapter does not
// know. So the adapter checks the release first, from `schema._zod.version`, and refuses a schema that it cannot read.
//
// The schema can come from any copy of zod: the ES module or the CommonJS build of an install, a copy in a bundle, or
// another install. The check reads the schema's own version object, and the adapter reads and runs the schema through
// its own internals only (`definition.ts`, `runs.ts`). The adapter imports no zod module.

import { isNumber, isObjectLike } from "../codec/values.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { ZodSchema } from "./internals.ts";

/** A zod release number. */
export type ZodVersion = { readonly major: number; readonly minor: number; readonly patch: number };

/**
 * The zod releases that this adapter reads: from `lowest` up to, not including, `below`. The peer dependency range of
 * `horsies/package.json` is `range` (a test keeps them equal). CI runs the self-check and the zod adapter tests on the
 * newest patch release of each minor version in the range: 4.1.13, 4.2.1, 4.3.6, 4.4.3, 4.5.4, 4.6.7.
 */
export const SUPPORTED_ZOD: {
  readonly lowest: ZodVersion;
  readonly below: ZodVersion;
  readonly range: string;
} = Object.freeze({
  lowest: Object.freeze({ major: 4, minor: 1, patch: 13 }),
  below: Object.freeze({ major: 4, minor: 7, patch: 0 }),
  range: ">=4.1.13 <4.7.0",
});

/** A value with the Standard Schema marker, as the main entry recognises a schema of a validation library. */
export type StandardSchemaMarked = { readonly "~standard": { readonly vendor: string; readonly version: number } };

/** Why the adapter cannot read the zod release that built a schema. */
export type ZodReleaseRefused =
  /** The schema has no zod 4 internals: a zod 3 schema (zod 3.24 and later, or the `zod/v3` export of zod 4). */
  | { readonly kind: "Zod3Schema"; readonly fix: string }
  /** A zod 4 release outside `SUPPORTED_ZOD`. */
  | { readonly kind: "ZodVersionNotSupported"; readonly found: ZodVersion; readonly supported: string };

const ZOD_3: ZodReleaseRefused = Object.freeze({
  kind: "Zod3Schema",
  fix: 'build the schema with zod 4: import { z } from "zod", not from "zod/v3" or a zod 3 install',
});

/**
 * The zod 4 schema of a marked value, when the release that built it is inside `SUPPORTED_ZOD`. The release is read
 * from the schema itself, so a schema of each zod copy passes. Fails with `Zod3Schema` or `ZodVersionNotSupported`.
 * Pure: it reads `_zod.version` and the presence of `_zod.def` only.
 */
export function zodSchemaOf(marked: StandardSchemaMarked): Result<ZodSchema, ZodReleaseRefused> {
  const read = releaseOf(marked);

  switch (read.state) {
    case "zod3":
      return err(ZOD_3);
    case "zod4":
      if (!isSupported(read.version)) {
        return err(
          Object.freeze({ kind: "ZodVersionNotSupported", found: read.version, supported: SUPPORTED_ZOD.range }),
        );
      }

      // SAFETY: the value has zod 4 internals (`_zod.def`, `_zod.version`) of a release inside `SUPPORTED_ZOD`. Each
      // schema of these releases, from each zod copy, has the internals that `ZodSchema` names.
      return ok(marked as unknown as ZodSchema);
    default:
      return unreachable(read, "zodSchemaOf");
  }
}

/** The release of a marked value: zod 4 internals with a release number, or none (zod 3). */
type ReleaseRead = { readonly state: "zod3" } | { readonly state: "zod4"; readonly version: ZodVersion };

const ZOD_3_READ: ReleaseRead = Object.freeze({ state: "zod3" });

function releaseOf(marked: StandardSchemaMarked): ReleaseRead {
  if (!("_zod" in marked) || !isObjectLike(marked._zod)) {
    return ZOD_3_READ;
  }

  const internals = marked._zod;

  if (!("def" in internals) || !("version" in internals) || !isObjectLike(internals.version)) {
    return ZOD_3_READ;
  }

  const versionObject = internals.version;

  if (!("major" in versionObject) || !("minor" in versionObject) || !("patch" in versionObject)) {
    return ZOD_3_READ;
  }

  const { major, minor, patch } = versionObject;

  return isNumber(major) && isNumber(minor) && isNumber(patch)
    ? { state: "zod4", version: Object.freeze({ major, minor, patch }) }
    : ZOD_3_READ;
}

/** Whether a release is inside `SUPPORTED_ZOD`. Pure. */
export function isSupported(version: ZodVersion): boolean {
  return compare(version, SUPPORTED_ZOD.lowest) >= 0 && compare(version, SUPPORTED_ZOD.below) < 0;
}

function compare(first: ZodVersion, second: ZodVersion): number {
  return first.major - second.major || first.minor - second.minor || first.patch - second.patch;
}
