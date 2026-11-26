// A local npm registry of the packed tarballs of one run, for the install checks: npm and Bun install `horsies` from it
// and choose its platform package as they do from the npm registry. Run with Node or Bun:
//
//   node tests/packages/registry.ts --packages <folder with index.json and the tarballs> --port <port>
//
// It serves `GET /<name>` (the package document of each name in `index.json`) and `GET /-/tarballs/<file>`, on
// 127.0.0.1 only, and answers 404 to any other request. It runs until it is stopped. Exit status: 2 when the
// arguments or the index do not read.

import { readFileSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { join } from "node:path";
import { parseArgs } from "node:util";

import { isArray, isPlainObject, isString } from "../../horsies/src/codec/values.ts";
import type { Result } from "../../horsies/src/wire/result.ts";
import type { ParsedJson } from "../public-api/package.ts";
import type { JsonObject } from "./manifest.ts";

/** The most tarballs that the registry serves. */
const TARBALL_COUNT_BOUND = 32;

/** The largest tarball that the registry reads into memory. */
const TARBALL_SIZE_BOUND_BYTES = 268_435_456;

/** The path prefix of the tarballs. */
const TARBALL_PREFIX = "/-/tarballs/";

/** One tarball of the index: what the registry needs to serve it. */
export type RegistryEntry = {
  readonly name: string;
  readonly version: string;
  readonly filename: string;
  readonly integrity: string;
  readonly shasum: string;
  readonly manifest: JsonObject;
};

export type IndexRefused =
  | { readonly kind: "NotAList" }
  | { readonly kind: "TooManyTarballs"; readonly count: number; readonly bound: number }
  | { readonly kind: "EntryForm"; readonly index: number }
  | { readonly kind: "RepeatedName"; readonly name: string };

/** The entries of `index.json`, as `pack.ts` writes it. Pure. */
export function registryEntriesOf(document: ParsedJson): Result<readonly RegistryEntry[], IndexRefused> {
  if (!isArray(document)) {
    return { ok: false, error: { kind: "NotAList" } };
  }

  if (document.length > TARBALL_COUNT_BOUND) {
    return { ok: false, error: { kind: "TooManyTarballs", count: document.length, bound: TARBALL_COUNT_BOUND } };
  }

  const entries: RegistryEntry[] = [];

  for (const [index, entry] of document.entries()) {
    if (!isPlainObject(entry)) {
      return { ok: false, error: { kind: "EntryForm", index } };
    }

    const { name, version, filename, integrity, shasum, manifest } = entry;

    if (!isString(name) || !isString(version) || !isString(filename) || !isString(integrity) || !isString(shasum) || !isPlainObject(manifest)) {
      return { ok: false, error: { kind: "EntryForm", index } };
    }

    if (entries.some((known) => known.name === name)) {
      return { ok: false, error: { kind: "RepeatedName", name } };
    }

    // SAFETY: the members of a parsed JSON object are parsed JSON.
    entries.push({ name, version, filename, integrity, shasum, manifest: manifest as JsonObject });
  }

  return { ok: true, value: entries };
}

/** The package document of one entry, as the npm registry answers `GET /<name>`. Pure. */
export function packumentOf(entry: RegistryEntry, baseUrl: string): JsonObject {
  const version = {
    ...entry.manifest,
    _id: `${entry.name}@${entry.version}`,
    dist: { tarball: `${baseUrl}${TARBALL_PREFIX}${entry.filename}`, integrity: entry.integrity, shasum: entry.shasum },
  };

  return { name: entry.name, "dist-tags": { latest: entry.version }, versions: { [entry.version]: version } };
}

/** What a request path asks for. */
export type RequestTarget =
  | { readonly kind: "packument"; readonly name: string }
  | { readonly kind: "tarball"; readonly filename: string }
  | { readonly kind: "unknown" };

/** The target of a request path: `/<name>` (a scope's `/` may be `%2f`) or `/-/tarballs/<file>`. Pure. */
export function requestTargetOf(path: string): RequestTarget {
  let decoded: string;

  try {
    decoded = decodeURIComponent(path.split("?")[0] ?? "");
  } catch {
    return { kind: "unknown" };
  }

  if (decoded.startsWith(TARBALL_PREFIX)) {
    const filename = decoded.slice(TARBALL_PREFIX.length);

    return filename.length > 0 && !filename.includes("/") ? { kind: "tarball", filename } : { kind: "unknown" };
  }

  const name = decoded.slice(1);

  return /^(?:@[a-z0-9-]+\/)?[a-z0-9][a-z0-9._-]*$/u.test(name) ? { kind: "packument", name } : { kind: "unknown" };
}

/** A tarball that the registry does not read into memory. */
export type TarballTooLarge = { readonly filename: string; readonly sizeBytes: number; readonly boundBytes: number };

/** The tarball files of the entries, read once. Effects: reads the files. */
function readTarballs(folder: string, entries: readonly RegistryEntry[]): Result<ReadonlyMap<string, Uint8Array>, TarballTooLarge> {
  const files = new Map<string, Uint8Array>();

  for (const entry of entries) {
    const path = join(folder, entry.filename);
    const sizeBytes = statSync(path).size;

    if (sizeBytes > TARBALL_SIZE_BOUND_BYTES) {
      return { ok: false, error: { filename: entry.filename, sizeBytes, boundBytes: TARBALL_SIZE_BOUND_BYTES } };
    }

    files.set(entry.filename, readFileSync(path));
  }

  return { ok: true, value: files };
}

/** The answer to one request: its status, content type and body. */
export type RegistryAnswer = { readonly status: 200 | 404; readonly contentType: string; readonly body: string | Uint8Array };

const NOT_FOUND: RegistryAnswer = { status: 404, contentType: "application/json", body: '{"error":"not found"}' };

/** The answer to a request target, from the package documents and the tarballs. Pure. */
export function answerOf(
  target: RequestTarget,
  packuments: ReadonlyMap<string, string>,
  tarballs: ReadonlyMap<string, Uint8Array>,
): RegistryAnswer {
  switch (target.kind) {
    case "packument": {
      const packument = packuments.get(target.name);

      return packument === undefined ? NOT_FOUND : { status: 200, contentType: "application/json", body: packument };
    }

    case "tarball": {
      const tarball = tarballs.get(target.filename);

      return tarball === undefined ? NOT_FOUND : { status: 200, contentType: "application/octet-stream", body: tarball };
    }

    case "unknown":
      return NOT_FOUND;
  }
}

/** Serve the entries until the process is stopped. Effects: listens on 127.0.0.1:`port`; writes one line per request. */
function serve(entries: readonly RegistryEntry[], tarballs: ReadonlyMap<string, Uint8Array>, port: number): void {
  const baseUrl = `http://127.0.0.1:${port}`;
  const packuments = new Map(entries.map((entry) => [entry.name, JSON.stringify(packumentOf(entry, baseUrl))]));

  const server = createServer((request, response) => {
    const target: RequestTarget = request.method === "GET" ? requestTargetOf(request.url ?? "/") : { kind: "unknown" };
    const answer = answerOf(target, packuments, tarballs);

    response.writeHead(answer.status, { "content-type": answer.contentType }).end(answer.body);
    console.log(`${request.method ?? "?"} ${request.url ?? "?"} ${answer.status}`);
  });

  server.listen(port, "127.0.0.1", () => {
    console.log(`serving ${entries.length} packages on ${baseUrl}`);
  });
}

/** The highest TCP port. */
const PORT_BOUND = 65_535;

/** Read the index and serve it; the exit status of a refused start. Effects: as `serve`. */
function main(args: readonly string[]): number {
  let values: { readonly packages?: string | undefined; readonly port?: string | undefined };

  try {
    values = parseArgs({ args: [...args], options: { packages: { type: "string" }, port: { type: "string" } }, strict: true }).values;
  } catch (cause) {
    console.error(cause instanceof Error ? cause.message : String(cause));

    return 2;
  }

  const port = Number(values.port);

  if (values.packages === undefined || !Number.isInteger(port) || port < 1 || port > PORT_BOUND) {
    console.error("give --packages <folder> and --port <1 to 65535>");

    return 2;
  }

  // SAFETY: JSON.parse gives a parsed JSON document.
  const entries = registryEntriesOf(JSON.parse(readFileSync(join(values.packages, "index.json"), "utf8")) as ParsedJson);

  if (!entries.ok) {
    console.error(`index.json does not read: ${JSON.stringify(entries.error)}`);

    return 2;
  }

  const tarballs = readTarballs(values.packages, entries.value);

  if (!tarballs.ok) {
    console.error(`${tarballs.error.filename} has ${tarballs.error.sizeBytes} bytes, above the bound of ${tarballs.error.boundBytes}`);

    return 2;
  }

  serve(entries.value, tarballs.value, port);

  return 0;
}

if (import.meta.main) {
  process.exitCode = main(process.argv.slice(2));
}
