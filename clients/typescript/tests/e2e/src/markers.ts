// Marker files: how a task in a worker child and the test that sent it coordinate across processes.
//
// The test creates the directory and gives its path to the worker in `HORSIES_TS_E2E_MARKER_DIR`; every child
// inherits it. A marker is a file in that directory. An exclusive create (`open` with the flag `wx`) tells the first
// call from a later one, so a marker counts attempts across children and retries. A plain `mkdir` is not exclusive on
// every CI runner image, so no marker is a directory.

import { appendFileSync, closeSync, existsSync, mkdtempSync, openSync, readdirSync, readFileSync, rmSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";

/** The variable that names the marker directory of a worker and its children. */
export const MARKER_DIR_ENV = "HORSIES_TS_E2E_MARKER_DIR";

/** The most attempts that `nextAttempt` counts: a task of the suite has at most 20 retries, so 21 runs. */
export const MAX_ATTEMPTS = 21;

/** Why a marker operation failed in a task. */
export type MarkerError =
  | { readonly kind: "directoryNotSet" }
  | { readonly kind: "io"; readonly cause: string }
  | { readonly kind: "tooManyAttempts" };

/** The text of a marker error, for a task error message. Pure. */
export function markerErrorText(error: MarkerError): string {
  switch (error.kind) {
    case "directoryNotSet":
      return `${MARKER_DIR_ENV} is not set`;
    case "io":
      return error.cause;
    case "tooManyAttempts":
      return `more than ${String(MAX_ATTEMPTS)} attempts`;
  }
}

/** Whether an exclusive create made the marker or found it. */
export type CreateOnce = "created" | "existed";

/** The path of marker `name` in the directory of `MARKER_DIR_ENV`. */
export function markerPath(name: string): Result<string, MarkerError> {
  const directory = process.env[MARKER_DIR_ENV] ?? "";

  return directory === "" ? err({ kind: "directoryNotSet" }) : ok(join(directory, name));
}

/** Create marker `name` with `content`, or find that it exists. Effects: an exclusive create of the file. */
export function createOnce(name: string, content: string): Result<CreateOnce, MarkerError> {
  const path = markerPath(name);

  if (!path.ok) {
    return path;
  }

  try {
    const descriptor = openSync(path.value, "wx");

    try {
      writeSync(descriptor, content);
    } finally {
      closeSync(descriptor);
    }

    return ok("created");
  } catch (cause) {
    return isCode(cause, "EEXIST") ? ok("existed") : err({ kind: "io", cause: `create ${path.value}: ${String(cause)}` });
  }
}

/** The attempt number of this call for `token`: 1 for the first call, then 2, 3, ... Each creates `<token>-attempt-<n>`. */
export function nextAttempt(token: string): Result<number, MarkerError> {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
    const created = createOnce(`${token}-attempt-${String(attempt)}`, "");

    if (!created.ok) {
      return created;
    }

    if (created.value === "created") {
      return ok(attempt);
    }
  }

  return err({ kind: "tooManyAttempts" });
}

/** Append `line` to marker `name`, which is created when absent. Effects: an append to the file. */
export function appendLine(name: string, line: string): Result<undefined, MarkerError> {
  const path = markerPath(name);

  if (!path.ok) {
    return path;
  }

  try {
    appendFileSync(path.value, `${line}\n`);

    return ok(undefined);
  } catch (cause) {
    return err({ kind: "io", cause: `append to ${path.value}: ${String(cause)}` });
  }
}

/** The wall clock in milliseconds since 1970, for marker lines that a test compares across processes. */
export function unixMillis(): number {
  return Date.now();
}

function isCode(cause: unknown, code: string): boolean {
  return cause instanceof Error && "code" in cause && cause.code === code;
}

/** The marker directory of one test, on the test side. */
export class MarkerDir {
  readonly path: string;

  private constructor(path: string) {
    this.path = path;
  }

  /** A new empty directory under the system temporary directory. Throws when the create fails: the test cannot run. */
  static create(): MarkerDir {
    return new MarkerDir(mkdtempSync(join(tmpdir(), "horsies-ts-e2e-")));
  }

  /** The path of marker `name`. */
  marker(name: string): string {
    return join(this.path, name);
  }

  /** The text of marker `name`, or `absent`. */
  read(name: string): { readonly kind: "found"; readonly text: string } | { readonly kind: "absent" } {
    const path = this.marker(name);

    return existsSync(path) ? { kind: "found", text: readFileSync(path, "utf8") } : { kind: "absent" };
  }

  /** The names of the markers that start with `prefix`. */
  names(prefix: string): readonly string[] {
    return readdirSync(this.path).filter((name) => name.startsWith(prefix));
  }

  /** The number of markers whose name starts with `prefix`. */
  count(prefix: string): number {
    return this.names(prefix).length;
  }

  /** Remove the directory and every marker in it. */
  remove(): void {
    rmSync(this.path, { recursive: true, force: true });
  }
}
