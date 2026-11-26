// Paths and readers of the files that the codec tests read: the core's stored-form vectors and the committed
// corpus of Python's texts.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

/** The core's stored-form vectors (`core/tests/fixtures/stored_forms/`). */
export const STORED_FORMS_DIR: string = fileURLToPath(
  new URL("../../../../../../core/tests/fixtures/stored_forms/", import.meta.url),
);

/** Python's texts, written by `corpus/make_corpus.py`. */
export const CORPUS_DIR: string = fileURLToPath(new URL("../corpus/", import.meta.url));

/** The source folder of the package. */
export const SOURCE_DIR: string = fileURLToPath(new URL("../../../../horsies/src/", import.meta.url));

/** The non-empty lines of a corpus file. */
export function corpusLines(fileName: string): readonly string[] {
  return readFileSync(`${CORPUS_DIR}${fileName}`, "utf8")
    .split("\n")
    .filter((line) => line.length > 0);
}

/** The runtime that runs the tests, for messages. */
export function runtimeLabel(): string {
  return process.versions.bun === undefined ? `node ${process.version}` : `bun ${process.versions.bun}`;
}
