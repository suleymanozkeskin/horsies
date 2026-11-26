// The file operations that the commands of the `horsies` tool share: the cause of a failed system call, a read with a
// size bound, and the folder of the installed package. No module of this folder loads the native addon.

import { closeSync, openSync, readSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { isString } from "../codec/values.ts";
import { err, ok, type Result } from "../wire/result.ts";

/** The cause of a system call that failed: the error code of the system (`ENOENT`, `EACCES`, ...) and its text. */
export type IoCause = { readonly code: string; readonly message: string };

/** A system error of Node: an `Error` with a text `code`. */
type SystemError = Error & { readonly code: string };

function isSystemError(cause: unknown): cause is SystemError {
  return cause instanceof Error && "code" in cause && isString(cause.code);
}

/**
 * The cause of a throw of a system call of `node:fs` or `node:child_process`. A throw that is not a system error is a
 * defect, and it is thrown again. Pure.
 */
export function ioCauseOf(cause: unknown): IoCause {
  if (!isSystemError(cause)) {
    throw cause;
  }

  return { code: cause.code, message: cause.message };
}

/** The result of `call`, or the cause of the system error that it throws. Effects: those of `call`. */
export function attempt<T>(call: () => T): Result<T, IoCause> {
  try {
    return ok(call());
  } catch (cause) {
    return err(ioCauseOf(cause));
  }
}

/** Why a bounded read gave no bytes. */
export type BoundedReadError =
  | { readonly kind: "tooLarge"; readonly boundBytes: number }
  | { readonly kind: "io"; readonly cause: IoCause };

/** The text of a failed bounded read, for a message. Pure. */
export function boundedReadText(error: BoundedReadError): string {
  switch (error.kind) {
    case "tooLarge":
      return `the file is larger than ${error.boundBytes} bytes`;
    case "io":
      return error.cause.message;
  }
}

/** The size of one read call of `readBounded`, in bytes. */
const READ_CHUNK_BYTES = 65_536;

/**
 * The bytes of the file at `path`, at most `boundBytes` of them. Effects: reads the file. Errors: `tooLarge` for a
 * file of more than `boundBytes` bytes; `io` for a file that cannot be opened or read.
 */
export function readBounded(path: string, boundBytes: number): Result<Buffer, BoundedReadError> {
  const opened = attempt(() => openSync(path, "r"));

  if (!opened.ok) {
    return err({ kind: "io", cause: opened.error });
  }

  try {
    const buffer = Buffer.alloc(boundBytes + 1);
    let length = 0;

    while (length <= boundBytes) {
      const read = attempt(() => readSync(opened.value, buffer, length, Math.min(READ_CHUNK_BYTES, boundBytes + 1 - length), null));

      if (!read.ok) {
        return err({ kind: "io", cause: read.error });
      }

      if (read.value === 0) {
        return ok(buffer.subarray(0, length));
      }

      length += read.value;
    }

    return err({ kind: "tooLarge", boundBytes });
  } finally {
    closeSync(opened.value);
  }
}

/**
 * The folder of the installed `horsies` package: the folder of its `package.json` and of its skill folder. This module
 * is `src/tools/files.ts` in the source and `dist/tools/files.js` in the package, two folders below the package
 * folder in both. Pure.
 */
export function packageFolder(): string {
  return fileURLToPath(new URL("../../", import.meta.url));
}
