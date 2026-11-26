// The body of a monitoring request, read up to a bound, and its form at the addon boundary.

import { Buffer } from "node:buffer";

import { thrownFacts } from "../native/call/thrown-facts.ts";

/** The outcome of a bounded body read. */
export type BodyRead =
  | { readonly kind: "read"; readonly bytes: Uint8Array }
  | { readonly kind: "failed"; readonly cause: string };

/**
 * The first `limit` bytes of `body`, or all of a shorter body. The read stops at `limit` bytes and
 * cancels the rest of the body, so it holds at most `limit` bytes. A body that the transfer could
 * not deliver (the client went away, a bad chunk) is `failed` with the cause.
 */
export async function readBoundedBody(body: ReadableStream<Uint8Array> | null, limit: number): Promise<BodyRead> {
  if (body === null) {
    return { kind: "read", bytes: new Uint8Array(0) };
  }

  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;

  try {
    while (total < limit) {
      const read = await reader.read();

      if (read.done) {
        return { kind: "read", bytes: joined(chunks, total) };
      }

      const chunk = read.value.subarray(0, limit - total);

      chunks.push(chunk);
      total += chunk.byteLength;
    }

    // The bytes past the bound are not read. A cancel that fails changes nothing for the answer,
    // which holds the bytes up to the bound.
    await reader.cancel().then(noAnswer, noAnswer);

    return { kind: "read", bytes: joined(chunks, total) };
  } catch (cause) {
    return { kind: "failed", cause: thrownFacts(cause).message };
  } finally {
    reader.releaseLock();
  }
}

/** The explicit discard of the outcome of a cancel. */
function noAnswer(): undefined {
  return undefined;
}

/** The chunks as one array of `total` bytes. Pure. */
function joined(chunks: readonly Uint8Array[], total: number): Uint8Array {
  const bytes = new Uint8Array(total);
  let offset = 0;

  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return bytes;
}

/** `bytes` as lower-case hexadecimal text, the form of bytes at the addon boundary. Pure. */
export function hexOf(bytes: Uint8Array): string {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("hex");
}
