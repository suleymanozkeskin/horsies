// The bounded body read, the hexadecimal form of a body, and the trace link answer.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { hexOf, readBoundedBody } from "../../../horsies/src/web/request-body.ts";
import { traceLinkOf } from "../../../horsies/src/web/trace-link.ts";

/** A body of `chunks` chunks of `size` bytes, and the count of chunks that it gave. */
function chunkedBody(chunks: number, size: number): { readonly body: ReadableStream<Uint8Array>; readonly pulled: () => number } {
  let given = 0;

  const body = new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        if (given === chunks) {
          controller.close();

          return;
        }

        given += 1;
        controller.enqueue(new Uint8Array(size).fill(given));
      },
    },
    { highWaterMark: 0 },
  );

  return { body, pulled: () => given };
}

describe("readBoundedBody", () => {
  test("no body is an empty body", async () => {
    assert.deepEqual(await readBoundedBody(null, 8), { kind: "read", bytes: new Uint8Array(0) });
  });

  test("a body under the bound is read whole", async () => {
    const { body } = chunkedBody(2, 3);

    assert.deepEqual(await readBoundedBody(body, 8), { kind: "read", bytes: Uint8Array.of(1, 1, 1, 2, 2, 2) });
  });

  test("a body over the bound is cut at the bound and the rest is not read", async () => {
    const { body, pulled } = chunkedBody(100, 3);
    const read = await readBoundedBody(body, 7);

    assert.deepEqual(read, { kind: "read", bytes: Uint8Array.of(1, 1, 1, 2, 2, 2, 3) });
    assert.equal(pulled(), 3);
  });

  test("a body that fails is failed with the cause", async () => {
    const body = new ReadableStream<Uint8Array>({
      pull() {
        throw new Error("connection reset");
      },
    });

    assert.deepEqual(await readBoundedBody(body, 8), { kind: "failed", cause: "connection reset" });
  });

  test("a body that is locked is failed with the cause", async () => {
    const { body } = chunkedBody(2, 3);
    const _held = body.getReader();
    const read = await readBoundedBody(body, 8);

    assert.equal(read.kind, "failed");
    assert.ok(read.kind === "failed" && read.cause !== "", "the cause is named");
  });
});

describe("hexOf", () => {
  test("gives lower-case hexadecimal text of the bytes of the view only", () => {
    const bytes = Uint8Array.of(0x00, 0x0a, 0xff, 0x7b, 0x7d);

    assert.equal(hexOf(bytes), "000aff7b7d");
    assert.equal(hexOf(bytes.subarray(3)), "7b7d");
    assert.equal(hexOf(new Uint8Array(0)), "");
  });
});

describe("traceLinkOf", () => {
  test("an accepted template keeps its text", () => {
    const template = traceLinkOf("https://grafana.test/explore?traceId={trace_id}", { kind: "accepted" });

    assert.ok(template.ok);
    assert.equal(template.value.text, "https://grafana.test/explore?traceId={trace_id}");
  });

  test("a refused template names the rule", () => {
    assert.deepEqual(traceLinkOf("ftp://x/{trace_id}", { kind: "refused", rule: "the template does not start with https:// or http://" }), {
      ok: false,
      error: {
        kind: "traceLinkRefused",
        template: "ftp://x/{trace_id}",
        rule: "the template does not start with https:// or http://",
      },
    });
  });
});
