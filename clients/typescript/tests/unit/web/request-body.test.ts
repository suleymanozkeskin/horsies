// The bounded body read, the hexadecimal form of a body, and the trace link answer.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { hexOf, readBoundedBody } from "../../../horsies/src/web/request-body.ts";
import type { TraceLinkCheck } from "../../../horsies/src/native/facades/web-handler.ts";
import { traceLinkOf } from "../../../horsies/src/web/trace-link.ts";
import type { TraceLinkRefusal } from "../../../horsies/src/web/trace-link.ts";

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

  /** One refusal of the core per rule: the template, the core's answer, and the refusal that the caller gets. */
  const refusals: readonly {
    readonly template: string;
    readonly check: TraceLinkCheck;
    readonly refusal: TraceLinkRefusal;
  }[] = [
    {
      template: "",
      check: { kind: "refused", refusal: { kind: "length", bytes: 0 }, rule: "the template has 0 bytes" },
      refusal: { kind: "length", bytes: 0, template: "", rule: "the template has 0 bytes" },
    },
    {
      template: "https://x/a b/{trace_id}",
      check: { kind: "refused", refusal: { kind: "character", byte: 0x20 }, rule: "the template holds the byte 0x20" },
      refusal: { kind: "character", byte: 0x20, template: "https://x/a b/{trace_id}", rule: "the template holds the byte 0x20" },
    },
    {
      template: "ftp://x/{trace_id}",
      check: { kind: "refused", refusal: { kind: "scheme" }, rule: "the template does not start with https:// or http://" },
      refusal: {
        kind: "scheme",
        template: "ftp://x/{trace_id}",
        rule: "the template does not start with https:// or http://",
      },
    },
    {
      template: "https:///{trace_id}",
      check: { kind: "refused", refusal: { kind: "noHost" }, rule: "the template has no host after its scheme" },
      refusal: { kind: "noHost", template: "https:///{trace_id}", rule: "the template has no host after its scheme" },
    },
    {
      template: "https://x/trace",
      check: { kind: "refused", refusal: { kind: "placeholderCount", count: 0 }, rule: "the template holds {trace_id} 0 times" },
      refusal: { kind: "placeholderCount", count: 0, template: "https://x/trace", rule: "the template holds {trace_id} 0 times" },
    },
    {
      template: "https://x/{span_id}/{trace_id}",
      check: { kind: "refused", refusal: { kind: "strayBrace" }, rule: "the template holds a { or } outside {trace_id}" },
      refusal: {
        kind: "strayBrace",
        template: "https://x/{span_id}/{trace_id}",
        rule: "the template holds a { or } outside {trace_id}",
      },
    },
  ];

  for (const { template, check, refusal } of refusals) {
    test(`a ${refusal.kind} refusal keeps its facts, the template and the rule`, () => {
      assert.deepEqual(traceLinkOf(template, check), { ok: false, error: refusal });
    });
  }
});
