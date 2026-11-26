// The event stream of the web handler over a fake frame source: frames, a heartbeat, the client's
// abort, the cancel of the body, the degraded frame, a source that throws, and `close()`.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { allowAll } from "../../../horsies/src/web/auth.ts";
import { eventStreamBody } from "../../../horsies/src/web/events.ts";
import { monitoringHandlerOver } from "../../../horsies/src/web/handler.ts";
import { CoreStopped } from "../../../horsies/src/thrown-errors.ts";
import { DEGRADED_FRAME, FakeFrameSource, fakeBridge, recordingPolicy } from "./support.ts";

/** The heartbeat of the fake source in these tests. */
const TEST_HEARTBEAT_MS = 20;

const decoder = new TextDecoder();

/** Read the next chunk of `reader` as text, or `undefined` at the end. */
async function nextText(reader: {
  read(): Promise<{ readonly done: boolean; readonly value?: Uint8Array | undefined }>;
}): Promise<string | undefined> {
  const read = await reader.read();

  return read.done ? undefined : decoder.decode(read.value);
}

describe("eventStreamBody", () => {
  test("gives the frames of the source in order, then ends with it", async () => {
    const source = new FakeFrameSource();
    const reader = eventStreamBody(source, new AbortController().signal).getReader();

    source.push("data: {\"topic\": \"tasks\", \"ids\": [\"a\"]}\n\n");
    source.push(": heartbeat\n\n");
    source.finish();

    assert.equal(await nextText(reader), "data: {\"topic\": \"tasks\", \"ids\": [\"a\"]}\n\n");
    assert.equal(await nextText(reader), ": heartbeat\n\n");
    assert.equal(await nextText(reader), undefined);
  });

  test("a quiet stream gets the heartbeat frames of the source", async () => {
    const source = new FakeFrameSource();
    const heartbeat = setInterval(() => source.push(": heartbeat\n\n"), TEST_HEARTBEAT_MS);
    const reader = eventStreamBody(source, new AbortController().signal).getReader();

    try {
      assert.equal(await nextText(reader), ": heartbeat\n\n");
      assert.equal(await nextText(reader), ": heartbeat\n\n");
    } finally {
      clearInterval(heartbeat);
      await reader.cancel();
    }
  });

  test("pulls one frame per read", async () => {
    const source = new FakeFrameSource();
    const reader = eventStreamBody(source, new AbortController().signal).getReader();

    source.push("data: 1\n\n");
    source.push("data: 2\n\n");
    await nextText(reader);

    assert.equal(source.nextCalls, 1);
    await reader.cancel();
  });

  test("the degraded frame comes, then the end", async () => {
    const source = new FakeFrameSource();
    const reader = eventStreamBody(source, new AbortController().signal).getReader();

    source.degrade();

    assert.equal(await nextText(reader), DEGRADED_FRAME);
    assert.equal(await nextText(reader), undefined);
    assert.equal(source.closeCalls, 1);
  });

  test("the client's abort closes the source, and a pending read ends", async () => {
    const source = new FakeFrameSource();
    const leaving = new AbortController();
    const reader = eventStreamBody(source, leaving.signal).getReader();
    const pending = nextText(reader);

    leaving.abort();

    assert.equal(await pending, undefined);
    assert.equal(source.closeCalls, 1);
  });

  test("a signal that aborted before the stream closes the source at once", async () => {
    const source = new FakeFrameSource();
    const reader = eventStreamBody(source, AbortSignal.abort()).getReader();

    assert.equal(source.closeCalls, 1);
    assert.equal(await nextText(reader), undefined);
  });

  test("the cancel of the body closes the source once", async () => {
    const source = new FakeFrameSource();
    const leaving = new AbortController();
    const reader = eventStreamBody(source, leaving.signal).getReader();

    await reader.cancel();
    leaving.abort();

    assert.equal(source.closeCalls, 1);
  });

  test("a source that throws closes itself and errors the body", async () => {
    const stopped = new CoreStopped({ subject: "listener", cause: "panic", stateAfter: "noChange" });
    let closed = 0;

    const source = {
      next: async () => Promise.reject(stopped),
      close: () => {
        closed += 1;
      },
    };

    const reader = eventStreamBody(source, new AbortController().signal).getReader();

    await assert.rejects(reader.read(), stopped);
    assert.equal(closed, 1);
  });
});

describe("the event stream route", () => {
  test("an allowed view answers the stream with the event stream headers", async () => {
    const { bridge, calls } = fakeBridge();
    const handler = monitoringHandlerOver(bridge, { auth: allowAll() });
    const response = await handler(new Request("http://monitoring.test/api/events"));

    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "text/event-stream; charset=utf-8");
    assert.equal(response.headers.get("cache-control"), "no-cache");
    assert.equal(response.headers.get("x-accel-buffering"), "no");
    assert.equal(calls.sources.length, 1);
    assert.deepEqual(calls.handled, []);
    await response.body?.cancel();
  });

  test("a refused view answers through the core with 403, with no subscription", async () => {
    const { bridge, calls } = fakeBridge();
    const { policy } = recordingPolicy(() => false, () => false);
    const handler = monitoringHandlerOver(bridge, { auth: policy });
    const response = await handler(new Request("http://monitoring.test/api/events"));

    assert.equal(response.status, 403);
    assert.equal(calls.sources.length, 0);
  });

  test("close() ends every open stream after the degraded frame", async () => {
    const { bridge, calls } = fakeBridge();
    const handler = monitoringHandlerOver(bridge, { auth: allowAll() });
    const first = await handler(new Request("http://monitoring.test/api/events"));
    const second = await handler(new Request("http://monitoring.test/api/events"));

    await handler.close();

    assert.equal(calls.closeEvents, 1);
    assert.equal(await first.text(), DEGRADED_FRAME);
    assert.equal(await second.text(), DEGRADED_FRAME);
  });

  test("the client's abort of the request closes its subscription", async () => {
    const { bridge, calls } = fakeBridge();
    const handler = monitoringHandlerOver(bridge, { auth: allowAll() });
    const leaving = new AbortController();
    const response = await handler(new Request("http://monitoring.test/api/events", { signal: leaving.signal }));

    leaving.abort();

    assert.equal(calls.sources[0]?.closeCalls, 1);
    await response.body?.cancel();
  });
});
