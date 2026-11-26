// The stop signals of the worker: the first SIGTERM or SIGINT stops once; a later signal is only written.

import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { describe, test } from "node:test";

import { type SignalSource, type StopSignalName, stopOnSignals } from "../../../horsies/src/cli/signals.ts";

/** A signal source over an event emitter, so a test can send a signal. */
function fakeSource(): SignalSource & { send(signal: StopSignalName): void; listeners(): number } {
  const emitter = new EventEmitter();

  return {
    on: (signal, listener) => {
      emitter.on(signal, listener);
    },
    off: (signal, listener) => {
      emitter.off(signal, listener);
    },
    send: (signal) => {
      emitter.emit(signal);
    },
    listeners: () => emitter.listenerCount("SIGTERM") + emitter.listenerCount("SIGINT"),
  };
}

function watched(): { readonly source: ReturnType<typeof fakeSource>; readonly stops: number[]; readonly lines: string[] } {
  const source = fakeSource();
  const stops: number[] = [];
  const lines: string[] = [];

  stopOnSignals(source, "worker", () => stops.push(1), (line) => lines.push(line));

  return { source, stops, lines };
}

describe("the stop signals", () => {
  test("the first SIGTERM stops once", () => {
    const { source, stops, lines } = watched();

    source.send("SIGTERM");

    assert.equal(stops.length, 1);
    assert.deepEqual(lines, ["horsies worker: SIGTERM received; stopping after the drain"]);
  });

  test("the first SIGINT stops once", () => {
    const { source, stops } = watched();

    source.send("SIGINT");

    assert.equal(stops.length, 1);
  });

  test("a later signal is written and does not stop again", () => {
    const { source, stops, lines } = watched();

    source.send("SIGINT");
    source.send("SIGTERM");
    source.send("SIGINT");

    assert.equal(stops.length, 1);
    assert.deepEqual(lines.slice(1), [
      "horsies worker: SIGTERM received; the stop asked by SIGINT goes on",
      "horsies worker: SIGINT received; the stop asked by SIGINT goes on",
    ]);
  });

  test("remove takes the handlers off", () => {
    const source = fakeSource();
    const handlers = stopOnSignals(source, "worker", () => undefined, () => undefined);

    assert.equal(source.listeners(), 2);
    handlers.remove();
    assert.equal(source.listeners(), 0);
  });
});
