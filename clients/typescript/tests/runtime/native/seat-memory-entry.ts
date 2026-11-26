// A seat isolate entry for the memory cap check: an app with two `SHARED_THREAD` tasks and `cli.run(app)`.
//
// - `allocate`: keeps chunks of the JavaScript heap (1 MiB each) until the isolate reaches its memory cap. It stops at
//   `MAX_CHUNKS` (1 GiB) on its own, so a runtime that ignores the cap cannot grow past the watchdog's bound.
// - `probe`: returns 1.

import { cli, defineTask, h, Horsies } from "../../../horsies/src/index.ts";

/** The most chunks that `allocate` keeps: 1 GiB of the heap. */
const MAX_CHUNKS = 1024;

/** The numbers of one chunk: 131,072 doubles, 1 MiB. */
const CHUNK_NUMBERS = 131_072;

const allocate = defineTask({ name: "allocate", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 60_000 });

const probe = defineTask({ name: "probe", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

// The isolate connects nowhere: the URL is never opened.
const databaseUrl = { kind: "value", value: "postgresql://postgres@127.0.0.1:55432/horsies" } as const;

const created = Horsies.create({ broker: { databaseUrl }, tasks: [allocate, probe] });

if (!created.ok) {
  throw new Error(`the fixture app was refused: ${created.error.refusals.map((refused) => refused.message).join("; ")}`);
}

const app = created.value;
const kept: number[][] = [];

app.implement(allocate, () => {
  for (let chunk = 0; chunk < MAX_CHUNKS; chunk += 1) {
    kept.push(Array.from({ length: CHUNK_NUMBERS }, (_, index) => index + 0.5));
  }

  return allocate.ok(kept.length);
});

app.implement(probe, () => probe.ok(1));

await cli.run(app);
