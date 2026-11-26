// A seat isolate entry: the spy on native module loads first, then an app with three `SHARED_THREAD` tasks and
// `cli.run(app)`, as an app entry runs in each seat isolate. The test runs it in a `worker_threads` isolate with the
// seat marker and plays the main thread of the child.
//
// - `probe`: the number of native module loads in this isolate.
// - `spin`: a sync body that holds the isolate until its cancel cell holds a cause, at most `SPIN_BOUND_MS`; 1 when the
//   cause is DEADLINE, 0 at the bound.
// - `send_and_wait`: sends `probe`, then waits for its result; the text names the send and the read error.
// - `exit_now`: ends its isolate with `process.exit(3)`.
// - `throw_later`: a callback that it scheduled throws, which ends its isolate with that error.

import { dlopenFiles } from "./dlopen-spy.ts";

import { cli, defineTask, h, Horsies } from "../../../../horsies/src/index.ts";

/** The longest hold of `spin`, in milliseconds. */
const SPIN_BOUND_MS = 5000;

const probe = defineTask({ name: "probe", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const spin = defineTask({ name: "spin", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const sendAndWait = defineTask({
  name: "send_and_wait",
  input: {},
  output: h.string(),
  execution: "SHARED_THREAD",
  timeoutMs: 5000,
});

// The isolate connects nowhere: the URL is never opened.
const databaseUrl = { kind: "value", value: "postgresql://postgres@127.0.0.1:55432/horsies" } as const;

const exitNow = defineTask({ name: "exit_now", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const throwLater = defineTask({ name: "throw_later", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const created = Horsies.create({ broker: { databaseUrl }, tasks: [probe, spin, sendAndWait, exitNow, throwLater] });

if (!created.ok) {
  throw new Error(`the fixture app was refused: ${created.error.refusals.map((refused) => refused.message).join("; ")}`);
}

const app = created.value;
const sleepCell = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));

app.implement(probe, () => probe.ok(dlopenFiles.length));

app.implement(spin, (_input, runtime) => {
  for (let waited = 0; waited < SPIN_BOUND_MS; waited += 1) {
    const state = runtime.cancelState();

    if (state.kind === "requested") {
      return spin.ok(state.cause === "DEADLINE" ? 1 : 0);
    }

    // A sync sleep of 1 ms: the isolate's event loop does not turn while the body holds it.
    Atomics.wait(sleepCell, 0, 0, 1);
  }

  return spin.ok(0);
});

app.implement(sendAndWait, async () => {
  const sent = await app.send(probe, {});

  if (!sent.ok) {
    return sendAndWait.ok(`send failed: ${sent.error.code}`);
  }

  const read = await sent.value.get({ timeoutMs: 100 });

  return sendAndWait.ok(`sent ${sent.value.taskId}; ${read.ok ? "read a result" : `read error ${read.error.kind}`}`);
});

app.implement(exitNow, () => {
  process.exit(3);
});

app.implement(throwLater, async () => {
  setTimeout(() => {
    throw new RangeError("a late throw");
  }, 0);

  return new Promise(() => undefined);
});

await cli.run(app);
