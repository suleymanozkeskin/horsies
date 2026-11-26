// A child entry in the form of a CommonJS entry: it does not await child mode, and user code keeps a referenced handle
// open (a pool of the app). The serve fails. The process must end with 1: not 0, and no hang.

import { createHorsies } from "../../../../horsies/src/app.ts";
import { runChildMode } from "../../../../horsies/src/child/host.ts";
import { processEventHooks } from "../../../../horsies/src/child/process-events.ts";
import { DATABASE_URL, fakeEnvironment } from "../../app/support.ts";
import { ping } from "../../worker/support.ts";
import { FakeChildNative } from "../child-native.ts";
import { unusedSeatHost } from "../../seats/support.ts";

const created = createHorsies({ broker: { databaseUrl: DATABASE_URL }, tasks: [ping] }, fakeEnvironment());

if (!created.ok) {
  throw new Error("the fixture app was refused");
}

const native = new FakeChildNative();

native.serveChild = async () => Promise.reject(new RangeError("the host thread did not start"));

// The open handle of user code: it would keep the process alive.
setInterval(() => undefined, 1000);

void runChildMode(created.value, ["--horsies-child", "3", "4242"], {
  native,
  events: processEventHooks(),
  timer: () => () => undefined,
  culprit: { kind: "none" },
  settlement: { returned: () => undefined, threw: () => undefined },
  hostname: "fixture",
  seats: unusedSeatHost(),
  writeLine: (line) => {
    process.stderr.write(`${line}\n`);
  },
  wait: async (milliseconds) =>
    new Promise((resolve) => {
      setTimeout(resolve, milliseconds);
    }),
  exit: (code) => process.exit(code),
  reportDefect: (defect) => {
    process.stderr.write(`defect: ${defect.message}\n`);
  },
});
