// The entry of the TypeScript side of the parity runs (`crossclient/parity/runner.py`), on Node or on Bun:
//
//   HORSIES_PARITY_DATABASE_URL=postgresql://... HORSIES_NATIVE_ADDON_PATH=/abs/horsies.<platform>.node \
//       <node|bun> clients/typescript/tests/parity/src/main.ts <command>
//
// - `send <scenario>`: the send process of the scenario (`send.ts`). Exit 0 when every step was written; 1 for a
//   refused step, or a refused app; 2 for a name that no scenario has.
// - Any other command goes to `cli.run(app)`: `worker --profile=<name>`, the worker's children (`--horsies-child`),
//   and `scheduler`.
//
// Every line before the command runs in each child too: the worker starts each child with this entry, and the child
// builds the same app from the same environment. A seat isolate of a `SHARED_THREAD` child runs this entry on a worker
// thread, where no addon loads, so only the main thread of a process loads the dev build of the addon. The entry
// imports the source entry of the package.

import { isMainThread } from "node:worker_threads";

import { cli } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildApp, DATABASE_URL_ENV } from "./app.ts";
import { entryCommand, EXIT_REFUSED, runSendCommand, type SendOutput } from "./send.ts";

if (isMainThread) {
  // The addon of the main thread of each process is the dev build that HORSIES_NATIVE_ADDON_PATH names.
  void requireDevAddon();
}

/** The lines of the send command: stdout for the step lines, stderr for a refusal. */
const PROCESS_OUTPUT: SendOutput = {
  line: (text) => {
    process.stdout.write(`${text}\n`);
  },
  error: (text) => {
    process.stderr.write(`${text}\n`);
  },
};

/** The exit status of the command of this process. */
async function entryExit(): Promise<number> {
  const databaseUrl = process.env[DATABASE_URL_ENV] ?? "";

  if (databaseUrl === "") {
    PROCESS_OUTPUT.error(`horsies parity: ${DATABASE_URL_ENV} is not set`);

    return EXIT_REFUSED;
  }

  const app = buildApp(databaseUrl);

  if (!app.ok) {
    PROCESS_OUTPUT.error(`horsies parity: Horsies.create refused the parity app: ${app.error}`);

    return EXIT_REFUSED;
  }

  const command = entryCommand(process.argv.slice(2));

  switch (command.kind) {
    case "send":
      return runSendCommand(app.value, command.scenario, PROCESS_OUTPUT);
    case "cli":
      return cli.run(app.value);
    default: {
      const unexpected: never = command;

      throw new TypeError(`the entry: a command outside its type: ${String(unexpected)}`);
    }
  }
}

process.exitCode = await entryExit();
