// A child process of `child.test.ts`: it runs one case of the child host entry and writes its outcome as one line on
// stdout. A process serves as a child once, so each case runs in its own process.

import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import type { ChildHostPlan } from "../../../horsies/src/native/child/addon.ts";
import { loopTurned, serveChild } from "../../../horsies/src/native/child/child.ts";
import { manifestJson } from "./manifest.ts";

/** A descriptor number that no test process holds open. */
const CLOSED_DESCRIPTOR = "987";

/** A pid that is never the parent of a test process. */
const NOT_THE_PARENT = "1";

/** How long the `backstop` case blocks its JavaScript thread: past the backstop of the host thread. */
const BLOCK_PAST_BACKSTOP_MS = 6_000;

/** A plan with no task and no hook, for the arguments `processArguments`. */
function plan(processArguments: readonly string[]): ChildHostPlan {
  return {
    tasks: [],
    startHooks: [],
    producerManifestJson: manifestJson("postgresql://horsies@127.0.0.1:55432/horsies", false),
    hostname: "test-host",
    bodyFacts: "NO_SCOPE",
    processArguments: [...processArguments],
  };
}

const noPost = (): void => undefined;
const noHook = (): void => undefined;

/** Serve with `processArguments` and give the end as `<exit> <code>`. */
async function served(processArguments: readonly string[]): Promise<string> {
  const end = await serveChild(plan(processArguments), noPost, noHook);

  return `${end.exit} ${String(end.code)} ${String(end.flushBoundMs)}`;
}

/** Block the JavaScript thread for `milliseconds`. */
function block(milliseconds: number): void {
  const until = Date.now() + milliseconds;

  while (Date.now() < until) {
    // The host thread's backstop ends the process while this loop holds the thread.
  }
}

async function runCase(name: string | undefined): Promise<string> {
  switch (name) {
    case "socket-not-open":
      return served(["--horsies-child", CLOSED_DESCRIPTOR, String(process.ppid)]);
    case "parent-gone":
      return served(["--horsies-child", "3", NOT_THE_PARENT]);
    case "backstop": {
      const line = await served(["--horsies-child", CLOSED_DESCRIPTOR, String(process.ppid)]);

      process.stdout.write(`${line}\n`);
      block(BLOCK_PAST_BACKSTOP_MS);

      return "the backstop did not end the process";
    }

    case "second-serve": {
      await served(["--horsies-child", CLOSED_DESCRIPTOR, String(process.ppid)]);

      try {
        await served(["--horsies-child", CLOSED_DESCRIPTOR, String(process.ppid)]);

        return "no throw";
      } catch (cause) {
        return cause instanceof ClientDefect ? "ClientDefect" : String(cause);
      }
    }

    case "marks-before-serve":
      try {
        loopTurned();

        return "no throw";
      } catch (cause) {
        return cause instanceof ClientDefect ? "ClientDefect" : String(cause);
      }

    default:
      return `unknown case ${String(name)}`;
  }
}

const outcome = await runCase(process.argv[2]);

process.stdout.write(`${outcome}\n`);

// The JavaScript exit comes before the backstop of a served child.
process.exit(0);
