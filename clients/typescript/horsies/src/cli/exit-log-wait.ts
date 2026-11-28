// The wait for the core log records before a process ends itself with `process.exit`.
//
// `process.exit` ends the process at once: Node and Bun do not run the Node-API cleanup hooks of the addon there, so
// the drain of the log channel in that hook does not run. A record that waits in the channel, or a batch that the
// forwarder handed to the JavaScript sink, is lost at the exit. So `cli.run` and the exit rule of a child wait, bounded,
// until the forwarder handed every record on. The event loop runs during the wait, so the JavaScript sink takes its
// batches.

import type { LogBacklog } from "../native/worker/addon.ts";
import { workerLogBacklog } from "../native/worker/worker.ts";

/** The longest wait for the core log records before the process ends, in milliseconds. */
export const EXIT_LOG_WAIT_MS = 2000;

/** The pause between two reads of the log backlog, in milliseconds. */
export const EXIT_LOG_POLL_MS = 10;

/** The most reads of the log backlog in one wait: the wait ends at this count or at `EXIT_LOG_WAIT_MS`, the first. */
export const MAX_EXIT_LOG_POLLS: number = EXIT_LOG_WAIT_MS / EXIT_LOG_POLL_MS;

/** What the wait for the log records reads and does. */
export interface LogWaitDeps {
  /** Whether core log records wait in the channel. Does not wait. */
  logBacklog(): LogBacklog;
  /** Wait `milliseconds`, letting the event loop run. */
  pause(milliseconds: number): Promise<void>;
  /** A monotonic clock, in milliseconds. */
  now(): number;
}

/**
 * Wait while core log records wait in the channel and the forwarder hands them on: until `EXIT_LOG_WAIT_MS` passed by
 * the clock, or `MAX_EXIT_LOG_POLLS` reads, the first. Records with no forwarder (`noForwarder`) end the wait at once.
 */
export async function waitForLogRecords(deps: LogWaitDeps): Promise<void> {
  const deadline = deps.now() + EXIT_LOG_WAIT_MS;

  for (let poll = 0; poll < MAX_EXIT_LOG_POLLS && deps.now() < deadline && deps.logBacklog() === "waiting"; poll += 1) {
    await deps.pause(EXIT_LOG_POLL_MS);
  }
}

/** The log backlog, the timers and the clock of this process. The reads load the addon. */
export function processLogWaitDeps(): LogWaitDeps {
  return {
    logBacklog: workerLogBacklog,
    pause: async (milliseconds) =>
      new Promise((resolve) => {
        setTimeout(resolve, milliseconds);
      }),
    now: () => performance.now(),
  };
}
