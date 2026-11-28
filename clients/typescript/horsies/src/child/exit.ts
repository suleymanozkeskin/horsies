// The exit rule of a child: the telemetry flush and the wait for the core log records, together at most
// `CHILD_EXIT_FLUSH_BOUND_MS`, then the end of the process with the code. A child never waits for its event loop to
// empty: a provider's pool may stay open. `process.exit` does not drain the log channel (`cli/exit-log-wait.ts`), so
// without the wait a record that the core wrote just before the end is lost: for example the failure line of a start
// hook.
//
// The plan gives the same bound to the host thread, whose backstop ends the process 1 s after it when the JavaScript
// thread does not exit (it is blocked, for example by a start hook past its bound).

/** The longest wait for the telemetry flush before the child ends its process. */
export const CHILD_EXIT_FLUSH_BOUND_MS = 1000;

/** What the exit rule does that has effects. */
export interface ExitDeps {
  /** The flush of the body observer. */
  flush(): Promise<void>;
  /** Wait, bounded, while core log records wait in the channel (`waitForLogRecords`). */
  logRecords(): Promise<void>;
  /** Wait `milliseconds`; the wait must not keep the process alive. */
  wait(milliseconds: number): Promise<void>;
  /** End the process with `code`. */
  exit(code: number): never;
}

/** How one wait before the exit ended. */
type WaitEnd = "settled" | "rejected";

/** How the waits before the exit ended: both settled, or the bound came first. */
type ExitWaitEnd = "waitsSettled" | "boundReached";

/** `waiting` settled: resolved or rejected. A rejection is left. */
async function settledEnd(waiting: Promise<void>): Promise<WaitEnd> {
  return waiting.then(
    (): WaitEnd => "settled",
    (): WaitEnd => "rejected",
  );
}

/**
 * Await the flush and the wait for the log records, together at most the flush bound, then end the process with
 * `code`. A flush or a log wait that rejects is left.
 */
export async function exitAfterFlush(code: number, deps: ExitDeps): Promise<never> {
  const waits: Promise<ExitWaitEnd> = Promise.all([settledEnd(deps.flush()), settledEnd(deps.logRecords())]).then(
    (): ExitWaitEnd => "waitsSettled",
  );

  const bound: Promise<ExitWaitEnd> = deps.wait(CHILD_EXIT_FLUSH_BOUND_MS).then((): ExitWaitEnd => "boundReached");

  await Promise.race([waits, bound]);

  return deps.exit(code);
}

/** The state of the end of a child: running, or ending with the first code. */
type EndState = { readonly kind: "running" } | { readonly kind: "ending"; readonly ending: Promise<never> };

/**
 * The end of a child, once: the first call runs the exit rule with its code; a later call (a second uncaught throw
 * during the flush) waits for the same end.
 */
export class ChildEnd {
  #state: EndState = { kind: "running" };
  readonly #deps: ExitDeps;

  constructor(deps: ExitDeps) {
    this.#deps = deps;
  }

  /** End the process with `code`, or wait for the end that runs. */
  async end(code: number): Promise<never> {
    switch (this.#state.kind) {
      case "running": {
        const ending = exitAfterFlush(code, this.#deps);

        this.#state = { kind: "ending", ending };

        return ending;
      }

      case "ending":
        return this.#state.ending;
      default: {
        const unexpected: never = this.#state;

        throw new TypeError(`ChildEnd: an end state outside its type: ${String(unexpected)}`);
      }
    }
  }
}
