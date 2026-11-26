// The exit rule of a task child: the telemetry flush, at most `CHILD_EXIT_FLUSH_BOUND_MS`, then the end of the process
// with the code. A child never waits for its event loop to empty: a provider's pool may stay open.
//
// The plan gives the same bound to the host thread, whose backstop ends the process 1 s after it when the JavaScript
// thread does not exit (it is blocked, for example by a start hook past its bound).

/** The longest wait for the telemetry flush before the child ends its process. */
export const CHILD_EXIT_FLUSH_BOUND_MS = 1000;

/** What the exit rule does that has effects. */
export interface ExitDeps {
  /** The flush of the body observer. */
  flush(): Promise<void>;
  /** Wait `milliseconds`; the wait must not keep the process alive. */
  wait(milliseconds: number): Promise<void>;
  /** End the process with `code`. */
  exit(code: number): never;
}

/** How the wait before the exit ended. */
type FlushEnd = "flushed" | "flushFailed" | "boundReached";

/** Await the flush, at most the flush bound, then end the process with `code`. A flush that rejects is left. */
export async function exitAfterFlush(code: number, deps: ExitDeps): Promise<never> {
  const flushed: Promise<FlushEnd> = deps.flush().then(
    (): FlushEnd => "flushed",
    (): FlushEnd => "flushFailed",
  );

  const bound: Promise<FlushEnd> = deps.wait(CHILD_EXIT_FLUSH_BOUND_MS).then((): FlushEnd => "boundReached");

  await Promise.race([flushed, bound]);

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
