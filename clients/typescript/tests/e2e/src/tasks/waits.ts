// The waits of the task bodies: a wait that holds the thread (as a blocking Rust body or a Python `time.sleep`), and
// a wait on the event loop that a cancel signal may end.

/** How long a body that ignores its deadline waits: far past every deadline of the suite. */
export const PAST_THE_DEADLINE_MS = 3_600_000;

/** The cell that `holdThread` waits on: no code writes it, so each wait ends at its time limit. */
const IDLE_CELL = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));

/** Hold the thread for `milliseconds`: no callback, timer or Promise of this thread runs meanwhile. */
export function holdThread(milliseconds: number): void {
  // The only outcome of a wait on a cell that no code writes is `timed-out`.
  const outcome = Atomics.wait(IDLE_CELL, 0, 0, milliseconds);

  if (outcome !== "timed-out") {
    throw new TypeError(`holdThread: a wait on the idle cell ended with ${outcome}`);
  }
}

/** How an awaited wait ended. */
export type WaitEnd = { readonly kind: "slept" } | { readonly kind: "aborted"; readonly reason: string };

/** Wait `milliseconds` on the event loop, or until `signal` fires; its reason's `cause` is in the end. */
export async function sleepFor(milliseconds: number, signal: AbortSignal | "noSignal"): Promise<WaitEnd> {
  if (signal !== "noSignal" && signal.aborted) {
    return { kind: "aborted", reason: causeOf(signal) };
  }

  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      resolve({ kind: "slept" });
    }, milliseconds);

    if (signal !== "noSignal") {
      signal.addEventListener(
        "abort",
        () => {
          clearTimeout(timer);
          resolve({ kind: "aborted", reason: causeOf(signal) });
        },
        { once: true },
      );
    }
  });
}

/** The `cause` of the reason of a fired signal (`{ cause: "DEADLINE" }`), or a text that names what it is. */
export function causeOf(signal: AbortSignal): string {
  const reason: unknown = signal.reason;

  if (reason instanceof Object && "cause" in reason) {
    return String(reason.cause);
  }

  return `a reason with no cause: ${String(reason)}`;
}

/** The runtime of this process. */
export type RuntimeName = "node" | "bun";

/** The runtime of this process, from `process.versions`. */
export function runtimeName(): RuntimeName {
  return process.versions["bun"] === undefined ? "node" : "bun";
}
