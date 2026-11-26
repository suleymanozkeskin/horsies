// Fakes of the I/O of the `web` and `metrics` commands: the output lines and the stop signals.

import type { SignalSource, StopSignalName } from "../../../../horsies/src/cli/signals.ts";
import type { CommandOutput } from "../../../../horsies/src/cli/usage.ts";

/** An output that keeps its lines. */
export function recordedOutput(): { readonly output: CommandOutput; readonly out: string[]; readonly errors: string[] } {
  const out: string[] = [];
  const errors: string[] = [];

  return {
    out,
    errors,
    output: {
      writeOut: (line) => {
        out.push(line);
      },
      writeError: (line) => {
        errors.push(line);
      },
    },
  };
}

/** A signal source that the test drives: it keeps each handler and sends a signal to its handlers. */
export class FakeStopSignals {
  readonly #listeners = new Map<StopSignalName, (() => void)[]>();
  readonly #firstHandler = Promise.withResolvers<void>();

  /** Settles when a command installs its first handler. */
  readonly installedOnce: Promise<void> = this.#firstHandler.promise;

  /** The source that a command takes. */
  readonly source: SignalSource = {
    on: (signal, listener) => {
      this.#listeners.set(signal, [...(this.#listeners.get(signal) ?? []), listener]);
      this.#firstHandler.resolve();
    },
  };

  /** How many handlers a command installed. */
  get installed(): number {
    return [...this.#listeners.values()].reduce((count, listeners) => count + listeners.length, 0);
  }

  /** Send `signal` to its handlers. */
  send(signal: StopSignalName): void {
    for (const listener of this.#listeners.get(signal) ?? []) {
      listener();
    }
  }
}
