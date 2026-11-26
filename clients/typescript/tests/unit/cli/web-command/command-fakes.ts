// Fakes of the I/O of the `web` and `metrics` commands: the output lines and the stop signals.

import type { CommandOutput, StopSignalName, StopSignals } from "../../../../horsies/src/cli/command-parts.ts";

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

        return undefined;
      },
      writeError: (line) => {
        errors.push(line);

        return undefined;
      },
    },
  };
}

/** Stop signals that the test sends, and the count of removals. */
export class FakeStopSignals {
  readonly #received = Promise.withResolvers<StopSignalName>();
  installed = 0;
  removed = 0;

  /** The factory that a command calls. */
  readonly listen = (): StopSignals => {
    this.installed += 1;

    return {
      received: this.#received.promise,
      remove: () => {
        this.removed += 1;

        return undefined;
      },
    };
  };

  /** Send `signal`. */
  send(signal: StopSignalName): void {
    this.#received.resolve(signal);
  }
}
