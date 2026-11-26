// The stop signals of a service command (the worker): the first `SIGTERM` or `SIGINT` asks the service to stop once;
// each later signal is written to stderr and does nothing more. The handlers stay installed until the run ends, so
// the default action of a later signal (the end of the process) does not run while the service drains. The addon
// installs no signal handler: the runtime of this process owns the signals.

import type { StopHandlers } from "../worker/run.ts";
import { unreachable } from "../wire/unreachable.ts";

/** A signal that stops a service command. */
export type StopSignalName = "SIGINT" | "SIGTERM";

/** The stop signals, in the order the handlers are installed. */
const STOP_SIGNALS: readonly StopSignalName[] = ["SIGTERM", "SIGINT"];

/** Where the handlers of the stop signals go: the process, or a fake in a test. */
export interface SignalSource {
  on(signal: StopSignalName, listener: () => void): void;
  off(signal: StopSignalName, listener: () => void): void;
}

/** The signals of this process. */
export function processSignals(): SignalSource {
  return {
    on: (signal, listener) => {
      process.on(signal, listener);
    },
    off: (signal, listener) => {
      process.off(signal, listener);
    },
  };
}

/** Whether a stop was asked for. */
type StopState = { readonly kind: "waiting" } | { readonly kind: "stopAsked"; readonly signal: StopSignalName };

/**
 * Call `requestStop` at the first stop signal of `source`, and write one line for each signal to `writeLine`. The
 * handlers stay until `remove()`. `command` names the command in the lines.
 */
export function stopOnSignals(
  source: SignalSource,
  command: string,
  requestStop: () => void,
  writeLine: (line: string) => void,
): StopHandlers {
  let state: StopState = { kind: "waiting" };

  const listeners = STOP_SIGNALS.map((signal) => {
    const listener = (): void => {
      switch (state.kind) {
        case "waiting":
          state = { kind: "stopAsked", signal };
          writeLine(`horsies ${command}: ${signal} received; stopping after the drain`);
          requestStop();

          return;
        case "stopAsked":
          writeLine(`horsies ${command}: ${signal} received; the stop asked by ${state.signal} goes on`);

          return;
        default:
          unreachable(state, "stopOnSignals");
      }
    };

    source.on(signal, listener);

    return [signal, listener] as const;
  });

  return {
    remove: () => {
      for (const [signal, listener] of listeners) {
        source.off(signal, listener);
      }
    },
  };
}
