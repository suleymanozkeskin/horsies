// The channel of a seat isolate to the main thread of its child, and the core access of a worker thread.
//
// - A worker thread whose `workerData` holds the seat marker is a seat isolate. Its channel is made once, at the first
//   read in the thread (`Horsies.create` reads it for the app's core access): it parses the marker, makes the relay,
//   and listens on the port of the thread.
// - The channel gives each message of the main thread to its owner: `opReply` to the relay, `run`, `cancel`,
//   `bodyEnded` and `flush` to the runs of the seat once the isolate serves (`serve`).
// - A defect of the package in the isolate follows the defect rule of a worker process: the channel sends it to the
//   main thread of the child (`defect`), which aborts the child; then the isolate throws. A `run` before the serve, a
//   message outside the protocol, and any throw while the channel handles a message are such defects.
// - Another worker thread has no channel and no core.

import { isMainThread, parentPort, workerData } from "node:worker_threads";

import type { CoreAccess } from "../app/core-access.ts";
import { isObjectLike } from "../codec/values.ts";
import { raiseDefect, reportDefectsToMainThread } from "../native/call/defect.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import { ClientDefect, CoreDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import {
  type MainToSeat,
  parseMainToSeat,
  SEAT_PROTOCOL_VERSION,
  type SeatMarker,
  seatMarkerOf,
  type SeatToMain,
} from "./protocol.ts";
import { IsolateRelay } from "./relay.ts";

/** The port of a seat isolate to the main thread of its child. */
export interface IsolatePort {
  post(message: SeatToMain): void;
  /** Give each message that comes to `listener`. A message that is not an object is a defect of the package. */
  onMessage(listener: (message: object) => void): void;
}

/** What serves the runs of a seat: the `run`, `cancel`, `bodyEnded` and `flush` messages. */
export interface SeatRunHandler {
  run(message: Extract<MainToSeat, { readonly kind: "run" }>): void;
  cancel(message: Extract<MainToSeat, { readonly kind: "cancel" }>): void;
  bodyEnded(message: Extract<MainToSeat, { readonly kind: "bodyEnded" }>): void;
  flush(): void;
}

/** Whether the isolate serves runs yet. */
type RunsSlot = { readonly kind: "notServing" } | { readonly kind: "serving"; readonly handler: SeatRunHandler };

/** The channel of one seat isolate. */
export class SeatChannel {
  readonly marker: SeatMarker;
  readonly relay: IsolateRelay;
  readonly #port: IsolatePort;
  /** The defects sent to the main thread already: a defect that two paths report goes once. */
  readonly #reported = new WeakSet<CoreDefect>();
  #runs: RunsSlot = { kind: "notServing" };

  constructor(marker: SeatMarker, port: IsolatePort) {
    this.marker = marker;
    this.#port = port;
    this.relay = new IsolateRelay((message) => {
      port.post(message);
    }, marker.relayBounds);
    port.onMessage((message) => {
      this.#guarded(message);
    });
  }

  /** Send the defect `defect` to the main thread of the child: its defect rule aborts the child. Never throws. */
  reportDefect(defect: CoreDefect): void {
    if (this.#reported.has(defect)) {
      return;
    }

    this.#reported.add(defect);

    try {
      this.#port.post({ kind: "defect", protocol: SEAT_PROTOCOL_VERSION, code: defect.code, name: defect.name, message: defect.message });
    } catch {
      // The port of an ending isolate refuses posts; the throw that follows ends the isolate.
    }
  }

  /** Send `message` to the main thread. */
  post(message: SeatToMain): void {
    this.#port.post(message);
  }

  /** Give the runs of the seat to `handler`, once. A second serve is a defect: `ClientDefect`. */
  serve(handler: SeatRunHandler): void {
    switch (this.#runs.kind) {
      case "notServing":
        this.#runs = { kind: "serving", handler };

        return;
      case "serving":
        raiseDefect(new ClientDefect({ subject: "seat isolate", rule: "a seat isolate serves its runs once" }));
      default:
        unreachable(this.#runs, "SeatChannel.serve");
    }
  }

  #dispatch(message: MainToSeat): void {
    switch (message.kind) {
      case "opReply":
        this.relay.settle(message.requestId, message.answer);

        return;
      case "run":
        this.#handler(message.kind).run(message);

        return;
      case "cancel":
        this.#handler(message.kind).cancel(message);

        return;
      case "bodyEnded":
        this.#handler(message.kind).bodyEnded(message);

        return;
      case "flush":
        this.#handler(message.kind).flush();

        return;
      default:
        unreachable(message, "SeatChannel.dispatch");
    }
  }

  /** Handle one message; a throw there is a defect of the package: the defect rule, then the throw. */
  #guarded(message: object): void {
    try {
      this.#dispatch(parseMainToSeat(message));
    } catch (cause) {
      const defect = cause instanceof CoreDefect ? cause : channelDefect(cause);

      this.reportDefect(defect);

      throw defect;
    }
  }

  #handler(kind: string): SeatRunHandler {
    switch (this.#runs.kind) {
      case "serving":
        return this.#runs.handler;
      case "notServing":
        return raiseDefect(new ClientDefect({ subject: "seat isolate", rule: `the main thread sent ${kind} before the isolate served its runs` }));
      default:
        return unreachable(this.#runs, "SeatChannel.handler");
    }
  }
}

/** The channel of this thread: a seat isolate's, or none. */
export type ThreadChannel = { readonly kind: "seat"; readonly channel: SeatChannel } | { readonly kind: "notSeat" };

/** The channel of this thread, once read. */
type ChannelSlot = { readonly kind: "notRead" } | { readonly kind: "read"; readonly thread: ThreadChannel };

let channelSlot: ChannelSlot = { kind: "notRead" };

/**
 * The channel of this thread. The first read in a seat isolate makes it: it parses the marker and listens on the port
 * of the thread. A marker that is not well formed is a defect: `ClientDefect`.
 */
export function threadChannel(): ThreadChannel {
  switch (channelSlot.kind) {
    case "read":
      return channelSlot.thread;
    case "notRead": {
      const thread = channelOfThread();

      channelSlot = { kind: "read", thread };

      return thread;
    }

    default:
      return unreachable(channelSlot, "threadChannel");
  }
}

/** The core access of a worker thread: the relay of a seat isolate, or no core. Reads the channel of this thread. */
export function workerThreadCore(): CoreAccess {
  const thread = threadChannel();

  switch (thread.kind) {
    case "seat":
      return { kind: "seatRelay", relay: thread.channel.relay };
    case "notSeat":
      return { kind: "noCore" };
    default:
      return unreachable(thread, "workerThreadCore");
  }
}

function channelOfThread(): ThreadChannel {
  // `workerData` is the value that the starter of the thread gave: `null` on the main thread and when none was given.
  const data: unknown = workerData;

  if (isMainThread || !isObjectLike(data)) {
    return { kind: "notSeat" };
  }

  const read = seatMarkerOf(data);

  switch (read.kind) {
    case "notSeat":
      return read;
    case "seat": {
      const channel = new SeatChannel(read.marker, threadPort());

      reportDefectsToMainThread((defect) => {
        channel.reportDefect(defect);
      });

      return { kind: "seat", channel };
    }

    default:
      return unreachable(read, "channelOfThread");
  }
}

/** A throw that is not a defect class, while the channel handled a message: a defect of the package. */
function channelDefect(cause: unknown): ClientDefect {
  const facts = thrownFacts(cause);

  return new ClientDefect({ subject: "seat isolate", rule: `a message of the main thread threw ${facts.name}: ${facts.message}` });
}

/** The port of this worker thread. */
function threadPort(): IsolatePort {
  const port = parentPort;

  if (port === null) {
    throw new ClientDefect({ subject: "seat isolate", rule: "a worker thread has a port to the thread that started it" });
  }

  return {
    post: (message) => {
      port.postMessage(message);
    },
    onMessage: (listener) => {
      port.on("message", (message) => {
        if (!isObjectLike(message)) {
          raiseDefect(new ClientDefect({ subject: "seat protocol", rule: "a message of the main thread is not an object" }));
        }

        listener(message);
      });
    },
  };
}
