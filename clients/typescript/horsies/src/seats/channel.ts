// The channel of a seat isolate to the main thread of its child, and the core access of a worker thread.
//
// - A worker thread whose `workerData` holds the seat marker is a seat isolate. Its channel is made once, at the first
//   read in the thread (`Horsies.create` reads it for the app's core access): it parses the marker, makes the relay,
//   and listens on the port of the thread.
// - The channel gives each message of the main thread to its owner: `opReply` to the relay, `run` and `cancel` to the
//   runs of the seat once the isolate serves (`serve`). A `run` or a `cancel` before the serve, and a message outside
//   the protocol, are defects of the package: the throw ends the isolate.
// - Another worker thread has no channel and no core.

import { isMainThread, parentPort, workerData } from "node:worker_threads";

import type { CoreAccess } from "../app/core-access.ts";
import { isObjectLike } from "../codec/values.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type MainToSeat, parseMainToSeat, type SeatMarker, seatMarkerOf, type SeatToMain } from "./protocol.ts";
import { IsolateRelay } from "./relay.ts";

/** The port of a seat isolate to the main thread of its child. */
export interface IsolatePort {
  post(message: SeatToMain): void;
  /** Give each message that comes to `listener`. A message that is not an object is a defect of the package. */
  onMessage(listener: (message: object) => void): void;
}

/** What serves the runs of a seat: the `run` and the `cancel` messages. */
export interface SeatRunHandler {
  run(message: Extract<MainToSeat, { readonly kind: "run" }>): void;
  cancel(message: Extract<MainToSeat, { readonly kind: "cancel" }>): void;
}

/** Whether the isolate serves runs yet. */
type RunsSlot = { readonly kind: "notServing" } | { readonly kind: "serving"; readonly handler: SeatRunHandler };

/** The channel of one seat isolate. */
export class SeatChannel {
  readonly marker: SeatMarker;
  readonly relay: IsolateRelay;
  readonly #port: IsolatePort;
  #runs: RunsSlot = { kind: "notServing" };

  constructor(marker: SeatMarker, port: IsolatePort) {
    this.marker = marker;
    this.#port = port;
    this.relay = new IsolateRelay((message) => {
      port.post(message);
    });
    port.onMessage((message) => {
      this.#dispatch(parseMainToSeat(message));
    });
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
        throw new ClientDefect({ subject: "seat isolate", rule: "a seat isolate serves its runs once" });
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
      default:
        unreachable(message, "SeatChannel.dispatch");
    }
  }

  #handler(kind: string): SeatRunHandler {
    switch (this.#runs.kind) {
      case "serving":
        return this.#runs.handler;
      case "notServing":
        throw new ClientDefect({ subject: "seat isolate", rule: `the main thread sent ${kind} before the isolate served its runs` });
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
    case "seat":
      return { kind: "seat", channel: new SeatChannel(read.marker, threadPort()) };
    default:
      return unreachable(read, "channelOfThread");
  }
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
          throw new ClientDefect({ subject: "seat protocol", rule: "a message of the main thread is not an object" });
        }

        listener(message);
      });
    },
  };
}
