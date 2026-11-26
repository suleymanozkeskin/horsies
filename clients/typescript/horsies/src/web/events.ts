// The event stream of the monitoring handler (`GET /api/events`): the core's frame texts as a
// `text/event-stream` body.
//
// - The body pulls one frame per call of the frame source; a frame is read only when the server
//   reads the body.
// - The core sends a heartbeat frame after its heartbeat with no event, the degraded frame when its
//   listener cannot be trusted, and then the end.
// - The abort of the request's signal and the cancel of the body close the frame source: its
//   pending and every later step is the end, and the core drops the subscription.

import { unreachable } from "../native/call/never.ts";
import type { MonitoringEventStep } from "../native/facades/web-handler.ts";

/** The headers of the event stream response, as the Python and Rust clients send them. */
export const EVENT_STREAM_HEADERS: Readonly<Record<string, string>> = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache",
  "x-accel-buffering": "no",
};

/** The frames of one event stream. */
export interface EventFrameSource {
  /** The next frame, or the end. Throws only the package's process-state errors and defects. */
  next(): Promise<MonitoringEventStep>;
  /** End the stream: a pending and every later `next()` answers the end. Returns at once. */
  close(): void;
}

/**
 * The body of one event stream: a stream that pulls one frame of `source` per read and ends with
 * the source. The abort of `signal` and the cancel of the body close `source`. A `next()` that
 * throws closes `source` and errors the body with the thrown value.
 */
export function eventStreamBody(source: EventFrameSource, signal: AbortSignal): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  const closing = sourceCloser(source, signal);

  return new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        let step: MonitoringEventStep;

        try {
          step = await source.next();
        } catch (cause) {
          closing.close();

          throw cause;
        }

        switch (step.kind) {
          case "frame":
            controller.enqueue(encoder.encode(step.text));

            return;
          case "ended":
            closing.close();
            controller.close();

            return;
          default:
            unreachable(step);
        }
      },
      cancel() {
        closing.close();
      },
    },
    { highWaterMark: 0 },
  );
}

/** The one close of a frame source, by its owner or by the abort of `signal`. */
interface SourceCloser {
  close(): void;
}

/** Close `source` once: at the first `close()` or at the abort of `signal`. */
function sourceCloser(source: EventFrameSource, signal: AbortSignal): SourceCloser {
  let state: "open" | "closed" = "open";

  const close = (): void => {
    switch (state) {
      case "closed":
        return;
      case "open":
        state = "closed";
        signal.removeEventListener("abort", close);
        source.close();

        return;
      default:
        unreachable(state);
    }
  };

  if (signal.aborted) {
    close();
  } else {
    signal.addEventListener("abort", close, { once: true });
  }

  return { close };
}
