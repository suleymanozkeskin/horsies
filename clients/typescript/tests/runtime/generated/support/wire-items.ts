// Helpers of the event decoder tests: wire items with one item changed, and a refusal as text.

import type { EventDecodeError } from "../../../../horsies/src/_catalog/events.ts";
import type { WireJson } from "../../../../horsies/src/_catalog/wire_json.ts";

/** A copy of `items` with `item` at `index`. */
export function withItem(
  items: ReadonlyArray<WireJson>,
  index: number,
  item: WireJson,
): ReadonlyArray<WireJson> {
  return items.map((current, position) => (position === index ? item : current));
}

/** A short text of a refusal, for the message of a failed assertion. */
export function refusalText(error: EventDecodeError): string {
  switch (error.kind) {
    case "notAnEvent":
      return "not an event";
    case "unknownEventType":
      return `unknown event type ${error.tag}`;
    case "malformedEvent":
      return `${error.event} at [${error.path.join(".")}]: ${error.cause.kind}`;
    default: {
      const unreachable: never = error;

      return unreachable;
    }
  }
}
