// The trace link of the monitoring page: an `http` or `https` URL template with one `{trace_id}`
// placeholder, which the page fills with the 32 hex digits of a task's trace id, for example
// `https://grafana.example.com/explore?traceId={trace_id}`. The core checks the template.

import { nativeValue } from "../native/call/envelope.ts";
import { unreachable } from "../native/call/never.ts";
import { webHandlerAddon } from "../native/facades/web-handler.ts";
import type { TraceLinkCheck } from "../native/facades/web-handler.ts";
import { err, ok } from "../wire/result.ts";
import type { Result } from "../wire/result.ts";

declare const traceLinkTemplateBrand: unique symbol;

/** A trace link template that the core accepted. Only `parseTraceLink` makes one. */
export interface TraceLinkTemplate {
  /** The template text. */
  readonly text: string;
  readonly [traceLinkTemplateBrand]: "TraceLinkTemplate";
}

/**
 * The parser of trace link templates, as the Rust client's `TraceLinkTemplate::parse`: the core checks the text.
 * Starts no core.
 */
export const TraceLinkTemplate: {
  readonly parse: (text: string) => Result<TraceLinkTemplate, TraceLinkRefusal>;
} = Object.freeze({ parse: (text: string) => parseTraceLink(text) });

/** A template that the core refused. `rule` names the rule that it breaks. */
export interface TraceLinkRefusal {
  readonly kind: "traceLinkRefused";
  readonly template: string;
  readonly rule: string;
}

/**
 * The trace link template of `text`, checked by the core: 1 to 2,048 bytes of printable ASCII with
 * no space, an `http://` or `https://` scheme, a host, one `{trace_id}`, and no other brace.
 * Starts no core.
 */
export function parseTraceLink(text: string): Result<TraceLinkTemplate, TraceLinkRefusal> {
  return traceLinkOf(
    text,
    nativeValue("checkTraceLinkTemplate", () => webHandlerAddon().checkTraceLinkTemplate(text)),
  );
}

/** The template of `text` by the answer `check` of the core. Pure. */
export function traceLinkOf(text: string, check: TraceLinkCheck): Result<TraceLinkTemplate, TraceLinkRefusal> {
  switch (check.kind) {
    case "accepted":
      // SAFETY: the brand is a type-only marker; the core accepted the text just above.
      return ok({ text } as TraceLinkTemplate);
    case "refused":
      return err({ kind: "traceLinkRefused", template: text, rule: check.rule });
    default:
      return unreachable(check);
  }
}
