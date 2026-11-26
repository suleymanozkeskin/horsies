// The trace link of the monitoring page: an `http` or `https` URL template with one `{trace_id}`
// placeholder, which the page fills with the 32 hex digits of a task's trace id, for example
// `https://grafana.example.com/explore?traceId={trace_id}`. The core checks the template.

import { nativeValue } from "../native/call/envelope.ts";
import { unreachable } from "../native/call/never.ts";
import { webHandlerAddon } from "../native/facades/web-handler.ts";
import type { TraceLinkCheck, TraceLinkRefusalRecord } from "../native/facades/web-handler.ts";
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

/**
 * A template that the core refused: one variant per rule of the core, with the facts of the rule,
 * the template (`template`), and the core's text of the rule (`rule`).
 */
export type TraceLinkRefusal =
  /** The template has 0 bytes or more than 2,048 bytes: `bytes`, its UTF-8 byte count. */
  | { readonly kind: "length"; readonly bytes: number; readonly template: string; readonly rule: string }
  /** The template holds a space, a control or a non-ASCII byte: `byte`, the first such byte. */
  | { readonly kind: "character"; readonly byte: number; readonly template: string; readonly rule: string }
  /** The template does not start with `https://` or `http://`. */
  | { readonly kind: "scheme"; readonly template: string; readonly rule: string }
  /** The template has no host after its scheme. */
  | { readonly kind: "noHost"; readonly template: string; readonly rule: string }
  /** The template does not hold `{trace_id}` once: `count`, the times that it holds it. */
  | { readonly kind: "placeholderCount"; readonly count: number; readonly template: string; readonly rule: string }
  /** The template holds a `{` or `}` outside `{trace_id}`. */
  | { readonly kind: "strayBrace"; readonly template: string; readonly rule: string };

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
      return err(refusalOf(text, check.refusal, check.rule));
    default:
      return unreachable(check);
  }
}

/** The refusal of `template` by the core's `refusal` and its text `rule`. Pure. */
function refusalOf(template: string, refusal: TraceLinkRefusalRecord, rule: string): TraceLinkRefusal {
  switch (refusal.kind) {
    case "length":
      return { kind: "length", bytes: refusal.bytes, template, rule };
    case "character":
      return { kind: "character", byte: refusal.byte, template, rule };
    case "scheme":
      return { kind: "scheme", template, rule };
    case "noHost":
      return { kind: "noHost", template, rule };
    case "placeholderCount":
      return { kind: "placeholderCount", count: refusal.count, template, rule };
    case "strayBrace":
      return { kind: "strayBrace", template, rule };
    default:
      return unreachable(refusal);
  }
}
