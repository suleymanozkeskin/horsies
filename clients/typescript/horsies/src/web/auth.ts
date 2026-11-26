// The access policy of the monitoring handler, and the three shipped policies.
//
// horsies never owns identity. A deployment gives a policy that answers two questions per request:
// may this caller read monitoring data, and may it act. The handler asks only the verdicts that a
// route needs, in order, and `canAct` only after `canView` allowed.
//
// Action requests carry a second check that is not identity: the `X-Horsies-Intent: action`
// header, which the core checks. A browser cannot set it on a cross-site form post.

import { unreachable } from "../native/call/never.ts";
import { err, ok } from "../wire/result.ts";
import type { Result } from "../wire/result.ts";

/**
 * The access policy of a monitoring handler. User code, called per request. Only an answer of
 * `true` allows; every other answer refuses. A throw or a rejection answers the request with 500.
 *
 * The `request` of a verdict has the method, the URL, the headers and the signal of the incoming
 * request, and no body: the core reads the body.
 */
export interface MonitoringAuthPolicy {
  /** Whether this request may read monitoring data. */
  canView(request: Request): boolean | Promise<boolean>;
  /** Whether this request may cancel, pause or resume. Asked only after `canView` allowed. */
  canAct(request: Request): boolean | Promise<boolean>;
}

/** Whether the action routes run. */
export type ActionsSwitch = "enabled" | "disabled";

/** The options of `trustedHeader`. */
export interface TrustedHeaderOptions {
  /** `enabled`: a request with the header may act too. */
  readonly actions: ActionsSwitch;
}

/** Why `trustedHeader` refused a header name. */
export type TrustedHeaderRefused =
  | { readonly kind: "blankHeaderName" }
  | { readonly kind: "invalidHeaderName"; readonly headerName: string; readonly rule: string };

/** The rule of a header name: an HTTP token (RFC 9110, section 5.6.2). */
export const HEADER_NAME_RULE = "a header name is one or more of the characters A-Z a-z 0-9 ! # $ % & ' * + - . ^ _ ` | ~";

/** The characters of an HTTP token. */
const HEADER_NAME_PATTERN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/u;

/** The separator of the values of a repeated header: `Headers.get` joins them with `", "`. */
const HEADER_VALUE_SEPARATOR = ",";

/**
 * Allow every request to read and to act. For a mount that sits behind the host application's own
 * authentication: choosing it states that something in front of the handler authenticated the
 * caller.
 */
export function allowAll(): MonitoringAuthPolicy {
  return { canView: () => true, canAct: () => true };
}

/** Allow every request to read, and refuse every action. */
export function viewOnly(): MonitoringAuthPolicy {
  return { canView: () => true, canAct: () => false };
}

/**
 * Allow a request whose header `headerName` has a value with no blank member between its commas: the
 * reverse proxy in front of the handler authenticates the caller and sets the header. With
 * `actions: "enabled"` such a request may act too. A request with the header two times, one of them
 * blank, is refused (`Headers.get` joins the values with `", "`).
 *
 * SECURITY: the proxy must strip or overwrite this header on incoming requests. A proxy that
 * forwards a header that the client set makes this policy trivial to spoof, and horsies cannot
 * detect that.
 *
 * Refuses a blank name and a name that is not an HTTP token.
 */
export function trustedHeader(
  headerName: string,
  options: TrustedHeaderOptions,
): Result<MonitoringAuthPolicy, TrustedHeaderRefused> {
  if (headerName.trim() === "") {
    return err({ kind: "blankHeaderName" });
  }

  if (!HEADER_NAME_PATTERN.test(headerName)) {
    return err({ kind: "invalidHeaderName", headerName, rule: HEADER_NAME_RULE });
  }

  const identified = (request: Request): boolean => identityPresent(request.headers.get(headerName));

  switch (options.actions) {
    case "enabled":
      return ok({ canView: identified, canAct: identified });
    case "disabled":
      return ok({ canView: identified, canAct: () => false });
    default:
      return unreachable(options.actions);
  }
}

/**
 * Whether `value`, the value of the identity header that `Headers.get` gives, names an identity: the
 * header is present and no member between its commas is blank. Python and Rust read the first value
 * of a repeated header and refuse a blank one; the joined value holds that blank value as a blank
 * member. An identity that holds a comma passes when no member is blank. Pure.
 */
function identityPresent(value: string | null): boolean {
  return value === null ? false : value.split(HEADER_VALUE_SEPARATOR).every((member) => member.trim() !== "");
}

/** The text of a refusal of `trustedHeader`. Pure. */
export function trustedHeaderRefusalText(refused: TrustedHeaderRefused): string {
  switch (refused.kind) {
    case "blankHeaderName":
      return "the trusted header needs a header name";
    case "invalidHeaderName":
      return `the trusted header name ${JSON.stringify(refused.headerName)} is not valid: ${refused.rule}`;
    default:
      return unreachable(refused);
  }
}
