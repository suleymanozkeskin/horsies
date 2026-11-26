// The `web` command: serve the monitoring web API and page of the app until `SIGINT` or `SIGTERM`.
//
// `<entry> web [--host 127.0.0.1] [--port 8600] [--auth none|trusted-header]
//              [--trusted-header X-Forwarded-User] [--enable-actions] [--trace-link TEMPLATE]
//              [--loglevel INFO]`
//
// Exit codes: 0 after an operator stop; 1 when the check refuses the app or the address does not
// bind; 2 for arguments that do not parse, `--auth none` on a host that is not a loopback host, a
// trusted header name that is not valid, and a trace link template that the core refuses.

import { isIPv4, isIPv6 } from "node:net";
import { parseArgs } from "node:util";

import { configureLogging } from "../logging.ts";
import { unreachable } from "../native/call/never.ts";
import { allowAll, trustedHeader, trustedHeaderRefusalText, viewOnly } from "../web/auth.ts";
import type { ActionsSwitch, MonitoringAuthPolicy } from "../web/auth.ts";
import { createMonitoringHandler } from "../web/handler.ts";
import type { MonitoringHandler, MonitoringHandlerApp, MonitoringHandlerOptions } from "../web/handler.ts";
import { serveHandler, serverRuntime } from "../web/serve.ts";
import { addressText } from "../web/server.ts";
import type { FetchHandler, ServeAddress, ServeOutcome } from "../web/server.ts";
import { parseTraceLink } from "../web/trace-link.ts";
import type { TraceLinkRefusal, TraceLinkTemplate } from "../web/trace-link.ts";
import { err, ok } from "../wire/result.ts";
import type { Result } from "../wire/result.ts";
import {
  DEFAULT_LOG_LEVEL,
  EXIT_FAILED,
  EXIT_STOPPED,
  EXIT_USAGE,
  listenForStopSignals,
  parseArgsUsageError,
  parseLogLevel,
  processOutput,
  usageError,
  usageErrorLine,
} from "./command-parts.ts";
import type { CliLogLevel, CommandExitCode, CommandOutput, StopSignals, UsageError } from "./command-parts.ts";

/** The default `--host`: this machine only. */
export const WEB_DEFAULT_HOST = "127.0.0.1";

/** The default `--port`. */
export const WEB_DEFAULT_PORT = 8600;

/** The default `--trusted-header`. */
export const WEB_DEFAULT_TRUSTED_HEADER = "X-Forwarded-User";

/** The largest port. */
const MAX_PORT = 65_535;

/** The flags of `web`, as `node:util` `parseArgs` reads them. */
const WEB_FLAG_OPTIONS = {
  host: { type: "string" },
  port: { type: "string" },
  auth: { type: "string" },
  "trusted-header": { type: "string" },
  "enable-actions": { type: "boolean" },
  "trace-link": { type: "string" },
  loglevel: { type: "string" },
} as const;

/** The `--auth` modes. */
export type WebAuthMode = "none" | "trustedHeader";

/** The `--trace-link` flag: absent, or the template text. */
export type TraceLinkFlag = { readonly kind: "noTraceLink" } | { readonly kind: "template"; readonly text: string };

/** The parsed flags of `web`. */
export interface WebFlags {
  readonly address: ServeAddress;
  readonly auth: WebAuthMode;
  readonly trustedHeaderName: string;
  readonly actions: ActionsSwitch;
  readonly traceLink: TraceLinkFlag;
  readonly logLevel: CliLogLevel;
}

/** The access of the served handler. */
export type WebAccess =
  | { readonly kind: "noAuthentication"; readonly policy: MonitoringAuthPolicy }
  | { readonly kind: "trustedHeader"; readonly policy: MonitoringAuthPolicy; readonly headerName: string };

/** The outcome of the check without the live step: passed, or the refusal report to print. */
export type CheckStepOutcome = { readonly kind: "passed" } | { readonly kind: "refused"; readonly report: string };

/** What `web` calls that does I/O or calls the addon. */
export interface WebCommandDeps {
  readonly output: CommandOutput;
  readonly setLogLevel: (level: CliLogLevel) => undefined;
  readonly checkStatic: () => Promise<CheckStepOutcome>;
  readonly parseTraceLink: (text: string) => Result<TraceLinkTemplate, TraceLinkRefusal>;
  readonly createHandler: (options: MonitoringHandlerOptions) => MonitoringHandler;
  readonly serve: (handler: FetchHandler, address: ServeAddress) => Promise<ServeOutcome>;
  readonly stopSignals: () => StopSignals;
}

/** The production dependencies of `web` for `app`, with the check step of the CLI runner. */
export function webCommandDeps(app: MonitoringHandlerApp, checkStatic: () => Promise<CheckStepOutcome>): WebCommandDeps {
  const output = processOutput();

  return {
    output,
    setLogLevel: (level) => {
      configureLogging({ levels: { default: level } });

      return undefined;
    },
    checkStatic,
    parseTraceLink,
    createHandler: (options) => createMonitoringHandler(app, options),
    serve: async (handler, address) =>
      serveHandler(serverRuntime(), handler, address, (failure) =>
        output.writeError(`horsies web: ${failure.method} ${failure.path} failed: ${failure.name}: ${failure.message}`),
      ),
    stopSignals: listenForStopSignals,
  };
}

/** Run `web` with the arguments after the command name. Resolves with the exit code. */
export async function runWebCommand(args: readonly string[], deps: WebCommandDeps): Promise<CommandExitCode> {
  const prepared = prepareWeb(args, deps.parseTraceLink);

  if (!prepared.ok) {
    deps.output.writeError(usageErrorLine("web", prepared.error));

    return EXIT_USAGE;
  }

  const { flags, access, traceLink } = prepared.value;

  if (access.kind === "trustedHeader") {
    deps.output.writeError(trustedHeaderWarning(access.headerName));
  }

  deps.setLogLevel(flags.logLevel);

  const check = await deps.checkStatic();

  switch (check.kind) {
    case "refused":
      deps.output.writeError(check.report);

      return EXIT_FAILED;
    case "passed":
      break;
    default:
      return unreachable(check);
  }

  const handler = deps.createHandler({
    auth: access.policy,
    actions: flags.actions,
    ...(traceLink === undefined ? {} : { ui: { traceLink } }),
  });

  return serveUntilStop(handler, flags.address, deps);
}

/** The checked flags of `web`, its access, and its trace link. */
interface PreparedWeb {
  readonly flags: WebFlags;
  readonly access: WebAccess;
  readonly traceLink: TraceLinkTemplate | undefined;
}

/** Every argument check of `web`, in order: the flags, the trace link, the access rule. */
function prepareWeb(
  args: readonly string[],
  check: (text: string) => Result<TraceLinkTemplate, TraceLinkRefusal>,
): Result<PreparedWeb, UsageError> {
  const flags = parseWebFlags(args);

  if (!flags.ok) {
    return flags;
  }

  const traceLink = checkedTraceLink(flags.value.traceLink, check);

  if (!traceLink.ok) {
    return traceLink;
  }

  const access = webAccess(flags.value);

  return access.ok ? ok({ flags: flags.value, access: access.value, traceLink: traceLink.value }) : access;
}

/** Serve `handler` on `address` until a stop signal; then close the handler, then the server. */
async function serveUntilStop(
  handler: MonitoringHandler,
  address: ServeAddress,
  deps: WebCommandDeps,
): Promise<CommandExitCode> {
  const signals = deps.stopSignals();
  const outcome = await deps.serve(handler, address);

  switch (outcome.kind) {
    case "bindFailed":
      signals.remove();
      deps.output.writeError(`could not bind the monitoring server to ${addressText(address)}: ${outcome.cause}`);
      await handler.close();

      return EXIT_FAILED;
    case "serving":
      break;
    default:
      return unreachable(outcome);
  }

  deps.output.writeError(`Serving horsies monitoring UI on http://${addressText(outcome.server.address)}`);

  const signal = await signals.received;

  deps.output.writeError(`horsies web: ${signal} received; closing the event streams, then the server`);

  try {
    await handler.close();
  } finally {
    // A close of the handler that throws (a stopped core) still closes the server.
    await outcome.server.close();
    signals.remove();
  }

  return EXIT_STOPPED;
}

/** The flags of `web`. Pure. */
export function parseWebFlags(args: readonly string[]): Result<WebFlags, UsageError> {
  let values: ReturnType<typeof parseWebArgs>["values"];

  try {
    values = parseWebArgs(args).values;
  } catch (cause) {
    return err(parseArgsUsageError(cause));
  }

  const port = parsePort(values.port ?? String(WEB_DEFAULT_PORT));

  if (!port.ok) {
    return port;
  }

  const auth = parseAuthMode(values.auth ?? "none");

  if (!auth.ok) {
    return auth;
  }

  const logLevel = parseLogLevel(values.loglevel ?? DEFAULT_LOG_LEVEL);

  if (!logLevel.ok) {
    return logLevel;
  }

  const traceLink = values["trace-link"];

  return ok({
    address: { host: values.host ?? WEB_DEFAULT_HOST, port: port.value },
    auth: auth.value,
    trustedHeaderName: values["trusted-header"] ?? WEB_DEFAULT_TRUSTED_HEADER,
    actions: values["enable-actions"] === true ? "enabled" : "disabled",
    traceLink: traceLink === undefined ? { kind: "noTraceLink" } : { kind: "template", text: traceLink },
    logLevel: logLevel.value,
  });
}

/** `parseArgs` over the flags of `web`: strict, no positional argument. Throws its usage error. */
function parseWebArgs(args: readonly string[]) {
  return parseArgs({ args: [...args], options: WEB_FLAG_OPTIONS, strict: true, allowPositionals: false });
}

/** A `--port` value: a whole number from 0 to 65535 (0: a free port). Pure. */
export function parsePort(text: string): Result<number, UsageError> {
  const port = /^[0-9]{1,5}$/u.test(text) ? Number(text) : Number.NaN;

  return port >= 0 && port <= MAX_PORT
    ? ok(port)
    : err(usageError(`--port must be a whole number from 0 to ${MAX_PORT}, not ${JSON.stringify(text)}`));
}

/** A `--auth` value. Pure. */
function parseAuthMode(text: string): Result<WebAuthMode, UsageError> {
  switch (text) {
    case "none":
      return ok("none");
    case "trusted-header":
      return ok("trustedHeader");
    default:
      return err(usageError(`--auth must be none or trusted-header, not ${JSON.stringify(text)}`));
  }
}

/** The template of `--trace-link`, checked by the core; a refusal is a usage error with the rule. */
function checkedTraceLink(
  flag: TraceLinkFlag,
  check: (text: string) => Result<TraceLinkTemplate, TraceLinkRefusal>,
): Result<TraceLinkTemplate | undefined, UsageError> {
  switch (flag.kind) {
    case "noTraceLink":
      return ok(undefined);
    case "template": {
      const checked = check(flag.text);

      return checked.ok ? checked : err(usageError(`invalid --trace-link: ${checked.error.rule}`));
    }

    default:
      return unreachable(flag);
  }
}

/**
 * The access of the flags. `--auth none` is allowed only on a loopback host: anyone who can reach
 * the port could read the task data. It gives `allowAll()` with `--enable-actions`, else
 * `viewOnly()`. Pure.
 */
export function webAccess(flags: WebFlags): Result<WebAccess, UsageError> {
  switch (flags.auth) {
    case "trustedHeader": {
      const policy = trustedHeader(flags.trustedHeaderName, { actions: flags.actions });

      return policy.ok
        ? ok({ kind: "trustedHeader", policy: policy.value, headerName: flags.trustedHeaderName })
        : err(usageError(`--trusted-header: ${trustedHeaderRefusalText(policy.error)}`));
    }

    case "none":
      return isLoopbackHost(flags.address.host)
        ? ok({ kind: "noAuthentication", policy: flags.actions === "enabled" ? allowAll() : viewOnly() })
        : err(usageError(exposedWithoutAuthenticationText(flags.address.host)));
    default:
      return unreachable(flags.auth);
  }
}

/** The refusal of `--auth none` on `host`. Pure. */
function exposedWithoutAuthenticationText(host: string): string {
  return `--auth none is only allowed on a loopback host; ${JSON.stringify(host)} is reachable from the network. Use --auth trusted-header with a proxy-set header.`;
}

/**
 * Whether a bind host reaches only this machine: `localhost`, an IPv4 address in 127.0.0.0/8, or
 * the IPv6 address ::1. A host that is not one of these, an IPv6 address with a zone included, is
 * taken as reachable from the network. Pure.
 */
export function isLoopbackHost(host: string): boolean {
  if (host === "localhost") {
    return true;
  }

  if (isIPv4(host)) {
    return host.split(".")[0] === "127";
  }

  return isIPv6(host) && !host.includes("%") && !host.includes(".") && isIpv6Loopback(host);
}

/** Whether an IPv6 address text with no zone and no IPv4 part is ::1. Pure. */
function isIpv6Loopback(host: string): boolean {
  const [head = "", tail = ""] = host.split("::");
  const headGroups = head === "" ? [] : head.split(":");
  const tailGroups = tail === "" ? [] : tail.split(":");
  const missing = 8 - headGroups.length - tailGroups.length;
  const groups = host.includes("::") ? [...headGroups, ...Array<string>(missing).fill("0"), ...tailGroups] : headGroups;

  return groups.length === 8 && groups.every((group, index) => Number.parseInt(group, 16) === (index === 7 ? 1 : 0));
}

/** The warning of `--auth trusted-header`, as the Python and Rust clients print it. Pure. */
export function trustedHeaderWarning(headerName: string): string {
  return `SECURITY: the reverse proxy in front of this server MUST strip or overwrite the ${headerName} header on incoming requests. A proxy that forwards a client-supplied header makes this mode trivially spoofable, and horsies cannot detect that.`;
}
