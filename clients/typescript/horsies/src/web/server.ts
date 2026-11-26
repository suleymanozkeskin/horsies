// The HTTP server of the `web` command: the types that the Node and the Bun servers share.

/** A Fetch-API handler. */
export type FetchHandler = (request: Request) => Promise<Response>;

/** The address to bind: a host name or an IP address, and a port from 0 to 65535 (0: a free port). */
export interface ServeAddress {
  readonly host: string;
  readonly port: number;
}

/** The address that the server holds: the bound port, which is never 0. */
export interface BoundAddress {
  readonly host: string;
  readonly port: number;
}

/** A server that serves until its close. */
export interface RunningServer {
  readonly address: BoundAddress;
  /**
   * Stop new connections, wait for the requests in flight, then end. Past `SERVER_CLOSE_GRACE_MS`
   * the connections that are still open are ended. Resolves when the server ended.
   */
  close(): Promise<undefined>;
}

/** How a start of a server ended. */
export type ServeOutcome =
  | { readonly kind: "serving"; readonly server: RunningServer }
  | { readonly kind: "bindFailed"; readonly address: ServeAddress; readonly cause: string };

/**
 * A handler failure that the server answered with 500: a throw or a rejection of the handler. The
 * server reports it here, with the method and the path of the request.
 */
export type HandlerFailureReport = (failure: HandlerFailure) => undefined;

/** One handler failure. */
export interface HandlerFailure {
  readonly method: string;
  readonly path: string;
  readonly name: string;
  readonly message: string;
}

/** How long a close waits for the requests in flight before it ends their connections. */
export const SERVER_CLOSE_GRACE_MS = 5_000;

/** The text of an address for a URL: an IPv6 address in brackets. Pure. */
export function addressText(address: BoundAddress): string {
  return address.host.includes(":") ? `[${address.host}]:${address.port}` : `${address.host}:${address.port}`;
}
