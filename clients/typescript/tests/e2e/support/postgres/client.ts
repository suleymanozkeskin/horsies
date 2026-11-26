// The PostgreSQL session of the harness: a TCP connect to a URL's server, the authentication (trust, cleartext, MD5
// or SCRAM-SHA-256), simple queries in text form, and the end. It does not speak TLS and does not use the extended
// query protocol; a URL that asks for TLS is refused.

import { connect as connectTcp, type Socket } from "node:net";

import { err, ok, type Result } from "../../../../horsies/src/wire/result.ts";
import {
  type AuthenticationRequest,
  type BackendMessage,
  MAX_MESSAGE_BYTES,
  passwordMessage,
  queryMessage,
  readMessage,
  saslInitialMessage,
  saslResponseMessage,
  startupMessage,
  terminateMessage,
} from "./messages.ts";
import {
  md5Password,
  SCRAM_MECHANISM,
  scramFinal,
  type ScramFinal,
  scramServerVerified,
  type ScramStart,
  scramStart,
} from "./passwords.ts";

/** The longest wait for the TCP connect and the authentication. */
export const CONNECT_LIMIT_MS = 10_000;

/** The longest wait for the reply of one query. */
export const QUERY_LIMIT_MS = 60_000;

/** The most rows that one query reads. A larger result is a bound refusal. */
export const MAX_QUERY_ROWS = 10_000;

/** The default port of a URL with no port. */
const DEFAULT_PORT = 5432;

/** The password of a URL: none, or its text. */
export type Password = { readonly kind: "none" } | { readonly kind: "given"; readonly text: string };

/** Where a session connects. */
export type PostgresTarget = {
  readonly host: string;
  readonly port: number;
  readonly user: string;
  readonly password: Password;
  readonly database: string;
};

/** Why a session operation failed. No variant holds a password. */
export type PostgresError =
  | { readonly kind: "urlRefused"; readonly detail: string }
  | { readonly kind: "connectFailed"; readonly cause: string }
  | { readonly kind: "authenticationUnsupported"; readonly method: string }
  | { readonly kind: "passwordRequired"; readonly method: string }
  | { readonly kind: "serverRefused"; readonly code: string; readonly message: string }
  | { readonly kind: "protocolViolation"; readonly detail: string }
  | { readonly kind: "timedOut"; readonly step: string; readonly limitMs: number }
  | { readonly kind: "connectionEnded"; readonly cause: string }
  | { readonly kind: "boundReached"; readonly bound: string };

/** The rows of one query, each value in text form or `null`. */
export type QueryRows = {
  readonly columns: readonly string[];
  readonly rows: readonly (readonly (string | null)[])[];
  readonly tag: string;
};

/** The text of an error, for a test failure. Pure. */
export function postgresErrorText(error: PostgresError): string {
  switch (error.kind) {
    case "urlRefused":
      return `the database URL is refused: ${error.detail}`;
    case "connectFailed":
      return `the connect failed: ${error.cause}`;
    case "authenticationUnsupported":
      return `the server asks for the authentication method ${error.method}, which the harness client does not speak`;
    case "passwordRequired":
      return `the server asks for a password (${error.method}) and the URL holds none`;
    case "serverRefused":
      return `the server refused the statement: ${error.code}: ${error.message}`;
    case "protocolViolation":
      return `the server broke the protocol: ${error.detail}`;
    case "timedOut":
      return `${error.step} did not end within ${String(error.limitMs)} ms`;
    case "connectionEnded":
      return `the connection ended: ${error.cause}`;
    case "boundReached":
      return `a bound of the harness client was reached: ${error.bound}`;
  }
}

/** The target of a `postgresql://` or `postgres://` URL. Pure. */
export function parseTarget(url: string): Result<PostgresTarget, PostgresError> {
  if (!URL.canParse(url)) {
    return err({ kind: "urlRefused", detail: "the text is not a URL" });
  }

  const parsed = new URL(url);

  if (parsed.protocol !== "postgresql:" && parsed.protocol !== "postgres:") {
    return err({ kind: "urlRefused", detail: `the scheme ${parsed.protocol} is not postgresql: or postgres:` });
  }

  const sslMode = parsed.searchParams.get("sslmode");

  if (sslMode !== null && sslMode !== "disable" && sslMode !== "prefer" && sslMode !== "allow") {
    return err({ kind: "urlRefused", detail: `sslmode=${sslMode}: the harness client does not speak TLS` });
  }

  if (parsed.hostname === "" || parsed.searchParams.has("host")) {
    return err({ kind: "urlRefused", detail: "the URL names no TCP host (a socket path is not supported)" });
  }

  const port = parsed.port === "" ? DEFAULT_PORT : Number(parsed.port);
  const user = decodeURIComponent(parsed.username);
  const database = decodeURIComponent(parsed.pathname.replace(/^\//, ""));

  if (user === "" || database === "") {
    return err({ kind: "urlRefused", detail: "the URL names no user or no database" });
  }

  const password: Password =
    parsed.password === "" ? { kind: "none" } : { kind: "given", text: decodeURIComponent(parsed.password) };

  return ok({ host: parsed.hostname.replace(/^\[|\]$/g, ""), port, user, password, database });
}

/** A session that is open. Each query waits for the one before it. */
export interface PostgresSession {
  /** Run one simple query and read its rows. */
  query(sql: string): Promise<Result<QueryRows, PostgresError>>;
  /** End the session. Never fails: a session that ended already stays ended. */
  close(): Promise<void>;
}

/** Open a session on `target`: the connect and the authentication, within `CONNECT_LIMIT_MS`. */
export async function openSession(target: PostgresTarget): Promise<Result<PostgresSession, PostgresError>> {
  const socket = connectTcp({ host: target.host, port: target.port });
  const reader = new MessageReader(socket);
  const connected = await reader.connected(CONNECT_LIMIT_MS);

  if (!connected.ok) {
    socket.destroy();

    return connected;
  }

  socket.write(startupMessage(target.user, target.database));

  const authenticated = await authenticate(socket, reader, target);

  if (!authenticated.ok) {
    socket.destroy();

    return authenticated;
  }

  return ok(new Session(socket, reader));
}

/** The authentication exchange, then the wait for the first `ReadyForQuery`. */
async function authenticate(socket: Socket, reader: MessageReader, target: PostgresTarget): Promise<Result<undefined, PostgresError>> {
  let scram: ScramExchange = { kind: "notStarted" };

  for (;;) {
    const next = await reader.next(CONNECT_LIMIT_MS, "the authentication");

    if (!next.ok) {
      return next;
    }

    const message = next.value;

    switch (message.kind) {
      case "authentication": {
        const step = authenticationStep(message.request, target, scram);

        if (!step.ok) {
          return step;
        }

        scram = step.value.scram;

        for (const reply of step.value.replies) {
          socket.write(reply);
        }

        break;
      }

      case "readyForQuery":
        return ok(undefined);
      case "errorResponse":
        return err({ kind: "serverRefused", code: message.fields.code, message: message.fields.message });
      case "rowDescription":
      case "dataRow":
      case "commandComplete":
      case "emptyQuery":
        return err({ kind: "protocolViolation", detail: `a ${message.kind} message during the authentication` });
      case "ignored":
        break;
    }
  }
}

/** Where a SCRAM exchange is. */
type ScramExchange =
  | { readonly kind: "notStarted" }
  | { readonly kind: "started"; readonly start: ScramStart }
  | { readonly kind: "finalSent"; readonly final: ScramFinal };

/** The replies to one authentication request, and the SCRAM state after it. */
type AuthStep = { readonly replies: readonly Buffer[]; readonly scram: ScramExchange };

function authenticationStep(
  request: AuthenticationRequest,
  target: PostgresTarget,
  scram: ScramExchange,
): Result<AuthStep, PostgresError> {
  switch (request.kind) {
    case "ok":
      return ok({ replies: [], scram });
    case "cleartextPassword":
      return withPassword(target, "password", (password) => ok({ replies: [passwordMessage(password)], scram }));
    case "md5Password":
      return withPassword(target, "md5", (password) =>
        ok({ replies: [passwordMessage(md5Password(target.user, password, request.salt))], scram }),
      );
    case "sasl":
      return saslStart(request.mechanisms, target);
    case "saslContinue":
      return saslContinue(request.data, target, scram);
    case "saslFinal":
      return scram.kind === "finalSent" && scramServerVerified(scram.final, request.data)
        ? ok({ replies: [], scram })
        : err({ kind: "protocolViolation", detail: "the SCRAM server signature does not verify" });
    case "unsupported":
      return err({ kind: "authenticationUnsupported", method: `code ${String(request.code)}` });
  }
}

function withPassword(
  target: PostgresTarget,
  method: string,
  use: (password: string) => Result<AuthStep, PostgresError>,
): Result<AuthStep, PostgresError> {
  return target.password.kind === "given" ? use(target.password.text) : err({ kind: "passwordRequired", method });
}

function saslStart(mechanisms: readonly string[], target: PostgresTarget): Result<AuthStep, PostgresError> {
  if (!mechanisms.includes(SCRAM_MECHANISM)) {
    return err({ kind: "authenticationUnsupported", method: `SASL ${mechanisms.join(", ")}` });
  }

  return withPassword(target, SCRAM_MECHANISM, () => {
    const start = scramStart();

    return ok({ replies: [saslInitialMessage(SCRAM_MECHANISM, start.clientFirstMessage)], scram: { kind: "started", start } });
  });
}

function saslContinue(serverFirst: string, target: PostgresTarget, scram: ScramExchange): Result<AuthStep, PostgresError> {
  if (scram.kind !== "started") {
    return err({ kind: "protocolViolation", detail: "a SASL continue before the SASL start" });
  }

  return withPassword(target, SCRAM_MECHANISM, (password) => {
    const final = scramFinal(scram.start, serverFirst, password);

    return final.ok
      ? ok({ replies: [saslResponseMessage(final.value.clientFinalMessage)], scram: { kind: "finalSent", final: final.value } })
      : err({ kind: "protocolViolation", detail: final.error.detail });
  });
}

class Session implements PostgresSession {
  readonly #socket: Socket;
  readonly #reader: MessageReader;
  #turn: Promise<unknown> = Promise.resolve();

  constructor(socket: Socket, reader: MessageReader) {
    this.#socket = socket;
    this.#reader = reader;
  }

  async query(sql: string): Promise<Result<QueryRows, PostgresError>> {
    const run = this.#turn.then(async () => this.#run(sql));

    this.#turn = run;

    return run;
  }

  async #run(sql: string): Promise<Result<QueryRows, PostgresError>> {
    this.#socket.write(queryMessage(sql));

    return readQueryReply(this.#reader);
  }

  async close(): Promise<void> {
    await this.#turn;

    if (!this.#socket.destroyed) {
      this.#socket.end(terminateMessage());
      this.#socket.destroy();
    }
  }
}

/** The reply of one simple query, up to its `ReadyForQuery`. A server error is read to its `ReadyForQuery` first. */
async function readQueryReply(reader: MessageReader): Promise<Result<QueryRows, PostgresError>> {
  let columns: readonly string[] = [];
  const rows: (readonly (string | null)[])[] = [];
  let tag = "";
  let refused: PostgresError | undefined;

  for (;;) {
    const next = await reader.next(QUERY_LIMIT_MS, "a query");

    if (!next.ok) {
      return next;
    }

    const message: BackendMessage = next.value;

    switch (message.kind) {
      case "rowDescription":
        columns = message.columns;
        break;
      case "dataRow":
        if (rows.length >= MAX_QUERY_ROWS) {
          refused ??= { kind: "boundReached", bound: `more than ${String(MAX_QUERY_ROWS)} rows` };
        } else {
          rows.push(message.values);
        }

        break;
      case "commandComplete":
        tag = message.tag;
        break;
      case "errorResponse":
        refused ??= { kind: "serverRefused", code: message.fields.code, message: message.fields.message };
        break;
      case "readyForQuery":
        return refused === undefined ? ok({ columns, rows, tag }) : err(refused);
      case "authentication":
        return err({ kind: "protocolViolation", detail: "an authentication message after the start" });
      case "emptyQuery":
      case "ignored":
        break;
    }
  }
}

/** How the socket of a reader ended. */
type ReaderEnd = { readonly kind: "open" } | { readonly kind: "ended"; readonly error: PostgresError };

/** The backend messages of one socket, in order, with a bounded buffer. */
class MessageReader {
  #buffer: Buffer = Buffer.alloc(0);
  readonly #messages: BackendMessage[] = [];
  #end: ReaderEnd = { kind: "open" };
  #wake: () => void = () => undefined;
  #isConnected = false;

  constructor(socket: Socket) {
    socket.on("connect", () => {
      this.#isConnected = true;
      this.#wake();
    });
    socket.on("data", (chunk: Buffer) => {
      this.#take(chunk);
    });
    socket.on("error", (error: Error) => {
      this.#finish(this.#isConnected ? { kind: "connectionEnded", cause: error.message } : { kind: "connectFailed", cause: error.message });
    });
    socket.on("close", () => {
      this.#finish({ kind: "connectionEnded", cause: "the server closed the connection" });
    });
  }

  /** Wait until the TCP connect ends, at most `limitMs`. */
  async connected(limitMs: number): Promise<Result<undefined, PostgresError>> {
    const deadline = performance.now() + limitMs;

    while (!this.#isConnected) {
      if (this.#end.kind === "ended") {
        return err(this.#end.error);
      }

      const left = deadline - performance.now();

      if (left <= 0) {
        return err({ kind: "timedOut", step: "the TCP connect", limitMs });
      }

      await this.#sleep(left);
    }

    return ok(undefined);
  }

  /** The next message, at most `limitMs` from now. */
  async next(limitMs: number, step: string): Promise<Result<BackendMessage, PostgresError>> {
    const deadline = performance.now() + limitMs;

    for (;;) {
      const message = this.#messages.shift();

      if (message !== undefined) {
        return ok(message);
      }

      if (this.#end.kind === "ended") {
        return err(this.#end.error);
      }

      const left = deadline - performance.now();

      if (left <= 0) {
        return err({ kind: "timedOut", step, limitMs });
      }

      await this.#sleep(left);
    }
  }

  async #sleep(milliseconds: number): Promise<void> {
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, milliseconds);

      this.#wake = () => {
        clearTimeout(timer);
        resolve();
      };
    });
    this.#wake = () => undefined;
  }

  #take(chunk: Buffer): void {
    this.#buffer = this.#buffer.length === 0 ? chunk : Buffer.concat([this.#buffer, chunk]);

    let offset = 0;

    for (;;) {
      const step = readMessage(this.#buffer, offset);

      if (step.kind === "incomplete") {
        break;
      }

      if (step.kind === "refused") {
        this.#finish({ kind: "protocolViolation", detail: step.detail });

        return;
      }

      this.#messages.push(step.message);
      offset = step.next;
    }

    this.#buffer = this.#buffer.subarray(offset);

    if (this.#buffer.length > MAX_MESSAGE_BYTES * 2) {
      this.#finish({ kind: "boundReached", bound: "the unread bytes of the connection" });

      return;
    }

    this.#wake();
  }

  #finish(error: PostgresError): void {
    if (this.#end.kind === "open") {
      this.#end = { kind: "ended", error };
    }

    this.#wake();
  }
}
