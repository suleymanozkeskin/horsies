// The messages of the PostgreSQL frontend/backend protocol 3.0 that the harness client uses: the startup, the
// password and SASL messages, a simple query and the end, and the backend messages of a connect and of a simple
// query. Pure: no I/O.
//
// The harness reads and writes rows of its own test databases (the waits and checks that the client API does not
// offer), as the Rust support crate does with tokio-postgres. The workspace has no PostgreSQL package, so the harness
// speaks the protocol itself: simple queries in text form only.

/** The protocol version 3.0 of the startup message. */
const PROTOCOL_VERSION = 196_608;

/** The bytes of a message length field. */
const LENGTH_BYTES = 4;

/** The bytes of a count field of a row description or a data row. */
const COUNT_BYTES = 2;

/** The bytes of a message header: the type byte and the length. */
const HEADER_BYTES = 5;

/** The largest backend message that the client reads, in bytes. A larger one is a bound refusal. */
export const MAX_MESSAGE_BYTES = 16 * 1024 * 1024;

/** The authentication request codes of the protocol. */
const AUTH_OK = 0;
const AUTH_CLEARTEXT = 3;
const AUTH_MD5 = 5;
const AUTH_SASL = 10;
const AUTH_SASL_CONTINUE = 11;
const AUTH_SASL_FINAL = 12;

/** The bytes of the salt of an MD5 password request. */
const MD5_SALT_BYTES = 4;

/** What the server asks for in an authentication message. */
export type AuthenticationRequest =
  | { readonly kind: "ok" }
  | { readonly kind: "cleartextPassword" }
  | { readonly kind: "md5Password"; readonly salt: Buffer }
  | { readonly kind: "sasl"; readonly mechanisms: readonly string[] }
  | { readonly kind: "saslContinue"; readonly data: string }
  | { readonly kind: "saslFinal"; readonly data: string }
  | { readonly kind: "unsupported"; readonly code: number };

/** The fields of an error response that the harness reports: the SQLSTATE code and the primary message. */
export type ServerErrorFields = { readonly code: string; readonly message: string };

/** One backend message. A message type that the client does not read is `ignored`. */
export type BackendMessage =
  | { readonly kind: "authentication"; readonly request: AuthenticationRequest }
  | { readonly kind: "readyForQuery" }
  | { readonly kind: "rowDescription"; readonly columns: readonly string[] }
  | { readonly kind: "dataRow"; readonly values: readonly (string | null)[] }
  | { readonly kind: "commandComplete"; readonly tag: string }
  | { readonly kind: "emptyQuery" }
  | { readonly kind: "errorResponse"; readonly fields: ServerErrorFields }
  | { readonly kind: "ignored"; readonly type: string };

/** One step of the read of a buffer: a whole message and the offset after it, more bytes needed, or a refusal. */
export type ReadStep =
  | { readonly kind: "message"; readonly message: BackendMessage; readonly next: number }
  | { readonly kind: "incomplete" }
  | { readonly kind: "refused"; readonly detail: string };

/** The startup message of `user` on `database`. */
export function startupMessage(user: string, database: string): Buffer {
  const pairs = Buffer.from(`user\0${user}\0database\0${database}\0application_name\0horsies-ts-e2e\0\0`, "utf8");
  const message = Buffer.alloc(LENGTH_BYTES * 2 + pairs.length);

  message.writeInt32BE(message.length, 0);
  message.writeInt32BE(PROTOCOL_VERSION, LENGTH_BYTES);
  pairs.copy(message, LENGTH_BYTES * 2);

  return message;
}

/** A password message (`p`) with `text`: a cleartext or an MD5 password. */
export function passwordMessage(text: string): Buffer {
  return typedMessage("p", Buffer.from(`${text}\0`, "utf8"));
}

/** The SASL initial response (`p`) of `mechanism` with `data`. */
export function saslInitialMessage(mechanism: string, data: string): Buffer {
  const name = Buffer.from(`${mechanism}\0`, "utf8");
  const body = Buffer.from(data, "utf8");
  const length = Buffer.alloc(LENGTH_BYTES);

  length.writeInt32BE(body.length, 0);

  return typedMessage("p", Buffer.concat([name, length, body]));
}

/** A SASL response (`p`) with `data`. */
export function saslResponseMessage(data: string): Buffer {
  return typedMessage("p", Buffer.from(data, "utf8"));
}

/** A simple query (`Q`) of `sql`. */
export function queryMessage(sql: string): Buffer {
  return typedMessage("Q", Buffer.from(`${sql}\0`, "utf8"));
}

/** The end of the session (`X`). */
export function terminateMessage(): Buffer {
  return typedMessage("X", Buffer.alloc(0));
}

function typedMessage(type: string, body: Buffer): Buffer {
  const header = Buffer.alloc(HEADER_BYTES);

  header.write(type, 0, "latin1");
  header.writeInt32BE(body.length + LENGTH_BYTES, 1);

  return Buffer.concat([header, body]);
}

/** The next backend message of `buffer` from `offset`. */
export function readMessage(buffer: Buffer, offset: number): ReadStep {
  if (buffer.length - offset < HEADER_BYTES) {
    return { kind: "incomplete" };
  }

  const type = String.fromCodePoint(buffer[offset] ?? 0);
  const length = buffer.readInt32BE(offset + 1);

  if (length < LENGTH_BYTES || length > MAX_MESSAGE_BYTES) {
    return { kind: "refused", detail: `a ${type} message of ${String(length)} bytes, outside 4 to ${String(MAX_MESSAGE_BYTES)}` };
  }

  const end = offset + 1 + length;

  if (buffer.length < end) {
    return { kind: "incomplete" };
  }

  const body = buffer.subarray(offset + HEADER_BYTES, end);
  const message = messageOf(type, body);

  return message.kind === "refused" ? message : { kind: "message", message: message.message, next: end };
}

type BodyRead = { readonly kind: "read"; readonly message: BackendMessage } | { readonly kind: "refused"; readonly detail: string };

function messageOf(type: string, body: Buffer): BodyRead {
  switch (type) {
    case "R":
      return authenticationOf(body);
    case "Z":
      return read({ kind: "readyForQuery" });
    case "T":
      return rowDescriptionOf(body);
    case "D":
      return dataRowOf(body);
    case "C":
      return read({ kind: "commandComplete", tag: cString(body, 0).text });
    case "I":
      return read({ kind: "emptyQuery" });
    case "E":
      return read({ kind: "errorResponse", fields: errorFieldsOf(body) });
    default:
      return read({ kind: "ignored", type });
  }
}

function read(message: BackendMessage): BodyRead {
  return { kind: "read", message };
}

function authenticationOf(body: Buffer): BodyRead {
  if (body.length < LENGTH_BYTES) {
    return { kind: "refused", detail: "an authentication message with no request code" };
  }

  const code = body.readInt32BE(0);
  const rest = body.subarray(LENGTH_BYTES);

  switch (code) {
    case AUTH_OK:
      return read({ kind: "authentication", request: { kind: "ok" } });
    case AUTH_CLEARTEXT:
      return read({ kind: "authentication", request: { kind: "cleartextPassword" } });
    case AUTH_MD5:
      return rest.length === MD5_SALT_BYTES
        ? read({ kind: "authentication", request: { kind: "md5Password", salt: Buffer.from(rest) } })
        : { kind: "refused", detail: `an MD5 request with a salt of ${String(rest.length)} bytes` };
    case AUTH_SASL:
      return read({ kind: "authentication", request: { kind: "sasl", mechanisms: cStrings(rest) } });
    case AUTH_SASL_CONTINUE:
      return read({ kind: "authentication", request: { kind: "saslContinue", data: rest.toString("utf8") } });
    case AUTH_SASL_FINAL:
      return read({ kind: "authentication", request: { kind: "saslFinal", data: rest.toString("utf8") } });
    default:
      return read({ kind: "authentication", request: { kind: "unsupported", code } });
  }
}

function rowDescriptionOf(body: Buffer): BodyRead {
  if (body.length < COUNT_BYTES) {
    return { kind: "refused", detail: "a row description with no column count" };
  }

  const count = body.readInt16BE(0);
  const columns: string[] = [];
  // Each column: its name, then the table oid (4), the column number (2), the type oid (4), the type size (2), the
  // type modifier (4) and the format code (2).
  const fixedBytes = 18;
  let offset = COUNT_BYTES;

  for (let index = 0; index < count; index += 1) {
    const name = cString(body, offset);

    if (name.next < 0) {
      return { kind: "refused", detail: "a row description with a column name that does not end" };
    }

    columns.push(name.text);
    offset = name.next + fixedBytes;

    if (offset > body.length) {
      return { kind: "refused", detail: "a row description that ends inside a column" };
    }
  }

  return read({ kind: "rowDescription", columns });
}

function dataRowOf(body: Buffer): BodyRead {
  if (body.length < COUNT_BYTES) {
    return { kind: "refused", detail: "a data row with no value count" };
  }

  const count = body.readInt16BE(0);
  const values: (string | null)[] = [];
  let offset = COUNT_BYTES;

  for (let index = 0; index < count; index += 1) {
    if (offset + LENGTH_BYTES > body.length) {
      return { kind: "refused", detail: "a data row that ends inside a value length" };
    }

    const length = body.readInt32BE(offset);

    offset += LENGTH_BYTES;

    if (length < 0) {
      values.push(null);
    } else if (offset + length > body.length) {
      return { kind: "refused", detail: "a data row with a value past the end of its message" };
    } else {
      values.push(body.toString("utf8", offset, offset + length));
      offset += length;
    }
  }

  return read({ kind: "dataRow", values });
}

/** The SQLSTATE code (`C`) and the message (`M`) of an error response. */
function errorFieldsOf(body: Buffer): ServerErrorFields {
  let code = "";
  let message = "";
  let offset = 0;

  while (offset < body.length && body[offset] !== 0) {
    const field = String.fromCodePoint(body[offset] ?? 0);
    const value = cString(body, offset + 1);

    if (value.next < 0) {
      break;
    }

    code = field === "C" ? value.text : code;
    message = field === "M" ? value.text : message;
    offset = value.next;
  }

  return { code, message };
}

/** The text of the NUL-ended string at `offset`, and the offset after its NUL; `next` is -1 when no NUL follows. */
function cString(body: Buffer, offset: number): { readonly text: string; readonly next: number } {
  const end = body.indexOf(0, offset);

  return end < 0 ? { text: body.toString("utf8", offset), next: -1 } : { text: body.toString("utf8", offset, end), next: end + 1 };
}

/** The NUL-ended strings of a list that ends with an empty string. */
function cStrings(body: Buffer): readonly string[] {
  const texts: string[] = [];
  let offset = 0;

  while (offset < body.length && body[offset] !== 0) {
    const item = cString(body, offset);

    if (item.next < 0) {
      break;
    }

    texts.push(item.text);
    offset = item.next;
  }

  return texts;
}
