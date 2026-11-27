// The password exchanges of the harness client: MD5 (PostgreSQL's `md5` method) and SCRAM-SHA-256 (RFC 5802 and
// RFC 7677, without channel binding). Pure, apart from the random client nonce of `scramStart`.

import { createHash, createHmac, pbkdf2Sync, randomBytes, timingSafeEqual } from "node:crypto";

import { err, ok, type Result } from "../../../../horsies/src/wire/result.ts";

/** The SASL mechanism that the client offers. */
export const SCRAM_MECHANISM = "SCRAM-SHA-256";

/** The bytes of the random client nonce. */
const CLIENT_NONCE_BYTES = 18;

/** The bytes of a SHA-256 key. */
const KEY_BYTES = 32;

/** The most PBKDF2 iterations that the client runs: a bound on the work that a server can ask for. */
export const MAX_SCRAM_ITERATIONS = 1_000_000;

/** The GS2 header of a client that does not bind the channel, and its base64 text. */
const GS2_HEADER = "n,,";
const GS2_HEADER_BASE64 = Buffer.from(GS2_HEADER, "utf8").toString("base64");

/** The MD5 password of `user`: `md5` and the hex of md5(md5(password + user) + salt). */
export function md5Password(user: string, password: string, salt: Buffer): string {
  const inner = createHash("md5").update(password + user, "utf8").digest("hex");

  return `md5${createHash("md5").update(Buffer.concat([Buffer.from(inner, "utf8"), salt])).digest("hex")}`;
}

/** The first message of a SCRAM exchange, and what the next steps need. */
export type ScramStart = { readonly clientFirstMessage: string; readonly clientFirstBare: string; readonly clientNonce: string };

/**
 * The client's first message with `userName` and `clientNonce`. PostgreSQL ignores the user name of SCRAM (it reads
 * the startup message), so the client sends an empty one. The password is not normalised with SASLprep: the harness
 * passwords are ASCII. Pure.
 */
export function scramFirst(userName: string, clientNonce: string): ScramStart {
  const clientFirstBare = `n=${userName},r=${clientNonce}`;

  return { clientFirstMessage: `${GS2_HEADER}${clientFirstBare}`, clientFirstBare, clientNonce };
}

/** The client's first message with a random nonce. Effects: reads random bytes. */
export function scramStart(): ScramStart {
  return scramFirst("", randomBytes(CLIENT_NONCE_BYTES).toString("base64"));
}

/** The client's final message, and the server signature that the server's final message must hold. */
export type ScramFinal = { readonly clientFinalMessage: string; readonly serverSignature: string };

/** Why a SCRAM step refused the server's message. `detail` holds no password. */
export type ScramRefused = { readonly detail: string };

/** The client's final message for the server's first message `serverFirst`. Pure. */
export function scramFinal(start: ScramStart, serverFirst: string, password: string): Result<ScramFinal, ScramRefused> {
  const fields = scramFields(serverFirst);
  const nonce = fields.get("r");
  const salt = fields.get("s");
  const iterations = Number(fields.get("i"));

  if (nonce === undefined || !nonce.startsWith(start.clientNonce) || nonce.length === start.clientNonce.length) {
    return err({ detail: "the server nonce does not extend the client nonce" });
  }

  if (salt === undefined || !Number.isSafeInteger(iterations) || iterations < 1 || iterations > MAX_SCRAM_ITERATIONS) {
    return err({ detail: `the server's first message has no salt or an iteration count outside 1 to ${String(MAX_SCRAM_ITERATIONS)}` });
  }

  const salted = pbkdf2Sync(password, Buffer.from(salt, "base64"), iterations, KEY_BYTES, "sha256");
  const clientKey = hmac(salted, "Client Key");
  const storedKey = createHash("sha256").update(clientKey).digest();
  const withoutProof = `c=${GS2_HEADER_BASE64},r=${nonce}`;
  const authMessage = `${start.clientFirstBare},${serverFirst},${withoutProof}`;
  const clientSignature = hmac(storedKey, authMessage);
  const proof = Buffer.alloc(KEY_BYTES);

  for (let index = 0; index < KEY_BYTES; index += 1) {
    proof[index] = (clientKey[index] ?? 0) ^ (clientSignature[index] ?? 0);
  }

  const serverSignature = hmac(hmac(salted, "Server Key"), authMessage).toString("base64");

  return ok({ clientFinalMessage: `${withoutProof},p=${proof.toString("base64")}`, serverSignature });
}

/** Whether the server's final message `serverFinal` holds the expected server signature. Pure. */
export function scramServerVerified(final: ScramFinal, serverFinal: string): boolean {
  const signature = scramFields(serverFinal).get("v");

  if (signature === undefined) {
    return false;
  }

  const given = Buffer.from(signature, "base64");
  const expected = Buffer.from(final.serverSignature, "base64");

  return given.length === expected.length && timingSafeEqual(given, expected);
}

/** The `name=value` attributes of a SCRAM message. A value may hold `=`; the first one splits. */
function scramFields(message: string): ReadonlyMap<string, string> {
  const fields = new Map<string, string>();

  for (const part of message.split(",")) {
    const split = part.indexOf("=");

    if (split > 0) {
      fields.set(part.slice(0, split), part.slice(split + 1));
    }
  }

  return fields;
}

function hmac(key: Buffer, text: string): Buffer {
  return createHmac("sha256", key).update(text, "utf8").digest();
}
