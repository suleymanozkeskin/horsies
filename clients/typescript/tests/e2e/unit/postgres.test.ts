// The pure parts of the harness's PostgreSQL client: the backend messages, the password exchanges and the URL target.
// No database: each case reads or writes bytes.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { parseTarget } from "../support/postgres/client.ts";
import { queryMessage, readMessage, startupMessage } from "../support/postgres/messages.ts";
import { md5Password, scramFinal, scramFirst, scramServerVerified } from "../support/postgres/passwords.ts";

/** A backend message of `type` with `body`. */
function backend(type: string, body: Buffer): Buffer {
  const header = Buffer.alloc(5);

  header.write(type, 0, "latin1");
  header.writeInt32BE(body.length + 4, 1);

  return Buffer.concat([header, body]);
}

function int32(value: number): Buffer {
  const bytes = Buffer.alloc(4);

  bytes.writeInt32BE(value, 0);

  return bytes;
}

function int16(value: number): Buffer {
  const bytes = Buffer.alloc(2);

  bytes.writeInt16BE(value, 0);

  return bytes;
}

describe("the backend messages", () => {
  test("a buffer shorter than a header is incomplete", () => {
    assert.deepEqual(readMessage(Buffer.from([0x52, 0, 0]), 0), { kind: "incomplete" });
  });

  test("a message whose body has not all arrived is incomplete", () => {
    assert.deepEqual(readMessage(backend("C", Buffer.from("SELECT 1\0")).subarray(0, 8), 0), { kind: "incomplete" });
  });

  test("a length outside its bound is refused", () => {
    const step = readMessage(Buffer.concat([Buffer.from("C"), int32(2)]), 0);

    assert.equal(step.kind, "refused");
  });

  test("an authentication request with no code is refused", () => {
    assert.equal(readMessage(backend("R", Buffer.alloc(0)), 0).kind, "refused");
  });

  test("each authentication request reads as its kind", () => {
    const read = (body: Buffer): unknown => {
      const step = readMessage(backend("R", body), 0);

      return step.kind === "message" ? step.message : step;
    };

    assert.deepEqual(read(int32(0)), { kind: "authentication", request: { kind: "ok" } });
    assert.deepEqual(read(int32(3)), { kind: "authentication", request: { kind: "cleartextPassword" } });
    assert.deepEqual(read(Buffer.concat([int32(5), Buffer.from([1, 2, 3, 4])])), {
      kind: "authentication",
      request: { kind: "md5Password", salt: Buffer.from([1, 2, 3, 4]) },
    });
    assert.deepEqual(read(Buffer.concat([int32(10), Buffer.from("SCRAM-SHA-256\0SCRAM-SHA-256-PLUS\0\0")])), {
      kind: "authentication",
      request: { kind: "sasl", mechanisms: ["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"] },
    });
    assert.deepEqual(read(Buffer.concat([int32(11), Buffer.from("r=abc")])), { kind: "authentication", request: { kind: "saslContinue", data: "r=abc" } });
    assert.deepEqual(read(Buffer.concat([int32(12), Buffer.from("v=xyz")])), { kind: "authentication", request: { kind: "saslFinal", data: "v=xyz" } });
    assert.deepEqual(read(int32(7)), { kind: "authentication", request: { kind: "unsupported", code: 7 } });
  });

  test("a row description gives the column names; a data row gives text values and null", () => {
    const column = (name: string): Buffer => Buffer.concat([Buffer.from(`${name}\0`), Buffer.alloc(18)]);
    const description = backend("T", Buffer.concat([int16(2), column("task_id"), column("status")]));
    const row = backend("D", Buffer.concat([int16(2), int32(4), Buffer.from("abcd"), int32(-1)]));
    const both = Buffer.concat([description, row]);
    const first = readMessage(both, 0);

    assert.equal(first.kind, "message");

    if (first.kind === "message") {
      assert.deepEqual(first.message, { kind: "rowDescription", columns: ["task_id", "status"] });
      assert.deepEqual(readMessage(both, first.next), { kind: "message", message: { kind: "dataRow", values: ["abcd", null] }, next: both.length });
    }
  });

  test("a data row whose value runs past its message is refused", () => {
    assert.equal(readMessage(backend("D", Buffer.concat([int16(1), int32(10), Buffer.from("ab")])), 0).kind, "refused");
  });

  test("an error response gives its code and message; other types are ignored", () => {
    const error = backend("E", Buffer.from("SERROR\0C42P04\0Mdatabase exists\0\0"));
    const notice = backend("N", Buffer.from("SNOTICE\0\0"));

    assert.deepEqual(readMessage(error, 0), {
      kind: "message",
      message: { kind: "errorResponse", fields: { code: "42P04", message: "database exists" } },
      next: error.length,
    });
    assert.deepEqual(readMessage(notice, 0), { kind: "message", message: { kind: "ignored", type: "N" }, next: notice.length });
  });

  test("the startup and query messages carry their length", () => {
    const startup = startupMessage("postgres", "horsies");
    const query = queryMessage("SELECT 1");

    assert.equal(startup.readInt32BE(0), startup.length);
    assert.equal(startup.readInt32BE(4), 196_608);
    assert.equal(query.toString("latin1", 0, 1), "Q");
    assert.equal(query.readInt32BE(1), query.length - 1);
  });
});

describe("the password exchanges", () => {
  test("the MD5 password is md5 of the inner md5 and the salt", () => {
    assert.equal(md5Password("postgres", "secret", Buffer.from([1, 2, 3, 4])), "md5bb41a296aab6baccb36ff243a562abff");
  });

  // RFC 7677, section 3.
  const start = scramFirst("user", "rOprNGfwEbeRWgbNEkqO");
  const serverFirst = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";

  test("SCRAM-SHA-256 gives the client proof of the RFC 7677 example, and verifies its server signature", () => {
    assert.equal(start.clientFirstMessage, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");

    const final = scramFinal(start, serverFirst, "pencil");

    assert.ok(final.ok);
    assert.equal(
      final.value.clientFinalMessage,
      "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
    );
    assert.equal(scramServerVerified(final.value, "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="), true);
    assert.equal(scramServerVerified(final.value, "v=AAAATRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="), false);
    assert.equal(scramServerVerified(final.value, "e=other-error"), false);
  });

  test("a server nonce that does not extend the client nonce is refused", () => {
    assert.equal(scramFinal(start, "r=another,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", "pencil").ok, false);
  });

  test("an iteration count outside its bound is refused", () => {
    assert.equal(scramFinal(start, "r=rOprNGfwEbeRWgbNEkqOxyz,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=0", "pencil").ok, false);
    assert.equal(scramFinal(start, "r=rOprNGfwEbeRWgbNEkqOxyz,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=99999999", "pencil").ok, false);
  });
});

describe("the URL target", () => {
  test("a URL gives its host, port, user, password and database", () => {
    assert.deepEqual(parseTarget("postgresql://postgres:p%40ss@127.0.0.1:55432/horsies?sslmode=disable"), {
      ok: true,
      value: { host: "127.0.0.1", port: 55_432, user: "postgres", password: { kind: "given", text: "p@ss" }, database: "horsies" },
    });
  });

  test("a URL with no password and no port has none and the default port", () => {
    assert.deepEqual(parseTarget("postgres://postgres@[::1]/horsies"), {
      ok: true,
      value: { host: "::1", port: 5432, user: "postgres", password: { kind: "none" }, database: "horsies" },
    });
  });

  test("TLS, a socket path, another scheme, a missing user and a text that is not a URL are refused", () => {
    for (const url of [
      "postgresql://postgres@127.0.0.1:5432/horsies?sslmode=require",
      "postgresql://postgres@127.0.0.1/horsies?host=/tmp",
      "mysql://postgres@127.0.0.1/horsies",
      "postgresql://127.0.0.1/horsies",
      "not a url",
    ]) {
      const target = parseTarget(url);

      assert.equal(target.ok ? "parsed" : target.error.kind, "urlRefused", url);
    }
  });
});
