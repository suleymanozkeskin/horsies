import { describe, expect, test } from "bun:test";

import { producerManifestToWire } from "../../../horsies/src/_catalog/manifest.ts";
import type { ProducerManifest } from "../../../horsies/src/_catalog/manifest.ts";
import { wireText } from "./support/wire-text.ts";

// Each expected text is the text of Python's `dump_producer_manifest` for the
// same manifest. The two manifests hold every variant of every tagged type.

const SEPARATE_SESSION: ProducerManifest = {
  manifest_version: 1,
  database: {
    database_url: "postgresql://app@127.0.0.1:55432/app",
    session_database: { kind: "SEPARATE", url: "postgresql://app@127.0.0.1:55432/session" },
    pgbouncer_transaction_mode: false,
    pool_pre_ping: true,
    pool_size: 5,
    max_overflow: 10,
    pool_timeout: 30,
    pool_recycle: { kind: "AFTER_SECONDS", seconds: 3600 },
    echo: false,
    tcp_keepalives: { kind: "ON", idle_seconds: 60, interval_seconds: 10, probes: 5 },
    tcp_user_timeout: { kind: "AFTER", milliseconds: 30_000 },
  },
  schema: "MIGRATE_AND_VALIDATE",
  send: { retain_rerun_input_default: true, idempotency_reservation_window: 2_592_000_000_000 },
};

const SEPARATE_SESSION_TEXT = [
  String.raw`{"manifest_version":1,`,
  String.raw`"database":{"database_url":"postgresql://app@127.0.0.1:55432/app",`,
  String.raw`"session_database":{"kind":"SEPARATE","url":"postgresql://app@127.0.0.1:55432/session"},`,
  String.raw`"pgbouncer_transaction_mode":false,`,
  String.raw`"pool_pre_ping":true,`,
  String.raw`"pool_size":5,`,
  String.raw`"max_overflow":10,`,
  String.raw`"pool_timeout":30,`,
  String.raw`"pool_recycle":{"kind":"AFTER_SECONDS","seconds":3600},`,
  String.raw`"echo":false,`,
  String.raw`"tcp_keepalives":{"kind":"ON","idle_seconds":60,"interval_seconds":10,"probes":5},`,
  String.raw`"tcp_user_timeout":{"kind":"AFTER","milliseconds":30000}},`,
  String.raw`"schema":"MIGRATE_AND_VALIDATE",`,
  String.raw`"send":{"retain_rerun_input_default":true,`,
  String.raw`"idempotency_reservation_window":2592000000000}}`,
].join("");

const SAME_URL_SESSION: ProducerManifest = {
  manifest_version: 1,
  database: {
    database_url: "postgresql://app@127.0.0.1:6432/app",
    session_database: { kind: "SAME_AS_DATABASE_URL" },
    pgbouncer_transaction_mode: true,
    pool_pre_ping: false,
    pool_size: 1,
    max_overflow: 0,
    pool_timeout: 0,
    pool_recycle: { kind: "NEVER" },
    echo: true,
    tcp_keepalives: { kind: "OFF" },
    tcp_user_timeout: { kind: "SYSTEM_DEFAULT" },
  },
  schema: "OBSERVE_ONLY",
  send: { retain_rerun_input_default: false, idempotency_reservation_window: 1 },
};

const SAME_URL_SESSION_TEXT = [
  String.raw`{"manifest_version":1,`,
  String.raw`"database":{"database_url":"postgresql://app@127.0.0.1:6432/app",`,
  String.raw`"session_database":{"kind":"SAME_AS_DATABASE_URL"},`,
  String.raw`"pgbouncer_transaction_mode":true,`,
  String.raw`"pool_pre_ping":false,`,
  String.raw`"pool_size":1,`,
  String.raw`"max_overflow":0,`,
  String.raw`"pool_timeout":0,`,
  String.raw`"pool_recycle":{"kind":"NEVER"},`,
  String.raw`"echo":true,`,
  String.raw`"tcp_keepalives":{"kind":"OFF"},`,
  String.raw`"tcp_user_timeout":{"kind":"SYSTEM_DEFAULT"}},`,
  String.raw`"schema":"OBSERVE_ONLY",`,
  String.raw`"send":{"retain_rerun_input_default":false,`,
  String.raw`"idempotency_reservation_window":1}}`,
].join("");

describe("producerManifestToWire", () => {
  test("gives Python's text of a manifest with a separate session database", () => {
    expect(wireText(producerManifestToWire(SEPARATE_SESSION))).toBe(SEPARATE_SESSION_TEXT);
  });

  test("gives Python's text of a manifest with the other variant of each tagged type", () => {
    expect(wireText(producerManifestToWire(SAME_URL_SESSION))).toBe(SAME_URL_SESSION_TEXT);
  });

  test("throws a defect for a number that is not a safe integer, with its field", () => {
    const manifest: ProducerManifest = {
      ...SEPARATE_SESSION,
      database: { ...SEPARATE_SESSION.database, pool_size: 1.5 },
    };

    expect(() => producerManifestToWire(manifest)).toThrow(
      "integerToWire: DatabaseManifest.pool_size is 1.5, not a safe integer",
    );
  });
});
