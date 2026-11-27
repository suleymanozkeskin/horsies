// The coverage bounds of the retention config: each bound is refused before the addon, with the field path and the
// code of the retention config, and each edge value is accepted.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  HEARTBEAT_LEAF_HORIZON_HOURS_MAX,
  HEARTBEAT_LEAF_HORIZON_HOURS_MIN,
  HISTORY_LEAF_HORIZON_DAYS_MAX,
  HISTORY_LEAF_HORIZON_DAYS_MIN,
  RETENTION_DAYS_MAX,
  RETENTION_DAYS_MIN,
} from "../../../horsies/src/_catalog/worker_manifest.ts";
import { type AppConfigInput, parseAppConfig } from "../../../horsies/src/config/app-config.ts";
import type { RetentionConfigInput } from "../../../horsies/src/config/retention.ts";

const DATABASE_URL = "postgresql://horsies:secret@127.0.0.1:55432/horsies";
const RETENTION_CODE = "HRS-216";
const U32_MAX = 4_294_967_295;

/** The refusals as `[code, path]` pairs, or `"accepted"`. */
function outcomeOf(retention: RetentionConfigInput): "accepted" | readonly (readonly [string, string])[] {
  const input: AppConfigInput = { database: { databaseUrl: { kind: "value", value: DATABASE_URL } }, retention };
  const parsed = parseAppConfig(input, () => undefined);

  return parsed.ok ? "accepted" : parsed.error.map((one) => [one.code, one.path]);
}

function refusedAt(path: string): readonly (readonly [string, string])[] {
  return [[RETENTION_CODE, path]];
}

describe("retention bounds", () => {
  test("a declared period is accepted at 1 and 1825 days and refused at 0 and 1826", () => {
    for (const days of [RETENTION_DAYS_MIN, RETENTION_DAYS_MAX]) {
      assert.equal(outcomeOf({ retentionPeriods: [{ days }] }), "accepted");
    }

    for (const days of [RETENTION_DAYS_MIN - 1, RETENTION_DAYS_MAX + 1, 1.5, Number.NaN]) {
      assert.deepEqual(outcomeOf({ retentionPeriods: [{ days }] }), refusedAt("retention.retentionPeriods[0].days"));
    }
  });

  test("a queue retention period has the bounds of a declared period", () => {
    for (const days of [RETENTION_DAYS_MIN, RETENTION_DAYS_MAX]) {
      assert.equal(outcomeOf({ queueRetention: { bulk: { kind: "days", days } } }), "accepted");
    }

    for (const days of [RETENTION_DAYS_MIN - 1, RETENTION_DAYS_MAX + 1]) {
      assert.deepEqual(
        outcomeOf({ queueRetention: { bulk: { kind: "days", days } } }),
        refusedAt('retention.queueRetention["bulk"].days'),
      );
    }
  });

  test("the history horizon is accepted at 2 and 14 days and refused at 1 and 15", () => {
    for (const days of [HISTORY_LEAF_HORIZON_DAYS_MIN, HISTORY_LEAF_HORIZON_DAYS_MAX]) {
      assert.equal(outcomeOf({ historyLeafHorizonDays: days }), "accepted");
    }

    for (const days of [HISTORY_LEAF_HORIZON_DAYS_MIN - 1, HISTORY_LEAF_HORIZON_DAYS_MAX + 1, 32_768, U32_MAX + 1]) {
      assert.deepEqual(outcomeOf({ historyLeafHorizonDays: days }), refusedAt("retention.historyLeafHorizonDays"));
    }
  });

  test("the heartbeat horizon is accepted at 2 and 48 hours and refused at 1 and 49", () => {
    for (const hours of [HEARTBEAT_LEAF_HORIZON_HOURS_MIN, HEARTBEAT_LEAF_HORIZON_HOURS_MAX]) {
      assert.equal(outcomeOf({ heartbeatLeafHorizonHours: hours }), "accepted");
    }

    for (const hours of [HEARTBEAT_LEAF_HORIZON_HOURS_MIN - 1, HEARTBEAT_LEAF_HORIZON_HOURS_MAX + 1, U32_MAX + 1]) {
      assert.deepEqual(outcomeOf({ heartbeatLeafHorizonHours: hours }), refusedAt("retention.heartbeatLeafHorizonHours"));
    }
  });

  test("a count is accepted at 0 and 4294967295 and refused at -1 and 4294967296", () => {
    for (const batch of [0, U32_MAX]) {
      assert.equal(outcomeOf({ retentionDeleteBatchSize: batch }), "accepted");
    }

    for (const batch of [-1, U32_MAX + 1]) {
      assert.deepEqual(outcomeOf({ retentionDeleteBatchSize: batch }), refusedAt("retention.retentionDeleteBatchSize"));
    }
  });
});
