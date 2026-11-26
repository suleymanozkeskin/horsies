// The live check of `check --live`: the static check, then the live steps of the addon (`checkLive`): the
// connectivity of the database, then the connection fit of each worker profile. It stops at the first step that
// refuses. It installs no schema and writes nothing but the LISTEN probe's one NOTIFY.

import { HorsiesErrorCode } from "../_catalog/error_codes.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { refusalOfCore } from "../app/core-checks.ts";
import { refusal, type Refusal } from "../app/refusal.ts";
import type { LiveCheckOutcome } from "../native/worker/addon.ts";
import { unreachable } from "../native/call/never.ts";
import { type CheckOutcome, type CheckPassed, checkReport } from "./report.ts";

/** The place of a live check refusal. */
const BROKER_PATH = "broker";

/** The outcome of the live steps after a static check that passed with `passed`. Pure. */
export function liveCheckOutcome(passed: CheckPassed, live: LiveCheckOutcome): CheckOutcome {
  switch (live.kind) {
    case "passed":
      return { kind: "passed", passed };
    case "connectivityRefused":
      return refusedWith(live.step === "LISTENER" ? listenerRefusal(live.message) : connectivityRefusal(live.message));
    case "connectionsRefused": {
      const [first, ...rest] = live.refusals.map((refused) => refusalOfCore("workerProfiles", refused));

      if (first === undefined) {
        return raiseDefect(new ClientDefect({ subject: "checkLive", rule: "a connections refusal holds at least one refusal" }));
      }

      return refusedWith(first, ...rest);
    }

    default:
      return unreachable(live);
  }
}

function refusedWith(first: Refusal, ...rest: readonly Refusal[]): CheckOutcome {
  return { kind: "refused", report: checkReport("check", first, rest) };
}

/** A connect or a `SELECT 1` that failed (Python `_check_broker_connectivity`). Pure. */
function connectivityRefusal(cause: string): Refusal {
  return refusal(HorsiesErrorCode.DATABASE_INVALID_URL, BROKER_PATH, `broker connectivity check failed; ${cause}`, [
    "check broker.databaseUrl in Horsies.create",
  ]);
}

/** A session URL that delivers no NOTIFY to a LISTEN in PgBouncer transaction mode (Python `_run_listener_probe`). Pure. */
function listenerRefusal(cause: string): Refusal {
  return refusal(
    HorsiesErrorCode.DATABASE_INVALID_URL,
    BROKER_PATH,
    `broker listener capability check failed; the session database URL appears to be transaction-pooled: a LISTEN notification was not delivered; ${cause}`,
    ["use a direct or session-capable PostgreSQL URL for broker.sessionDatabase"],
  );
}
