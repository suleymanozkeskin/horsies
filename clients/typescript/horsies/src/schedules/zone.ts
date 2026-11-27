// The zone check of a schedule. The core's zone data decides, through one pure addon call on a main thread: the
// scheduler start reads the same table (`UnknownTimezone`), so a name that the check accepts also starts. The zone
// data of the runtime (`Temporal`) words the refusal only: an offset, another case, a name of neither table. In a
// worker thread no zone check runs: no scheduler and no `check` runs there.

import type { HorsiesError } from "../app/refusal.ts";
import type { TimezoneCheck } from "../native/scheduler/addon.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";

/** How the runtime's zone data reads a time zone text. */
export type ZoneReading =
  /** An IANA zone name as the runtime's zone data writes it. */
  | { readonly kind: "zoneName" }
  /** A UTC offset (`+05:00`), also inside a date-time text: the zone of a schedule comes from an IANA name. */
  | { readonly kind: "offset" }
  /** A zone name in another case than the zone data's (`europe/berlin`). */
  | { readonly kind: "otherSpelling"; readonly spelling: string }
  /** No zone of the runtime. */
  | { readonly kind: "unknown" };

/** The start of a UTC offset text. An IANA zone name starts with a letter. */
const OFFSET_SIGNS: readonly string[] = ["+", "-"];

function isOffset(text: string): boolean {
  return OFFSET_SIGNS.some((sign) => text.startsWith(sign));
}

/** How the runtime's zone data (`Temporal`) reads `name`. Pure: reads no clock. */
export function zoneReading(name: string): ZoneReading {
  if (isOffset(name)) {
    return { kind: "offset" };
  }

  let zoneId: string;

  try {
    zoneId = Temporal.ZonedDateTime.from({ timeZone: name, year: 2000, month: 1, day: 1 }).timeZoneId;
  } catch (cause) {
    // `Temporal` refuses a zone that its data does not hold with a `RangeError`; any other throw passes on.
    if (cause instanceof RangeError) {
      return { kind: "unknown" };
    }

    throw cause;
  }

  if (isOffset(zoneId)) {
    return { kind: "offset" };
  }

  return zoneId === name ? { kind: "zoneName" } : { kind: "otherSpelling", spelling: zoneId };
}

/** The zone check of the core's zone data on a main thread, or none in a worker thread. */
export type ZoneCheck = { readonly kind: "core"; readonly check: (name: string) => TimezoneCheck } | { readonly kind: "noCore" };

/** What a zone refusal says: the rule that the name breaks, and how to fix it. */
type ZoneRule = { readonly message: string; readonly help: string };

const ZONE_HELP = 'use an IANA zone name, for example "UTC" or "Europe/Berlin"';

/** `name` when the core's zone data holds it (always in a worker thread), else the refusal of `refuse`. */
export function checkedZone(name: string, zones: ZoneCheck, refuse: (rule: ZoneRule) => HorsiesError): Result<string, HorsiesError> {
  switch (zones.kind) {
    case "noCore":
      return ok(name);
    case "core":
      return coreZone(name, zones.check, refuse);
    default:
      return unreachable(zones, "checkedZone");
  }
}

/** `name` when the core's zone data holds it, else the refusal, worded from the runtime's reading of `name`. */
function coreZone(name: string, check: (name: string) => TimezoneCheck, refuse: (rule: ZoneRule) => HorsiesError): Result<string, HorsiesError> {
  const answer = check(name);

  switch (answer) {
    case "known":
      return ok(name);
    case "unknown":
      return err(refuse(unknownZoneRule(name, zoneReading(name), check)));
    default:
      return unreachable(answer, "coreZone");
  }
}

/** The words of the refusal of `name`, which the core's zone data does not hold. Pure apart from `check`. */
function unknownZoneRule(name: string, reading: ZoneReading, check: (name: string) => TimezoneCheck): ZoneRule {
  switch (reading.kind) {
    case "offset":
      return { message: `timezone: "${name}" is a UTC offset, not a zone name`, help: ZONE_HELP };
    case "otherSpelling":
      return check(reading.spelling) === "known"
        ? { message: `timezone: the zone data writes "${name}" as "${reading.spelling}"`, help: `write "${reading.spelling}"` }
        : { message: `timezone: "${name}" is not an IANA zone name of the core's zone data`, help: ZONE_HELP };
    case "zoneName":
    case "unknown":
      return { message: `timezone: "${name}" is not an IANA zone name of the core's zone data`, help: ZONE_HELP };
    default:
      return unreachable(reading, "unknownZoneRule");
  }
}
