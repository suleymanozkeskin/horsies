// Schema compatibility as reported by /api/meta.
//
// The monitoring layer never runs DDL: a missing or mismatched schema is a
// condition to report, never one to repair. `mismatch` degrades the UI to
// read-only; `absent` and `unknown` are dead ends for the whole UI.

import type { MonitoringMeta } from '@/types/meta';

export type SchemaState = 'match' | 'mismatch' | 'absent' | 'unknown';

/**
 * `absent` and `unknown` both carry a null `schema_version`, so the version
 * alone cannot tell them apart. The reason code is the discriminator, and it
 * has to be checked first: only a SUCCESSFUL probe may report `absent`, and
 * reporting an unreachable database as an empty one would tell an operator to
 * initialize a database that is merely down.
 */
export function schemaState(meta: MonitoringMeta): SchemaState {
  if (meta.actions_disabled_reason === 'SCHEMA_UNKNOWN') {
    return 'unknown';
  }
  if (meta.schema_version === null) {
    return 'absent';
  }
  return meta.schema_compatible ? 'match' : 'mismatch';
}

export const NO_SCHEMA_MESSAGE =
  'This database has no horsies schema. Start a horsies app or worker to ' +
  'initialize it — the monitoring tool never modifies the database schema.';

/** Shown when the schema probe has never succeeded — a reachability problem,
 * not a verdict about the schema. */
export const SCHEMA_UNREACHABLE_MESSAGE =
  'Cannot reach the database to determine its schema state.';

/** Persistent read-only banner shown while the stored schema does not match. */
export function schemaMismatchMessage(meta: MonitoringMeta): string {
  return (
    `Schema v${meta.schema_version}; this UI expects ` +
    `v${meta.expected_schema_version}. Read-only mode: actions are disabled. ` +
    'Upgrade horsies (workers apply migrations) or use the matching UI version.'
  );
}
