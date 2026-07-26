// Schema compatibility as reported by /api/meta.
//
// The monitoring layer never runs DDL: a missing or mismatched schema is a
// condition to report, never one to repair. `absent` is a dead end for the whole
// UI; `mismatch` degrades it to read-only.

import type { MonitoringMeta } from '@/types/meta';

export type SchemaState = 'match' | 'mismatch' | 'absent';

export function schemaState(meta: MonitoringMeta): SchemaState {
  if (meta.schema_version === null) {
    return 'absent';
  }
  return meta.schema_compatible ? 'match' : 'mismatch';
}

export const NO_SCHEMA_MESSAGE =
  'This database has no horsies schema. Start a horsies app or worker to ' +
  'initialize it — the monitoring tool never modifies the database schema.';

/** Persistent read-only banner shown while the stored schema does not match. */
export function schemaMismatchMessage(meta: MonitoringMeta): string {
  return (
    `Schema v${meta.schema_version}; this UI expects ` +
    `v${meta.expected_schema_version}. Read-only mode: actions are disabled. ` +
    'Upgrade horsies (workers apply migrations) or use the matching UI version.'
  );
}
