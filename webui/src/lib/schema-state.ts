// Schema compatibility as reported by /api/meta.
//
// The monitoring layer never runs DDL: a missing or mismatched schema is a
// condition to report, never one to repair. `mismatch` degrades the UI to
// read-only; `absent`, `zero_x` (a horsies 0.x schema) and `unknown` are dead
// ends for the whole UI.

import type { MonitoringMeta } from '@/types/meta';

export type SchemaState =
  | 'match'
  | 'mismatch'
  | 'absent'
  | 'zero_x'
  | 'unknown';

/** The states that show the persistent read-only banner. */
export type BannerSchemaState = Extract<SchemaState, 'mismatch'>;

/** Maps the server's verdict to the UI state. Every server state has one
 * UI state, and a new server state fails the typecheck here. */
export function schemaState(meta: MonitoringMeta): SchemaState {
  switch (meta.schema_state) {
    case 'MATCH':
      return 'match';
    case 'MISMATCH':
      return 'mismatch';
    case 'ABSENT':
      return 'absent';
    case 'ZERO_X':
      return 'zero_x';
    case 'UNKNOWN':
      return 'unknown';
    default: {
      const unhandled: never = meta.schema_state;
      return unhandled;
    }
  }
}

export const NO_SCHEMA_MESSAGE =
  'This database has no horsies schema. Start a horsies app or worker to ' +
  'initialize it — the monitoring tool never modifies the database schema.';

/** Shown when the database holds a horsies 0.x schema. A worker start cannot
 * fix it, so the copy names the remedy that can. */
export const ZERO_X_SCHEMA_MESSAGE =
  'This database holds a horsies 0.x schema, which this version does not read ' +
  'or upgrade. Drain it and point the fleet and this tool at a new database.';

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

/** Banner text for each state that shows the banner. */
export function schemaBannerMessage(
  meta: MonitoringMeta,
  state: BannerSchemaState
): string {
  switch (state) {
    case 'mismatch':
      return schemaMismatchMessage(meta);
    default: {
      const unhandled: never = state;
      return unhandled;
    }
  }
}
