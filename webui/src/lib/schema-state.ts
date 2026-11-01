// Schema compatibility as reported by /api/meta.
//
// The monitoring layer never runs DDL: a missing or mismatched schema is a
// condition to report, never one to repair. `mismatch` degrades the UI to
// read-only, and so does `cutover_required` (versions equal, offline cutover
// incomplete); `absent` and `unknown` are dead ends for the whole UI.

import type { MonitoringMeta } from '@/types/meta';

export type SchemaState =
  | 'match'
  | 'mismatch'
  | 'cutover_required'
  | 'absent'
  | 'unknown';

/** The states that show the persistent read-only banner. */
export type BannerSchemaState = Extract<
  SchemaState,
  'mismatch' | 'cutover_required'
>;

/** Maps the server's verdict to the UI state. Every server state has one
 * UI state, and a new server state fails the typecheck here. */
export function schemaState(meta: MonitoringMeta): SchemaState {
  switch (meta.schema_state) {
    case 'MATCH':
      return 'match';
    case 'MISMATCH':
      return 'mismatch';
    case 'CUTOVER_REQUIRED':
      return 'cutover_required';
    case 'ABSENT':
      return 'absent';
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

/** Persistent read-only banner shown while the offline task-history cutover
 * is incomplete. The versions are equal, so only one is named. */
export function schemaCutoverMessage(meta: MonitoringMeta): string {
  return (
    `Schema v${meta.schema_version} is current, but the offline task-history ` +
    'cutover is not complete. Read-only mode: actions are disabled. ' +
    'Run the cutover stages through tighten and validation. ' +
    'See the 0.5.0 migration guide.'
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
    case 'cutover_required':
      return schemaCutoverMessage(meta);
    default: {
      const unhandled: never = state;
      return unhandled;
    }
  }
}
