import { describe, expect, it } from 'vitest';

import { deriveCanAct } from '@/actions/capability';
import {
  NO_SCHEMA_MESSAGE,
  SCHEMA_UNREACHABLE_MESSAGE,
  ZERO_X_SCHEMA_MESSAGE,
  schemaBannerMessage,
  schemaMismatchMessage,
  schemaState,
} from '@/lib/schema-state';
import type { MonitoringMeta } from '@/types/meta';

const meta = (overrides: Partial<MonitoringMeta> = {}): MonitoringMeta => ({
  horsies_version: '0.3.1',
  base_path: '/',
  actions_enabled: true,
  can_act: true,
  schema_version: 14,
  expected_schema_version: 14,
  schema_compatible: true,
  schema_state: 'MATCH',
  actions_disabled_reason: null,
  trace_link_template: null,
  ...overrides,
});

describe('schema state', () => {
  const cases: ReadonlyArray<[MonitoringMeta['schema_state'], string]> = [
    ['MATCH', 'match'],
    ['MISMATCH', 'mismatch'],
    ['ABSENT', 'absent'],
    ['ZERO_X', 'zero_x'],
    ['UNKNOWN', 'unknown'],
  ];

  it.each(cases)('maps the server state %s to %s', (serverState, expected) => {
    expect(schemaState(meta({ schema_state: serverState }))).toBe(expected);
  });

  // Regression: the state used to be derived from the version numbers. The
  // server state decides, whatever the numbers say.
  it('takes the server state over any version the payload carries', () => {
    expect(
      schemaState(
        meta({
          schema_state: 'MISMATCH',
          schema_version: 14,
          expected_schema_version: 14,
          schema_compatible: false,
        })
      )
    ).toBe('mismatch');
    expect(
      schemaState(meta({ schema_state: 'UNKNOWN', schema_version: 14 }))
    ).toBe('unknown');
    expect(
      schemaState(
        meta({ schema_state: 'ZERO_X', schema_version: 65, expected_schema_version: 1000 })
      )
    ).toBe('zero_x');
    expect(
      schemaState(
        meta({ schema_state: 'ABSENT', schema_version: null })
      )
    ).toBe('absent');
  });
});

describe('schema copy', () => {
  it('names both versions and states the remedy', () => {
    const mismatched = meta({
      schema_version: 13,
      expected_schema_version: 14,
      schema_compatible: false,
    });
    expect(schemaMismatchMessage(mismatched)).toBe(
      'Schema v13; this UI expects v14. Read-only mode: actions are disabled. ' +
        'Upgrade horsies (workers apply migrations) or use the matching UI version.'
    );
    expect(schemaBannerMessage(mismatched, 'mismatch')).toBe(
      schemaMismatchMessage(mismatched)
    );
  });

  it('names a new database as the remedy for a 0.x schema', () => {
    expect(ZERO_X_SCHEMA_MESSAGE).toBe(
      'This database holds a horsies 0.x schema, which this version does not ' +
        'read or upgrade. Drain it and point the fleet and this tool at a new ' +
        'database.'
    );
    // A worker start cannot fix a 0.x database: the copy must not say so.
    expect(ZERO_X_SCHEMA_MESSAGE).not.toContain('Start a horsies app or worker');
  });

  it('tells the operator that the tool never initializes the schema', () => {
    expect(NO_SCHEMA_MESSAGE).toBe(
      'This database has no horsies schema. Start a horsies app or worker to ' +
        'initialize it — the monitoring tool never modifies the database schema.'
    );
  });

  it('states unreachability without claiming anything about the schema', () => {
    expect(SCHEMA_UNREACHABLE_MESSAGE).toBe(
      'Cannot reach the database to determine its schema state.'
    );
    // The two states must never share copy: one says "initialize this", the
    // other says "we could not look".
    expect(SCHEMA_UNREACHABLE_MESSAGE).not.toBe(NO_SCHEMA_MESSAGE);
  });
});

describe('action capability', () => {
  it('allows actions only when server config, policy and schema all agree', () => {
    expect(deriveCanAct(meta(), false)).toBe(true);
  });

  it('denies actions when the server disabled them', () => {
    expect(deriveCanAct(meta({ actions_enabled: false }), false)).toBe(false);
  });

  it('denies actions when the policy says view-only', () => {
    expect(deriveCanAct(meta({ can_act: false }), false)).toBe(false);
  });

  it('denies actions on a schema mismatch even if the flags were left true', () => {
    expect(
      deriveCanAct(meta({ schema_version: 13, schema_compatible: false }), false)
    ).toBe(false);
  });

  it('denies actions once a 403 has revoked them for the session', () => {
    expect(deriveCanAct(meta(), true)).toBe(false);
  });

  it('denies actions whenever the server set a reason, whatever the flags say', () => {
    expect(
      deriveCanAct(meta({ actions_disabled_reason: 'SCHEMA_UNKNOWN' }), false)
    ).toBe(false);
    expect(
      deriveCanAct(
        meta({ actions_disabled_reason: 'SCHEMA_INCOMPATIBLE' }),
        false
      )
    ).toBe(false);
  });
});
