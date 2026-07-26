import { describe, expect, it } from 'vitest';

import { deriveCanAct } from '@/actions/capability';
import {
  NO_SCHEMA_MESSAGE,
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
  actions_disabled_reason: null,
  ...overrides,
});

describe('schema state', () => {
  it('matches when the stored version equals the expected one', () => {
    expect(schemaState(meta())).toBe('match');
  });

  it('mismatches on an older stored version', () => {
    expect(
      schemaState(meta({ schema_version: 13, schema_compatible: false }))
    ).toBe('mismatch');
  });

  it('mismatches on a newer stored version', () => {
    expect(
      schemaState(meta({ schema_version: 15, schema_compatible: false }))
    ).toBe('mismatch');
  });

  it('reports absent when no version row exists, whatever the compatibility flag says', () => {
    expect(
      schemaState(meta({ schema_version: null, schema_compatible: false }))
    ).toBe('absent');
    expect(
      schemaState(meta({ schema_version: null, schema_compatible: true }))
    ).toBe('absent');
  });
});

describe('schema copy', () => {
  it('names both versions and states the remedy', () => {
    expect(
      schemaMismatchMessage(
        meta({
          schema_version: 13,
          expected_schema_version: 14,
          schema_compatible: false,
        })
      )
    ).toBe(
      'Schema v13; this UI expects v14. Read-only mode: actions are disabled. ' +
        'Upgrade horsies (workers apply migrations) or use the matching UI version.'
    );
  });

  it('tells the operator that the tool never initializes the schema', () => {
    expect(NO_SCHEMA_MESSAGE).toBe(
      'This database has no horsies schema. Start a horsies app or worker to ' +
        'initialize it — the monitoring tool never modifies the database schema.'
    );
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
});
