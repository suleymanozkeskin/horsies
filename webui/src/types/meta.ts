// `GET /api/meta` — deployment capabilities the SPA gates its UI on.

/** The server's schema verdict, as `SchemaState` in `horsies/web/schema.py`. */
export type ServerSchemaState =
  | 'MATCH'
  | 'MISMATCH'
  | 'CUTOVER_REQUIRED'
  | 'ABSENT'
  | 'UNKNOWN';

export interface MonitoringMeta {
  horsies_version: string;
  /** Mount path the SPA is served under. */
  base_path: string;
  /** Static server config ANDed with schema compatibility. */
  actions_enabled: boolean;
  /** This request's auth-policy verdict for mutating endpoints. */
  can_act: boolean;
  /** Version stored in the database; null when no horsies schema exists. */
  schema_version: number | null;
  /** Version this build was written against. */
  expected_schema_version: number;
  /** False on any mismatch, and on an absent or undetermined schema. */
  schema_compatible: boolean;
  /** The schema probe's verdict. The SPA maps it; it never derives it. */
  schema_state: ServerSchemaState;
  /** Why the server force-disabled actions, independent of the auth policy.
   * `SCHEMA_UNKNOWN` means the schema probe has never succeeded — it is the
   * only signal that separates an unreachable database from an empty one. */
  actions_disabled_reason: 'SCHEMA_INCOMPATIBLE' | 'SCHEMA_UNKNOWN' | null;
}
