// `GET /api/meta` — deployment capabilities the SPA gates its UI on.

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
  /** False on any mismatch, and on an absent schema. */
  schema_compatible: boolean;
  actions_disabled_reason: 'SCHEMA_INCOMPATIBLE' | null;
}
