// Deployment capability gate.
//
// A view-only deployment must not advertise what it cannot do: when actions are
// off, the affordances are ABSENT rather than disabled-with-a-tooltip. A 403 on
// a POST revokes the capability for the rest of the session — the server is the
// authority, and it just said no. A schema mismatch disables actions the same
// way: the server already refuses them, and the UI must not offer what will be
// rejected.

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from 'react';

import { schemaState, type SchemaState } from '@/lib/schema-state';
import type { MonitoringMeta } from '@/types/meta';

interface Capability {
  /** True only when the server enabled actions, the schema matches, AND this
   * viewer may act. */
  canAct: boolean;
  schema: SchemaState;
  meta: MonitoringMeta;
  /** Called after a 403 or a SCHEMA_INCOMPATIBLE rejection on a POST. */
  revokeActions: () => void;
}

const CapabilityContext = createContext<Capability | null>(null);

export function useCapability(): Capability {
  const capability = useContext(CapabilityContext);
  if (capability === null) {
    throw new Error('useCapability must be used inside <CapabilityProvider>.');
  }
  return capability;
}

/** Pure gate, so the precedence between the inputs is testable. A server-set
 * `actions_disabled_reason` is authoritative on its own: the endpoints already
 * refuse, so the UI must not offer what will be rejected. */
export function deriveCanAct(meta: MonitoringMeta, revoked: boolean): boolean {
  return (
    !revoked &&
    meta.actions_enabled &&
    meta.can_act &&
    meta.schema_compatible &&
    meta.actions_disabled_reason === null
  );
}

export function CapabilityProvider({
  meta,
  children,
}: {
  meta: MonitoringMeta;
  children: ReactNode;
}): ReactNode {
  const [revoked, setRevoked] = useState(false);
  const revokeActions = useCallback(() => setRevoked(true), []);

  const value = useMemo<Capability>(
    () => ({
      canAct: deriveCanAct(meta, revoked),
      schema: schemaState(meta),
      meta,
      revokeActions,
    }),
    [revoked, meta, revokeActions]
  );

  return (
    <CapabilityContext.Provider value={value}>
      {children}
    </CapabilityContext.Provider>
  );
}
