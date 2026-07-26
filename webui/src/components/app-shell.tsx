// Application chrome and the boot-time gates.
//
// Four conditions stop the UI before any surface renders: the viewer is not
// authorized, /api/meta is unreachable, the database has no horsies schema, or
// its schema state could not be determined at all. A schema mismatch is not a
// stop — it degrades the whole UI to read-only with a persistent banner.

import type { ReactNode } from 'react';

import { AlertTriangle, Database, DatabaseZap, ShieldOff } from 'lucide-react';
import { Link, Outlet } from '@tanstack/react-router';

import { CapabilityProvider } from '@/actions/capability';
import { ErrorState } from '@/components/monitoring/states';
import { ThemeToggle } from '@/components/theme-toggle';
import { useLiveMode } from '@/events/live-provider';
import { useMeta } from '@/hooks/use-meta';
import { ApiError } from '@/lib/http';
import {
  NO_SCHEMA_MESSAGE,
  SCHEMA_UNREACHABLE_MESSAGE,
  schemaMismatchMessage,
  schemaState,
} from '@/lib/schema-state';
import type { MonitoringMeta } from '@/types/meta';

const NAV = [
  { to: '/', label: 'Tasks' },
  { to: '/workflows', label: 'Workflows' },
  { to: '/workers', label: 'Workers' },
] as const;

function FullScreen({
  icon: Icon,
  title,
  children,
}: {
  icon: typeof ShieldOff;
  title: string;
  children: ReactNode;
}) {
  return (
    <div className="flex min-h-screen items-center justify-center p-6">
      <div className="glass flex max-w-lg flex-col items-center gap-3 rounded-xl border border-border p-8 text-center">
        <Icon className="size-6 text-muted-foreground" aria-hidden />
        <h1 className="text-lg font-semibold">{title}</h1>
        {children}
      </div>
    </div>
  );
}

/** Read-only banner shown for the whole session while the schema mismatches. */
function SchemaBanner({ meta }: { meta: MonitoringMeta }) {
  return (
    <div
      role="status"
      className="flex items-center gap-2 border-b px-4 py-2 text-sm"
      style={{
        borderColor: 'var(--warning-dark)',
        background: 'var(--warning-bg)',
      }}
    >
      <AlertTriangle
        className="size-4 shrink-0"
        style={{ color: 'var(--warning-dark)' }}
        aria-hidden
      />
      <span>{schemaMismatchMessage(meta)}</span>
    </div>
  );
}

function LiveIndicator() {
  const mode = useLiveMode();
  return (
    <span
      className="flex items-center gap-1.5 text-xs text-muted-foreground"
      title={
        mode === 'events'
          ? 'Live updates are streaming from the server.'
          : 'The event stream is disconnected; falling back to interval polling.'
      }
    >
      <span
        className="size-2 rounded-full"
        style={{
          background: mode === 'events' ? 'var(--success)' : 'var(--warning-dark)',
        }}
        aria-hidden
      />
      {mode === 'events' ? 'live' : 'polling'}
    </span>
  );
}

export function AppShell() {
  const { meta, isLoading, error, refetch } = useMeta();

  if (error instanceof ApiError && error.status === 403) {
    return (
      <FullScreen icon={ShieldOff} title="Not authorized">
        <p className="text-sm text-muted-foreground">
          This deployment did not authorize you to view monitoring data.
        </p>
      </FullScreen>
    );
  }

  if (error !== null && error !== undefined) {
    return (
      <FullScreen icon={AlertTriangle} title="Could not reach the monitoring API">
        <ErrorState
          message={
            error instanceof ApiError
              ? (error.detail ?? `HTTP ${error.status}`)
              : 'The request could not be sent.'
          }
          onRetry={refetch}
        />
      </FullScreen>
    );
  }

  if (meta === undefined) {
    return (
      <FullScreen icon={Database} title="Loading">
        <p className="text-sm text-muted-foreground">
          {isLoading ? 'Contacting the monitoring API…' : 'No data.'}
        </p>
      </FullScreen>
    );
  }

  // Checked before `absent`: an unreachable database must never be reported as
  // an uninitialized one, which would tell an operator to initialize a database
  // that is merely down.
  if (schemaState(meta) === 'unknown') {
    return (
      <FullScreen icon={DatabaseZap} title="Database unreachable">
        <p className="text-sm text-muted-foreground">
          {SCHEMA_UNREACHABLE_MESSAGE}
        </p>
      </FullScreen>
    );
  }

  if (schemaState(meta) === 'absent') {
    return (
      <FullScreen icon={Database} title="No horsies schema">
        <p className="text-sm text-muted-foreground">{NO_SCHEMA_MESSAGE}</p>
      </FullScreen>
    );
  }

  return (
    <CapabilityProvider meta={meta}>
      <div className="flex min-h-screen flex-col">
        {schemaState(meta) === 'mismatch' && <SchemaBanner meta={meta} />}
        <header className="glass sticky top-0 z-40 flex flex-wrap items-center gap-4 border-b border-border px-4 py-3">
          <span className="font-mono text-sm font-semibold">horsies</span>
          <nav className="flex items-center gap-1">
            {NAV.map(item => (
              <Link
                key={item.to}
                to={item.to}
                className="rounded-md px-3 py-1.5 text-sm text-muted-foreground hover:bg-muted hover:text-foreground"
                activeProps={{
                  className:
                    'rounded-md px-3 py-1.5 text-sm bg-accent-surface text-foreground font-medium',
                }}
                activeOptions={{ exact: item.to === '/' }}
              >
                {item.label}
              </Link>
            ))}
          </nav>
          <div className="ml-auto flex items-center gap-4">
            <LiveIndicator />
            <span className="font-mono text-11 text-muted-foreground">
              v{meta.horsies_version}
            </span>
            <ThemeToggle />
          </div>
        </header>
        <main className="min-h-0 flex-1 p-4">
          <Outlet />
        </main>
      </div>
    </CapabilityProvider>
  );
}
