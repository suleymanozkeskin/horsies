// Application chrome and the boot-time gates.
//
// Five conditions stop the UI before any surface renders: the viewer is not
// authorized, /api/meta is unreachable, the database has no horsies schema, it
// holds a horsies 0.x schema, or its schema state could not be determined at
// all. A schema mismatch is not a stop — it degrades the whole UI to read-only
// with a persistent banner.
//
// Layout: a static backdrop fills the viewport. The top bar sits on it as
// glass capsules (brand, page switch, status). Every page renders in one
// window under the top bar; the window's <main> is the scroll container, so
// the document itself never scrolls.

import type { ReactNode } from 'react';

import { AlertTriangle, Database, DatabaseZap, ShieldOff } from 'lucide-react';
import { Link, Outlet } from '@tanstack/react-router';

import { CapabilityProvider } from '@/actions/capability';
import logoUrl from '@/assets/horsies-logo.svg';
import { ErrorState } from '@/components/monitoring/states';
import { ThemeToggle } from '@/components/theme-toggle';
import { Capsule, CapsuleSeparator } from '@/components/ui/capsule';
import { useLiveMode } from '@/events/live-provider';
import { useMeta } from '@/hooks/use-meta';
import { ApiError } from '@/lib/http';
import {
  NO_SCHEMA_MESSAGE,
  SCHEMA_UNREACHABLE_MESSAGE,
  ZERO_X_SCHEMA_MESSAGE,
  schemaBannerMessage,
  schemaState,
  type BannerSchemaState,
} from '@/lib/schema-state';
import type { MonitoringMeta } from '@/types/meta';

const NAV = [
  { to: '/', label: 'Tasks' },
  { to: '/workflows', label: 'Workflows' },
  { to: '/workers', label: 'Workers' },
] as const;

/** The static backdrop behind every screen: mesh, grid floor and grain. */
function Backdrop(): ReactNode {
  return (
    <div className="app-backdrop" aria-hidden>
      <div className="app-backdrop-floor" />
    </div>
  );
}

function FullScreen({
  icon: Icon,
  title,
  children,
}: {
  icon: typeof ShieldOff;
  title: string;
  children: ReactNode;
}): ReactNode {
  return (
    <>
      <Backdrop />
      <div className="flex min-h-full items-center justify-center p-6">
        <div className="content-surface flex max-w-lg flex-col items-center gap-3 rounded-2xl p-8 text-center">
          <Icon className="size-6 text-muted-foreground" aria-hidden />
          <h1 className="text-lg font-semibold">{title}</h1>
          {children}
        </div>
      </div>
    </>
  );
}

/** Read-only banner shown for the whole session while the schema mismatches. */
function SchemaBanner({
  meta,
  state,
}: {
  meta: MonitoringMeta;
  state: BannerSchemaState;
}): ReactNode {
  return (
    <div
      role="status"
      className="flex shrink-0 items-center gap-2 border-b px-(--shell-pad-inline) py-2 text-sm"
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
      <span>{schemaBannerMessage(meta, state)}</span>
    </div>
  );
}

function LiveIndicator(): ReactNode {
  const mode = useLiveMode();
  return (
    <span
      className="flex items-center gap-1.5 px-2 text-xs font-medium"
      title={
        mode === 'events'
          ? 'Live updates are streaming from the server.'
          : 'The event stream is disconnected; falling back to interval polling.'
      }
    >
      <span
        className="size-2 shrink-0 rounded-full"
        style={{
          background: mode === 'events' ? 'var(--success)' : 'var(--warning-dark)',
        }}
        aria-hidden
      />
      <span className="max-sm:sr-only">
        {mode === 'events' ? 'live' : 'polling'}
      </span>
    </span>
  );
}

/** The top bar: three capsules on the backdrop. */
function TopBar({ version }: { version: string }): ReactNode {
  return (
    <header className="fixed inset-x-(--shell-gap) top-0 z-40 flex h-(--shell-header) items-center gap-2.5 max-sm:gap-1.5 max-sm:overflow-x-auto max-sm:[scrollbar-width:none]">
      <Link
        to="/"
        className="liquid-glass inline-flex h-(--capsule-height) shrink-0 items-center gap-2 rounded-full py-1 pr-4 pl-1 focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring max-sm:pr-1"
      >
        <img src={logoUrl} alt="" className="size-8" />
        <span className="font-mono text-sm font-semibold max-sm:sr-only">
          horsies
        </span>
      </Link>
      <nav aria-label="Pages">
        <Capsule>
          {NAV.map(item => (
            <Link
              key={item.to}
              to={item.to}
              className="inline-flex h-(--capsule-item) items-center rounded-full px-4 text-sm font-medium text-foreground transition-colors focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring max-sm:px-3"
              activeProps={{
                className: 'bg-nav-on font-semibold shadow-(--segment-on-shadow)',
              }}
              inactiveProps={{ className: 'hover:bg-surface-hover' }}
              activeOptions={{ exact: item.to === '/' }}
            >
              {item.label}
            </Link>
          ))}
        </Capsule>
      </nav>
      <Capsule className="ml-auto pl-2">
        <LiveIndicator />
        <CapsuleSeparator className="max-sm:hidden" />
        <span className="px-1 font-mono text-xs font-medium max-sm:hidden">
          v{version}
        </span>
        <ThemeToggle />
      </Capsule>
    </header>
  );
}

export function AppShell(): ReactNode {
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

  if (schemaState(meta) === 'zero_x') {
    return (
      <FullScreen icon={Database} title="horsies 0.x schema">
        <p className="text-sm text-muted-foreground">{ZERO_X_SCHEMA_MESSAGE}</p>
      </FullScreen>
    );
  }

  const state = schemaState(meta);
  return (
    <CapabilityProvider meta={meta}>
      <Backdrop />
      <TopBar version={meta.horsies_version} />
      <div className="window-surface fixed inset-x-(--shell-gap) top-(--shell-header) bottom-(--shell-gap) flex flex-col overflow-hidden rounded-2xl">
        {state === 'mismatch' && <SchemaBanner meta={meta} state={state} />}
        <div className="relative flex min-h-0 flex-1 flex-col">
          <main className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-(--shell-pad-inline) py-(--shell-pad-block)">
            <Outlet />
          </main>
          {/* Content that scrolls past the top padding fades out at the
              window edge. A sticky element stops below this band. */}
          <div
            aria-hidden
            className="pointer-events-none absolute inset-x-0 top-0 z-30 h-(--shell-pad-block) bg-linear-to-b from-window-edge to-transparent"
          />
        </div>
      </div>
    </CapabilityProvider>
  );
}
