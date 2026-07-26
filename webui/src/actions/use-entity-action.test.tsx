import type { ReactNode } from 'react';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { CapabilityProvider } from '@/actions/capability';
import { useEntityAction } from '@/actions/use-entity-action';
import { ToastProvider } from '@/components/ui/toast';
import { LiveProvider } from '@/events/live-provider';
import type { MonitoringMeta } from '@/types/meta';

interface Row {
  status: string;
}

const META: MonitoringMeta = {
  horsies_version: '0.3.1',
  base_path: '/',
  actions_enabled: true,
  can_act: true,
  schema_version: 14,
  expected_schema_version: 14,
  schema_compatible: true,
  actions_disabled_reason: null,
};

const jsonResponse = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return (
    <QueryClientProvider client={client}>
      <ToastProvider>
        <LiveProvider>
          <CapabilityProvider meta={META}>{children}</CapabilityProvider>
        </LiveProvider>
      </ToastProvider>
    </QueryClientProvider>
  );
}

interface Harness {
  detail?: Row;
  reread?: () => Promise<Row | undefined>;
  onGone?: () => void;
}

function renderAction(harness: Harness = {}) {
  const detail: Row = harness.detail ?? { status: 'PENDING' };
  return renderHook(
    () =>
      useEntityAction<Row>({
        entity: { kind: 'task', id: 'task-1' },
        detail,
        snapshot: () => ({ wasStatus: detail.status, retryCountBefore: 0 }),
        hasSettled: (_action, row) => row.status === 'CANCELLED',
        reread: harness.reread ?? (async () => detail),
        successContext: () => ({ drainingNodes: 0, workerHostname: 'box-1' }),
        ...(harness.onGone === undefined ? {} : { onGone: harness.onGone }),
      }),
    { wrapper }
  );
}

/** click -> confirm, which is the only path that issues the POST. */
async function submit(result: {
  current: ReturnType<typeof useEntityAction<Row>>;
}): Promise<void> {
  await act(async () => {
    result.current.begin('task-cancel');
  });
  await act(async () => {
    result.current.confirm({ includeRunning: false });
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('successful action', () => {
  it('enters settling and reports what happens to the running process', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        jsonResponse(200, { outcome: 'cancelled', was_status: 'RUNNING' })
      )
    );
    const { result } = renderAction();

    await submit(result);

    expect(result.current.state.phase).toBe('settling');
    expect(
      await screen.findByText(
        'Task cancelled. The running process on box-1 will keep executing until it finishes.'
      )
    ).toBeTruthy();
  });

  it('settles immediately once the refetched row shows the effect', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        jsonResponse(200, { outcome: 'cancelled', was_status: 'PENDING' })
      )
    );
    const { result } = renderAction({ detail: { status: 'CANCELLED' } });

    await submit(result);

    await waitFor(() => expect(result.current.state.phase).toBe('idle'));
  });
});

describe('distinct failure handling', () => {
  it('404 closes the entity and names retention as the likely cause', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => jsonResponse(404, { detail: 'Task not found.' }))
    );
    const onGone = vi.fn();
    const { result } = renderAction({ onGone });

    await submit(result);

    expect(onGone).toHaveBeenCalledOnce();
    expect(
      await screen.findByText(
        'Task no longer exists (retention may have removed it).'
      )
    ).toBeTruthy();
    await waitFor(() => expect(result.current.state.phase).toBe('idle'));
  });

  it('409 reports the re-read status and releases the entity', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        jsonResponse(409, {
          code: 'TASK_NOT_CANCELLABLE',
          current_status: 'COMPLETED',
        })
      )
    );
    const { result } = renderAction();

    await submit(result);

    expect(
      await screen.findByText('Cannot cancel: the state is now completed.')
    ).toBeTruthy();
    await waitFor(() => expect(result.current.state.phase).toBe('idle'));
  });

  it('503 stays failed and offers a retry, because the action may still be possible', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        jsonResponse(503, { detail: 'task action failed: broker down' })
      )
    );
    const { result } = renderAction();

    await submit(result);

    expect(result.current.state.phase).toBe('failed');
    expect(
      await screen.findByText(
        'Cancel failed: task action failed: broker down. The broker may be unreachable.'
      )
    ).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeTruthy();
  });

  it('403 revokes the capability instead of retrying', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => jsonResponse(403, { detail: 'no' })));
    const { result } = renderAction();

    await submit(result);

    expect(
      await screen.findByText('Not authorized to perform actions.')
    ).toBeTruthy();
    expect(result.current.state.phase).toBe('idle');
  });

  it('409 SCHEMA_INCOMPATIBLE is not treated as a lost race', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        jsonResponse(409, {
          code: 'SCHEMA_INCOMPATIBLE',
          detail: 'schema v13 != v14',
        })
      )
    );
    const { result } = renderAction();

    await submit(result);

    expect(
      await screen.findByText(
        'Actions are disabled: the database schema does not match this UI version.'
      )
    ).toBeTruthy();
    expect(result.current.state.phase).toBe('idle');
  });

  it('400 TASK_IS_WORKFLOW_TASK explains that the workflow owns the row', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => jsonResponse(400, { code: 'TASK_IS_WORKFLOW_TASK' }))
    );
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { result } = renderAction();

    await submit(result);

    expect(
      await screen.findByText(
        'This task is managed by its workflow; task-level actions do not apply.'
      )
    ).toBeTruthy();
    expect(result.current.state.phase).toBe('idle');
  });
});

describe('lost response', () => {
  it('treats an observed effect as success', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => {
        throw new TypeError('Failed to fetch');
      })
    );
    const { result } = renderAction({
      reread: async () => ({ status: 'CANCELLED' }),
    });

    await submit(result);

    expect(await screen.findByText('Cancel applied.')).toBeTruthy();
  });

  it('reports failure with a retry when the effect is absent', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => {
        throw new TypeError('Failed to fetch');
      })
    );
    const { result } = renderAction({
      reread: async () => ({ status: 'PENDING' }),
    });

    await submit(result);

    expect(result.current.state.phase).toBe('failed');
    expect(
      await screen.findByText(
        'Cancel could not be confirmed. Nothing appears to have changed.'
      )
    ).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeTruthy();
  });
});

describe('one in-flight action per entity', () => {
  it('is busy from the click until the lifecycle returns to idle', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => jsonResponse(409, { code: 'STATE_CONFLICT', current_status: 'FAILED' }))
    );
    const { result } = renderAction();

    expect(result.current.busy).toBe(false);
    await act(async () => {
      result.current.begin('task-cancel');
    });
    expect(result.current.busy).toBe(true);

    await act(async () => {
      result.current.confirm({ includeRunning: false });
    });
    await waitFor(() => expect(result.current.busy).toBe(false));
  });

  it('ignores a confirm that was not preceded by a click', async () => {
    const fetchMock = vi.fn(async () => jsonResponse(200, { outcome: 'cancelled' }));
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderAction();

    await act(async () => {
      result.current.confirm({ includeRunning: false });
    });

    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current.state.phase).toBe('idle');
  });
});
