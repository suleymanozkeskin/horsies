import { cloneElement, type ReactElement, type ReactNode } from 'react';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  chartPoints,
  seriesValueText,
  WorkerHistoryChart,
} from '@/components/monitoring/worker-history-chart';
import { statusColorVar } from '@/lib/status-utils';
import type { WorkerHistoryPoint } from '@/types/workers';

/**
 * jsdom gives every element a size of 0, and a chart with no size draws no
 * series. The container gives the chart a fixed size here, so the tests can
 * read which series are drawn.
 */
vi.mock('recharts', async importOriginal => {
  const actual = await importOriginal<typeof import('recharts')>();
  return {
    ...actual,
    ResponsiveContainer: ({
      children,
    }: {
      children: ReactElement<{ width?: number; height?: number }>;
    }) => cloneElement(children, { width: 600, height: 140 }),
  };
});

/**
 * Focusing another worker changes the query key, so the new worker's history
 * starts from nothing cached. Carrying the previous worker's series over would
 * caption one worker's data with another's name, so the panel instead keeps its
 * two chart blocks and their heights while the request is in flight — otherwise
 * the whole focus card collapses and re-expands on every click.
 */

const point = (
  running: number,
  fields: Partial<WorkerHistoryPoint> = {}
): WorkerHistoryPoint => ({
  snapshot_at: '2026-08-12T10:00:00Z',
  tasks_running: running,
  tasks_claimed: 0,
  cpu_percent: 10,
  memory_percent: 20,
  memory_usage_mb: 512,
  ...fields,
});

const jsonRows = (rows: WorkerHistoryPoint[]): Promise<Response> =>
  Promise.resolve(
    new Response(JSON.stringify(rows), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  );

const stubFetch = (rows: WorkerHistoryPoint[] | 'pending'): void => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() =>
      rows === 'pending' ? new Promise<Response>(() => {}) : jsonRows(rows)
    )
  );
};

/** Answers for the first worker; leaves any other worker's request in flight. */
const stubFetchPerWorker = (answered: string, rows: WorkerHistoryPoint[]): void => {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) =>
      String(input).includes(encodeURIComponent(answered))
        ? jsonRows(rows)
        : new Promise<Response>(() => {})
    )
  );
};

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

/** The fixed-height body of each chart slot. The chart inside a body has the
 * same height, so it is left out. */
const slotBodies = (container: HTMLElement): HTMLElement[] =>
  Array.from(container.querySelectorAll<HTMLElement>('[style*="height"]')).filter(
    element =>
      element.style.height === '140px' &&
      !element.classList.contains('recharts-wrapper')
  );

/** The line of each series that the two charts draw, in document order. */
const seriesLines = (container: HTMLElement): Element[] =>
  Array.from(
    container.querySelectorAll('.recharts-area-curve, .recharts-line-curve')
  );

/** The name of each series that the two charts draw, in document order. */
const drawnSeries = (container: HTMLElement): (string | null)[] =>
  seriesLines(container).map(line => line.getAttribute('name'));

/** The tick values of the percent axis, which only the process chart has. */
const percentTicks = (container: HTMLElement): number[] =>
  Array.from(
    container.querySelectorAll(
      '.recharts-yAxis-tick-labels .recharts-cartesian-axis-tick-value'
    )
  )
    .map(tick => tick.textContent ?? '')
    .filter(text => text.endsWith('%'))
    .map(text => Number.parseFloat(text));

const legendItem = (label: string): HTMLElement =>
  screen.getByRole('button', { name: label });

/** The legend of a chart, found by the chart title. */
const legendOf = (chart: string): HTMLElement =>
  screen.getByRole('group', { name: `Series of ${chart}` });

/** The items of a legend, in order. */
const itemsOf = (legend: HTMLElement): HTMLElement[] =>
  within(legend).getAllByRole('button');

/** Whether `later` comes after `earlier` in the document, outside it. */
const comesAfter = (earlier: Node, later: Node): boolean =>
  !earlier.contains(later) &&
  (earlier.compareDocumentPosition(later) & Node.DOCUMENT_POSITION_FOLLOWING) !==
    0;

/** Renders the charts of a worker with two snapshots and waits for the series. */
async function renderWithHistory(): Promise<HTMLElement> {
  stubFetch([point(3), point(1)]);
  const { container } = render(<WorkerHistoryChart workerId="worker-1" />, {
    wrapper,
  });
  await waitFor(() => expect(drawnSeries(container)).toHaveLength(4));
  return container;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('WorkerHistoryChart', () => {
  it('holds both chart blocks while a worker’s history is in flight', () => {
    stubFetch('pending');
    const { container } = render(<WorkerHistoryChart workerId="worker-1" />, {
      wrapper,
    });

    expect(slotBodies(container)).toHaveLength(2);
    expect(container.textContent).toContain('Load');
    expect(container.textContent).toContain('Worker process');
    expect(container.textContent).toContain('Loading history…');
    expect(itemsOf(legendOf('Load'))).toHaveLength(2);
    expect(itemsOf(legendOf('Worker process'))).toHaveLength(2);
  });

  it('holds both chart blocks for a worker that has recorded nothing', async () => {
    stubFetch([]);
    const { container } = render(<WorkerHistoryChart workerId="worker-1" />, {
      wrapper,
    });

    await waitFor(() =>
      expect(container.textContent).toContain('No history recorded yet.')
    );
    expect(slotBodies(container)).toHaveLength(2);
  });

  it('holds the blocks — without the old series — when the focus moves to another worker', async () => {
    stubFetchPerWorker('worker-1', [point(3), point(1)]);
    const { container, rerender } = render(
      <WorkerHistoryChart workerId="worker-1" />,
      { wrapper }
    );

    await waitFor(() =>
      expect(container.textContent).not.toContain('Loading history…')
    );
    expect(slotBodies(container)).toHaveLength(2);

    rerender(<WorkerHistoryChart workerId="worker-2" />);

    // Worker 2's request is still in flight: the blocks stay, and they state
    // that they are loading rather than showing worker 1's series under it.
    await waitFor(() =>
      expect(container.textContent).toContain('Loading history…')
    );
    expect(slotBodies(container)).toHaveLength(2);
    expect(drawnSeries(container)).toEqual([]);
  });

  it('says that the process chart leaves the task children out', () => {
    stubFetch('pending');
    const { container } = render(<WorkerHistoryChart workerId="worker-1" />, {
      wrapper,
    });

    expect(container.textContent).toContain(
      'The worker process only. Its task children are not in these numbers.'
    );
  });

  it('puts a legend under each chart, with one item per series', async () => {
    const container = await renderWithHistory();
    const bodies = slotBodies(container);
    expect(bodies).toHaveLength(2);
    const [loadBody, processBody] = bodies;
    const loadLegend = legendOf('Load');
    const processLegend = legendOf('Worker process');

    expect(itemsOf(loadLegend).map(item => item.textContent)).toEqual([
      'running',
      'claimed',
    ]);
    expect(itemsOf(processLegend).map(item => item.textContent)).toEqual([
      'CPU',
      'memory',
    ]);
    // Each item carries the icon of its chart's mark: a block for an area,
    // a line with a dot for a line.
    for (const item of itemsOf(loadLegend)) {
      expect(item.querySelector('svg rect')).not.toBeNull();
    }
    for (const item of itemsOf(processLegend)) {
      expect(item.querySelector('svg circle')).not.toBeNull();
    }
    expect(comesAfter(loadBody!, loadLegend)).toBe(true);
    expect(comesAfter(loadLegend, processBody!)).toBe(true);
    expect(comesAfter(processBody!, processLegend)).toBe(true);
  });

  it('draws the four series before a click', async () => {
    const container = await renderWithHistory();

    expect(drawnSeries(container)).toEqual([
      'running',
      'claimed',
      'CPU',
      'memory',
    ]);
    for (const label of ['running', 'claimed', 'CPU', 'memory']) {
      expect(legendItem(label).getAttribute('aria-pressed')).toBe('true');
    }
  });

  it('gives the task series the colors of their task statuses', async () => {
    const container = await renderWithHistory();

    expect(
      seriesLines(container).map(line => [
        line.getAttribute('name'),
        line.getAttribute('stroke'),
      ])
    ).toEqual([
      ['running', statusColorVar('RUNNING')],
      ['claimed', statusColorVar('CLAIMED')],
      ['CPU', 'var(--foreground)'],
      ['memory', 'var(--chart-2)'],
    ]);
  });

  it('draws one load series alone after a click on its legend item', async () => {
    const container = await renderWithHistory();

    fireEvent.click(legendItem('claimed'));

    expect(drawnSeries(container)).toEqual(['claimed', 'CPU', 'memory']);
    expect(legendItem('claimed').getAttribute('aria-pressed')).toBe('true');
    expect(legendItem('running').getAttribute('aria-pressed')).toBe('false');
  });

  it('draws the two load series again after a second click on the same item', async () => {
    const container = await renderWithHistory();

    fireEvent.click(legendItem('running'));
    expect(drawnSeries(container)).toEqual(['running', 'CPU', 'memory']);

    fireEvent.click(legendItem('running'));

    expect(drawnSeries(container)).toEqual([
      'running',
      'claimed',
      'CPU',
      'memory',
    ]);
    expect(legendItem('running').getAttribute('aria-pressed')).toBe('true');
    expect(legendItem('claimed').getAttribute('aria-pressed')).toBe('true');
  });

  it('toggles a series from the keyboard', async () => {
    await renderWithHistory();
    const running = legendItem('running');

    // A native button in the tab order: the browser clicks it on Enter and
    // on Space. jsdom does not, so the test sends that click itself.
    expect(running.tagName).toBe('BUTTON');
    expect(running.getAttribute('type')).toBe('button');
    expect(running.tabIndex).toBe(0);
    running.focus();

    fireEvent.click(running);

    expect(legendItem('claimed').getAttribute('aria-pressed')).toBe('false');
    // The same item keeps the focus, so the next key press reaches it.
    expect(document.activeElement).toBe(running);

    fireEvent.click(running);

    expect(legendItem('claimed').getAttribute('aria-pressed')).toBe('true');
    expect(document.activeElement).toBe(running);
  });

  it('keeps the legend of one chart apart from the other chart', async () => {
    const container = await renderWithHistory();

    fireEvent.click(legendItem('memory'));

    expect(drawnSeries(container)).toEqual(['running', 'claimed', 'memory']);
    expect(legendItem('CPU').getAttribute('aria-pressed')).toBe('false');
    expect(legendItem('running').getAttribute('aria-pressed')).toBe('true');
    expect(legendItem('claimed').getAttribute('aria-pressed')).toBe('true');
  });

  it('keeps the selection when the focus moves to another worker', async () => {
    stubFetch([point(3), point(1)]);
    const { container, rerender } = render(
      <WorkerHistoryChart workerId="worker-1" />,
      { wrapper }
    );
    await waitFor(() => expect(drawnSeries(container)).toHaveLength(4));
    fireEvent.click(legendItem('CPU'));

    rerender(<WorkerHistoryChart workerId="worker-2" />);

    await waitFor(() =>
      expect(drawnSeries(container)).toEqual(['running', 'claimed', 'CPU'])
    );
    expect(legendItem('memory').getAttribute('aria-pressed')).toBe('false');
  });

  it('fits the value axis of the process chart to the series that is drawn', async () => {
    stubFetch([
      point(1, { cpu_percent: 0.4, memory_percent: 12 }),
      point(1, { cpu_percent: 0.2, memory_percent: 12 }),
    ]);
    const { container } = render(<WorkerHistoryChart workerId="worker-1" />, {
      wrapper,
    });
    await waitFor(() => expect(drawnSeries(container)).toHaveLength(4));
    expect(Math.max(...percentTicks(container))).toBe(12);

    fireEvent.click(legendItem('CPU'));

    // With the memory line (12 %) gone, the axis ends at the CPU maximum
    // (0.4 %): not at 12 %, and not at a fixed 100 %.
    await waitFor(() =>
      expect(Math.max(...percentTicks(container))).toBeCloseTo(0.4)
    );
    expect(Math.min(...percentTicks(container))).toBe(0);
  });
});

describe('chartPoints', () => {
  it('puts the snapshots in time order, oldest first', () => {
    const points = chartPoints([
      point(3, { snapshot_at: '2026-08-12T10:00:20Z' }),
      point(2, { snapshot_at: '2026-08-12T10:00:10Z' }),
      point(1, { snapshot_at: '2026-08-12T10:00:00Z' }),
    ]);

    expect(points.map(entry => entry.running)).toEqual([1, 2, 3]);
    expect(points.map(entry => entry.snapshotAt)).toEqual([
      '2026-08-12T10:00:00Z',
      '2026-08-12T10:00:10Z',
      '2026-08-12T10:00:20Z',
    ]);
  });

  it('returns no point for no snapshot', () => {
    expect(chartPoints([])).toEqual([]);
  });
});

describe('seriesValueText', () => {
  const [snapshot] = chartPoints([
    point(5, {
      tasks_claimed: 2,
      cpu_percent: 0.27,
      memory_percent: 0.2,
      memory_usage_mb: 32.78,
    }),
  ]);
  const [unread] = chartPoints([
    point(0, { cpu_percent: null, memory_percent: null, memory_usage_mb: null }),
  ]);
  const [percentOnly] = chartPoints([
    point(0, { memory_percent: 1.5, memory_usage_mb: null }),
  ]);

  it('writes the task counts as they are', () => {
    expect(seriesValueText('running', snapshot!)).toBe('5');
    expect(seriesValueText('claimed', snapshot!)).toBe('2');
  });

  it('writes the CPU in percent, and the memory in percent and in MB', () => {
    expect(seriesValueText('cpu', snapshot!)).toBe('0.27%');
    expect(seriesValueText('memory', snapshot!)).toBe('0.2% · 32.8 MB');
  });

  it('writes the memory in percent alone when the size was not read', () => {
    expect(seriesValueText('memory', percentOnly!)).toBe('1.5%');
  });

  it('says that a value was not read, never 0', () => {
    expect(seriesValueText('cpu', unread!)).toBe('not read');
    expect(seriesValueText('memory', unread!)).toBe('not read');
  });
});
