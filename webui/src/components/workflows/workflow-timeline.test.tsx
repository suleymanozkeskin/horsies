import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { LONG_ENGINE_STEP_MS } from '@/components/workflows/phase';
import {
  drawnPhases,
  segmentDrawing,
  segmentLabel,
  TimelineView,
} from '@/components/workflows/workflow-timeline';
import { readClockOf } from '@/hooks/use-draw-time';
import { clockText } from '@/lib/clock-text';
import { instantMs } from '@/lib/time-text';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import {
  at,
  CASCADE_EDGES,
  CASCADE_NODES,
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  exactTiming,
  nth,
  OPEN_RUN_WITH_PAUSES,
  PARTIAL_OPEN_NODE,
  UNAVAILABLE_NODE,
  UNAVAILABLE_RUN,
  workflowNode,
  workflowRun,
} from '@/test/workflow-fixtures';
import type { RunTiming } from '@/types/timing';
import type { WorkflowNode, WorkflowRunDetail, WorkflowTaskDetail } from '@/types/workflows';

/** The browser receives the read at the database time of the read. */
const RECEIVED_AT_MS = instantMs('2026-11-22T12:01:40Z');

/** Four nodes, out of task-index order: a retried exact node, a partial node
 * with an open segment, a node with unavailable timing, an inconsistent node. */
const NODES: WorkflowNode[] = [
  workflowNode(3, 'broken', 'RUNNING', CLOCK_ORDER_NODE),
  workflowNode(1, 'load', 'RUNNING', PARTIAL_OPEN_NODE),
  workflowNode(0, 'extract', 'COMPLETED', EXACT_RETRIED_NODE),
  workflowNode(2, 'legacy', 'COMPLETED', UNAVAILABLE_NODE),
];

function renderTimeline(detail: WorkflowRunDetail): ReturnType<typeof render> {
  // The tooltip of a failed node reads the error code of its task.
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <TimelineView
        detail={detail}
        readClock={readClockOf(detail.timing, RECEIVED_AT_MS)}
        selectedIndex={null}
        onSelectNode={vi.fn()}
        statusFilter={new Set()}
      />
    </QueryClientProvider>
  );
}

/** The track of a row: the pointer on it shows the tooltip of the row. */
function trackOf(row: HTMLElement): HTMLElement {
  const track = row.querySelector<HTMLElement>('[data-row-track]');
  if (track === null) {
    throw new Error('the row has no track');
  }
  return track;
}

/** The part of the tooltip that names the mark under the cursor. */
function focusText(kind: string): string {
  const focus = screen.getByRole('tooltip').querySelector(`[data-focus="${kind}"]`);
  if (focus === null) {
    throw new Error(`the tooltip has no ${kind} focus`);
  }
  return focus.textContent ?? '';
}

/** A run with one node of real times: 3 s of task code, engine steps of
 * milliseconds, on an axis of 50 s. */
const NORMAL_NODE = exactTiming(
  [
    ['pending', 0, 5],
    ['enqueued', 5, 5.003],
    ['claimed', 5.003, 5.005],
    ['running', 5.005, 8.005],
    ['finalizing', 8.005, 8.006],
    ['advancing', 8.006, 8.007],
  ],
  'COMPLETED',
  8.007
);

const openRun = (): WorkflowRunDetail => workflowRun('RUNNING', NODES, OPEN_RUN_WITH_PAUSES);

function rowOf(nodeId: string): HTMLElement {
  const row = screen
    .getAllByRole('button')
    .find(button => button.getAttribute('aria-label')?.startsWith(`${nodeId},`));
  if (row === undefined) {
    throw new Error(`no timeline row for ${nodeId}`);
  }
  return row;
}

function segmentOf(row: HTMLElement, index: number): HTMLElement {
  const segment = row.querySelector<HTMLElement>(`[data-segment-index="${index}"]`);
  if (segment === null) {
    throw new Error(`no segment ${index} in the row`);
  }
  return segment;
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(RECEIVED_AT_MS);
  vi.stubGlobal('fetch', vi.fn());
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('TimelineView rows', () => {
  it('draws one row per node, in task-index order', () => {
    renderTimeline(openRun());

    const names = screen
      .getAllByRole('button')
      .map(row => row.getAttribute('aria-label')?.split(',')[0]);

    expect(names).toEqual(['extract', 'load', 'legacy', 'broken']);
  });

  it('stacks every phase segment of a retried node, attempt by attempt', () => {
    renderTimeline(openRun());

    const phases = [...rowOf('extract').querySelectorAll('[data-phase]')].map(segment =>
      segment.getAttribute('data-phase')
    );

    // Attempt 1: queue, earlier claims, the recorded claim, running,
    // finalizing; attempt 2: retry wait and a claim; then the node advances.
    expect(phases).toEqual([
      'pending',
      'ready',
      'enqueued',
      'claimed',
      'claimed',
      'running',
      'finalizing',
      'retry_wait',
      'claimed',
      'advancing',
    ]);
  });

  it('places each segment at its absolute time on the run axis', () => {
    renderTimeline(openRun());

    // The run axis is 100 s; the enqueued segment runs from 2 s to 3 s.
    const enqueued = segmentOf(rowOf('extract'), 2);

    expect(enqueued.style.left).toBe('2%');
    expect(enqueued.style.width).toBe('1%');
  });

  it('marks the terminal status as an endpoint, and a node that is not terminal has none', () => {
    renderTimeline(openRun());

    const endpoint = rowOf('extract').querySelector<HTMLElement>('[data-endpoint]');

    expect(endpoint?.getAttribute('data-endpoint')).toBe('COMPLETED');
    expect(endpoint?.style.left).toBe('12%');
    expect(rowOf('load').querySelector('[data-endpoint]')).toBeNull();
  });

  it('hatches a row whose timing is unavailable or inconsistent, with its label', () => {
    renderTimeline(openRun());

    const legacy = rowOf('legacy');
    const broken = rowOf('broken');

    expect(legacy.textContent).toContain('Timing unavailable');
    expect(legacy.querySelector('[data-phase]')).toBeNull();
    expect(broken.textContent).toContain('Timing inconsistent');
    expect(broken.querySelector('[data-phase]')).toBeNull();
  });

  it('says why a hatched row has no timing in the tooltip of the row', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(trackOf(rowOf('broken')));
    expect(screen.getByRole('tooltip').textContent).toContain('timing inconsistent: Clock order: claim');

    fireEvent.pointerMove(trackOf(rowOf('legacy')));
    expect(screen.getByRole('tooltip').textContent).toContain(
      'timing unavailable: The run began before the lifecycle record.'
    );
  });

  it('shows the time of each node without its dependency wait in the right column', () => {
    renderTimeline(openRun());

    // The retried node: 11 s of segments less 1 s of dependency wait. The
    // open node: from 0 s to the read.
    expect(rowOf('extract').textContent).toContain('10.0s');
    expect(rowOf('extract').textContent).not.toContain('12.0s');
    expect(rowOf('load').textContent).toContain('1m 40s');
    expect(rowOf('legacy').textContent).toContain('—');
    expect(screen.getByText('node time').getAttribute('aria-description')).toContain(
      'without its dependency wait'
    );
  });

  it('names each row with its timing summary', () => {
    renderTimeline(openRun());

    const name = rowOf('extract').getAttribute('aria-label') ?? '';

    expect(name).toContain('extract, completed, exact timing; dependency wait 1.0s');
    expect(name).toContain('slot wait 4.0s');
    expect(name).toContain('node time 10.0s');
    expect(name).toContain('completed at');
    expect(rowOf('broken').getAttribute('aria-label')).toContain('timing inconsistent');
  });
});

describe('TimelineView pauses and open segments', () => {
  it('draws each pause as a band over the rows; an open pause runs to the read', () => {
    const { container } = renderTimeline(openRun());

    const bands = [...container.querySelectorAll<HTMLElement>('[data-pause-band]')];

    expect(bands).toHaveLength(3);
    expect(nth(bands, 0).style.left).toBe('1%');
    expect(nth(bands, 0).style.width).toBe('1%');
    expect(nth(bands, 2).style.left).toBe('5%');
    expect(nth(bands, 2).style.width).toBe('95%');
  });

  it('grows an open segment on the one-second clock, with no refetch', () => {
    renderTimeline(openRun());
    const open = (): HTMLElement => segmentOf(rowOf('load'), 1);

    expect(open().getAttribute('data-end')).toBe('open');
    expect(open().style.width).toBe('95%');
    expect(rowOf('load').textContent).toContain('1m 40s');
    expect(rowOf('load').textContent).not.toContain('so far');

    act(() => {
      vi.advanceTimersByTime(2_000);
    });

    expect(rowOf('load').textContent).toContain('1m 42s');
    expect(Number.parseFloat(open().style.width)).toBeGreaterThan(95);
    expect(fetch).not.toHaveBeenCalled();
  });

  it('runs no clock for a run with no open segment', () => {
    const closed = workflowRun(
      'COMPLETED',
      [workflowNode(0, 'extract', 'COMPLETED', EXACT_RETRIED_NODE)],
      nth(TIMING_EXAMPLES.run_timings, 2)
    );
    renderTimeline(closed);

    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('TimelineView run coverage', () => {
  it('hatches every row of a run whose timing is unavailable and says why', () => {
    renderTimeline(workflowRun('COMPLETED', NODES, UNAVAILABLE_RUN));

    expect(screen.getByText(/The run has more than 100,000 events\./)).not.toBeNull();
    for (const row of screen.getAllByRole('button')) {
      expect(row.textContent).toContain('Timing unavailable');
      expect(row.querySelector('[data-phase]')).toBeNull();
      expect(row.getAttribute('aria-label')).toContain('timing unavailable');
    }
  });

  it('names the reasons of a partial run and the conflicts of an inconsistent run', () => {
    const partial: RunTiming = nth(TIMING_EXAMPLES.run_timings, 2);
    const inconsistent: RunTiming = nth(TIMING_EXAMPLES.run_timings, 3);

    renderTimeline(workflowRun('COMPLETED', NODES, partial));
    expect(screen.getByText('Partial timing: claims not counted.')).not.toBeNull();
    cleanup();

    renderTimeline(workflowRun('COMPLETED', NODES, inconsistent));
    expect(
      screen.getByText(
        'Inconsistent timing: node #2; pause record: Unexpected event WORKFLOW_RESUMED (#4).'
      )
    ).not.toBeNull();
  });

  it('shows no coverage banner for an exact run', () => {
    renderTimeline(openRun());

    expect(screen.queryByText(/Partial timing|Inconsistent timing|Timing unavailable\./)).toBeNull();
  });
});

describe('TimelineView tooltip', () => {
  it('names the segment under the pointer: its phase, time, time span and source', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(segmentOf(rowOf('extract'), 3));
    const tooltip = screen.getByRole('tooltip');

    expect(focusText('segment')).toContain('slot wait · 1 earlier claim');
    expect(focusText('segment')).toContain('task 1 · attempt 1');
    expect(focusText('segment')).toContain('1.0s');
    expect(focusText('segment')).toContain(
      `${clockText(instantMs(at(3)))} → ${clockText(instantMs(at(4)))}`
    );
    expect(rowOf('extract').getAttribute('aria-describedby')).toBe(tooltip.id);

    fireEvent.pointerLeave(trackOf(rowOf('extract')));
    expect(screen.queryByRole('tooltip')).toBeNull();
  });

  it('says what a long engine step tells, on its segment', () => {
    renderTimeline(openRun());

    // The queue wait of the example is 1 s: at or above the long step limit.
    fireEvent.pointerMove(segmentOf(rowOf('extract'), 2));

    expect(focusText('segment')).toContain('queue wait');
    expect(focusText('segment')).toContain('the queue is at its concurrency limit');
  });

  it('gives each phase of the node as a number, also the phases too short to draw', () => {
    renderTimeline(workflowRun('COMPLETED', [workflowNode(0, 'pick', 'COMPLETED', NORMAL_NODE)], nth(TIMING_EXAMPLES.run_timings, 2)));

    fireEvent.pointerMove(trackOf(rowOf('pick')));
    const figures = within(screen.getByRole('tooltip')).getByRole('list', { name: 'Time of pick by phase' });

    expect([...figures.querySelectorAll('li')].map(item => item.textContent)).toEqual([
      'dependency wait5.0s',
      'queue wait3ms',
      'slot wait2ms',
      'running3.0s',
      'finalizing1ms',
      'advancing1ms',
    ]);
    expect(screen.getByRole('tooltip').textContent).toContain('node time 3.0s');
    // The pointer is on the track, not on a mark: no focus line.
    expect(screen.getByRole('tooltip').querySelector('[data-focus]')).toBeNull();
  });

  it('says what the end marker is: the status and the time of the end of the node', () => {
    renderTimeline(openRun());
    const marker = rowOf('extract').querySelector<HTMLElement>('[data-endpoint]');
    if (marker === null) {
      throw new Error('the completed row has its end marker');
    }

    fireEvent.pointerMove(marker);

    expect(focusText('endpoint')).toContain(`End of the node: completed at ${clockText(instantMs(at(12)))}`);
    expect(focusText('endpoint')).toContain('a point in time');
    expect(marker.getAttribute('title')).toBeNull();
  });

  it('adds the error code of the task to the end of a failed node', async () => {
    vi.useRealTimers();
    const failed: WorkflowTaskDetail = {
      task_index: 1,
      node_id: 'b',
      task_name: 'tasks.b',
      node_status: 'FAILED',
      is_subworkflow: false,
      node_error: null,
      leaf: {
        task_id: 't',
        status: 'FAILED',
        error_code: 'PSP_UNAVAILABLE',
        failed_reason: null,
        retry_count: 0,
        max_retries: 0,
        enqueued_at: null,
        started_at: null,
        completed_at: null,
        failed_at: null,
        queue_s: null,
        exec_s: null,
        worker_hostname: null,
        good_until: null,
        trace_id: null,
      },
      attempts: [],
    };
    const fetchStub = vi.fn((_input: RequestInfo | URL) =>
      Promise.resolve(
        new Response(JSON.stringify(failed), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      )
    );
    vi.stubGlobal('fetch', fetchStub);
    renderTimeline({
      ...workflowRun('FAILED', CASCADE_NODES, nth(TIMING_EXAMPLES.run_timings, 2)),
      edges: CASCADE_EDGES,
    });
    const marker = rowOf('b').querySelector<HTMLElement>('[data-endpoint="FAILED"]');
    if (marker === null) {
      throw new Error('the failed row has its end marker');
    }

    fireEvent.pointerMove(marker);

    expect(await screen.findByText(/failed at .*, error code PSP_UNAVAILABLE/)).not.toBeNull();
    expect(String(nth(fetchStub.mock.calls, 0)[0])).toContain('/workflows/run-1/tasks/1');
  });

  it('reads no error code for a node that did not fail', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(trackOf(rowOf('extract')));

    expect(fetch).not.toHaveBeenCalled();
  });

  it('says what the line under a row is: the critical path', () => {
    const run = {
      ...workflowRun('COMPLETED', CASCADE_NODES, nth(TIMING_EXAMPLES.run_timings, 2)),
      edges: CASCADE_EDGES,
    };
    renderTimeline(run);
    const line = rowOf('a').querySelector<HTMLElement>('[data-critical-line]');
    if (line === null) {
      throw new Error('the row of the critical path has its line');
    }

    fireEvent.pointerMove(line);

    expect(focusText('critical_line')).toContain('On the critical path');
    expect(focusText('critical_line')).toContain('from the end of its dependency wait to its end');
  });

  it('says that the run was paused at the time under the pointer', () => {
    renderTimeline(openRun());
    const track = trackOf(rowOf('extract'));
    // A track of 1,000 px from x = 200: the pointer at 50 % is at 50 s, in
    // the open pause that began at 5 s.
    track.getBoundingClientRect = () =>
      ({ left: 200, top: 0, width: 1_000, height: 16, right: 1_200, bottom: 16, x: 200, y: 0 }) as DOMRect;

    fireEvent.pointerMove(track, { clientX: 700, clientY: 8 });
    expect(focusText('pause')).toContain(`The run was paused here: ${clockText(instantMs(at(5)))} → open`);

    // At 0.5 % (0.5 s) no pause holds the pointer.
    fireEvent.pointerMove(track, { clientX: 205, clientY: 8 });
    expect(screen.getByRole('tooltip').querySelector('[data-focus="pause"]')).toBeNull();
  });

  it('steps through the segments of a row with the arrow keys and moves between rows', () => {
    renderTimeline(openRun());
    const extract = rowOf('extract');
    extract.focus();

    fireEvent.keyDown(extract, { key: 'ArrowRight' });
    expect(focusText('segment')).toContain('dependency wait');
    fireEvent.keyDown(extract, { key: 'ArrowRight' });
    expect(focusText('segment')).toContain('ready');
    fireEvent.keyDown(extract, { key: 'ArrowLeft' });
    expect(focusText('segment')).toContain('dependency wait');
    fireEvent.keyDown(extract, { key: 'Escape' });
    expect(screen.queryByRole('tooltip')).toBeNull();

    fireEvent.keyDown(extract, { key: 'ArrowDown' });
    expect(document.activeElement).toBe(rowOf('load'));
  });

  it('shows an open segment as open, with its duration so far', () => {
    renderTimeline(openRun());

    fireEvent.pointerMove(segmentOf(rowOf('load'), 1));

    expect(focusText('segment')).toContain('unknown · worker lost');
    expect(focusText('segment')).toContain('open');
    expect(focusText('segment')).toContain('1m 35s so far');
  });
});

describe('TimelineView emphasis', () => {
  it('draws dependency wait as a thin track and the other phases at full height', () => {
    renderTimeline(openRun());

    const pending = segmentOf(rowOf('extract'), 0);
    const enqueued = segmentOf(rowOf('extract'), 2);

    expect(pending.getAttribute('data-phase')).toBe('pending');
    expect(pending.firstElementChild?.className).toContain('h-1');
    expect(enqueued.firstElementChild?.className).toContain('inset-0');
  });

  it('marks the nodes of the critical path with one mark: a line under the active span', () => {
    const run = {
      ...workflowRun('FAILED', CASCADE_NODES, nth(TIMING_EXAMPLES.run_timings, 2)),
      edges: CASCADE_EDGES,
    };
    renderTimeline(run);

    const onPath = ['a', 'b', 'e'].map(rowOf);
    const offPath = ['c', 'd'].map(rowOf);

    for (const row of onPath) {
      expect(row.getAttribute('aria-label')).toContain('on the critical path');
      expect(row.querySelector('[data-critical-line]')).not.toBeNull();
    }
    for (const row of offPath) {
      expect(row.getAttribute('aria-label')).not.toContain('on the critical path');
      expect(row.querySelector('[data-critical-line]')).toBeNull();
    }
    // e waits 5 s for its dependencies: its line starts at 5 s of the 50 s axis.
    expect(rowOf('e').querySelector<HTMLElement>('[data-critical-line]')?.style.left).toBe('10%');
    // The name of a row has no second mark, and the same weight on and off the path.
    expect(rowOf('a').firstElementChild?.className).toBe(rowOf('c').firstElementChild?.className);
  });
});

describe('TimelineView key', () => {
  it('lists the fills that the timeline draws and its marks, with no time', () => {
    const run = {
      ...workflowRun('FAILED', CASCADE_NODES, nth(TIMING_EXAMPLES.run_timings, 2)),
      edges: CASCADE_EDGES,
    };
    renderTimeline(run);

    const legend = screen.getByRole('list', { name: 'Legend' });
    expect([...legend.querySelectorAll('li')].map(item => item.textContent)).toEqual([
      'dependency wait',
      'queue wait',
      'running',
      'advancing',
      'end of a node',
      'critical path',
    ]);
  });

  it('adds the pause band to the key of a paused run', () => {
    renderTimeline(openRun());

    expect(document.querySelector('[data-legend-mark="paused"]')).not.toBeNull();
  });

  it('has no key for a run with no timing', () => {
    renderTimeline(workflowRun('COMPLETED', NODES, UNAVAILABLE_RUN));

    expect(screen.queryByRole('list', { name: 'Legend' })).toBeNull();
  });
});

describe('segmentDrawing', () => {
  const wide = { kind: 'measured', px: 1_000 } as const;

  it('draws a segment of one pixel or more at its own width', () => {
    // 3 s of 50 s on 1,000 px: 60 px.
    expect(segmentDrawing('running', 3_000, 6, wide)).toEqual({ kind: 'drawn', minWidthPx: 0 });
    expect(segmentDrawing('pending', 50, 0.1, wide)).toEqual({ kind: 'drawn', minWidthPx: 0 });
  });

  it('does not draw a segment below one pixel: a normal engine step has no forced sliver', () => {
    // 3 ms of 50 s on 1,000 px: 0.06 px.
    expect(segmentDrawing('enqueued', 3, 0.006, wide)).toEqual({ kind: 'not_drawn' });
    expect(segmentDrawing('finalizing', 1, 0.002, wide)).toEqual({ kind: 'not_drawn' });
    expect(segmentDrawing('running', 20, 0.04, wide)).toEqual({ kind: 'not_drawn' });
  });

  it('keeps a marked segment visible on a long axis', () => {
    // A slot wait of 800 ms on an axis of 10 minutes: 1.3 px.
    expect(segmentDrawing('claimed', 800, 0.13, wide)).toEqual({ kind: 'drawn', minWidthPx: 3 });
    expect(segmentDrawing('enqueued', LONG_ENGINE_STEP_MS, 0.001, wide)).toEqual({
      kind: 'drawn',
      minWidthPx: 3,
    });
  });

  it('draws every segment before the track has a width', () => {
    expect(segmentDrawing('enqueued', 3, 0.006, { kind: 'unmeasured' })).toEqual({
      kind: 'drawn',
      minWidthPx: 0,
    });
  });
});

describe('drawnPhases', () => {
  const axis = { startMs: instantMs(at(0)), spanMs: 50_000 };
  const nodes = [workflowNode(0, 'pick', 'COMPLETED', NORMAL_NODE)];

  it('lists only the phases wide enough to draw, in lifecycle order', () => {
    expect(drawnPhases(nodes, axis, RECEIVED_AT_MS, { kind: 'measured', px: 1_000 })).toEqual([
      'pending',
      'running',
    ]);
  });

  it('lists every phase before the track has a width', () => {
    expect(drawnPhases(nodes, axis, RECEIVED_AT_MS, { kind: 'unmeasured' })).toEqual([
      'pending',
      'enqueued',
      'claimed',
      'running',
      'finalizing',
      'advancing',
    ]);
  });
});

describe('segmentLabel', () => {
  const wide = { kind: 'measured', px: 1_000 } as const;

  it('labels a full-height segment with its duration when the text fits', () => {
    expect(segmentLabel('running', 4_500, 10, wide)).toEqual({ kind: 'label', text: '4.5s' });
  });

  it('leaves a narrow segment, dependency wait and an unmeasured track without a label', () => {
    expect(segmentLabel('running', 4_500, 1, wide)).toEqual({ kind: 'no_label' });
    expect(segmentLabel('pending', 60_000, 90, wide)).toEqual({ kind: 'no_label' });
    expect(segmentLabel('running', 4_500, 50, { kind: 'unmeasured' })).toEqual({ kind: 'no_label' });
  });
});
