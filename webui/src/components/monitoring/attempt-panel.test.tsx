import { cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { AttemptPanel } from '@/components/monitoring/attempt-panel';
import { clockSecondText, clockText } from '@/lib/clock-text';
import { instantMs } from '@/lib/time-text';
import { at, leafTask, normalAttempt, timedAttempt, NORMAL_TIMES } from '@/test/attempt-fixtures';
import type { TaskDetail } from '@/types/tasks';
import type { LeafTask, TaskAttempt } from '@/types/workflows';

const ms = (seconds: number): number => instantMs(at(seconds));
const GROUPING_KEY = 'attempt-bars-grouping';
const MESSAGE = 'Unhandled exception in task sync_supplier_feed: TimeoutError: brera-knitwear did not answer';

function task(attempts: TaskAttempt[], leaf: Partial<LeafTask> = {}): TaskDetail {
  return {
    leaf: { ...leafTask('FAILED', attempts.length - 1, 3, { enqueuedAt: null, startedAt: null }), ...leaf },
    task_name: 'sync_supplier_feed',
    queue_name: 'analytics',
    priority: 90,
    is_workflow_task: false,
    error_category: 'OPERATIONAL',
    attempts,
    workflow_id: null,
    workflow_task_index: null,
  };
}

/** Four failed attempts with one message, as the retried task of the demo. */
function retried(): TaskDetail {
  const attempts = [0, 10, 30, 60].map((start, index) => ({
    ...normalAttempt(index + 1, start, 'FAILED'),
    error_code: 'SUPPLIER_TIMEOUT',
    error_message: MESSAGE,
  }));
  return task(attempts, { error_code: 'SUPPLIER_TIMEOUT' });
}

function single(): TaskDetail {
  return task([normalAttempt(1, 0, 'COMPLETED')], { status: 'COMPLETED', retry_count: 0, max_retries: 2 });
}

const renderPanel = (detail: TaskDetail, traceTemplate: string | null = null): ReturnType<typeof render> =>
  render(<AttemptPanel detail={detail} traceTemplate={traceTemplate} />);

const tooltip = (): HTMLElement => screen.getByRole('tooltip');

function pointAt(element: Element): void {
  fireEvent.pointerMove(element, { clientX: 10, clientY: 10 });
}

/** The storage of the browser, in memory: the global storage of Node has no
 * file under the test runner. */
class MemoryStorage {
  private readonly items = new Map<string, string>();

  get length(): number {
    return this.items.size;
  }

  clear(): void {
    this.items.clear();
  }

  getItem(key: string): string | null {
    return this.items.get(key) ?? null;
  }

  key(index: number): string | null {
    return [...this.items.keys()][index] ?? null;
  }

  removeItem(key: string): void {
    this.items.delete(key);
  }

  setItem(key: string, value: string): void {
    this.items.set(key, value);
  }
}

beforeEach(() => {
  Object.defineProperty(window, 'localStorage', { configurable: true, value: new MemoryStorage() });
});

afterEach(() => {
  cleanup();
});

describe('AttemptPanel title and meta', () => {
  it('names the task with its status and error code, and puts queue, priority and attempt on one quiet line', () => {
    renderPanel(retried());

    const header = screen.getByRole('heading', { name: 'sync_supplier_feed' }).closest('header');
    expect(header?.textContent).toContain('failed');
    expect(header?.textContent).toContain('SUPPLIER_TIMEOUT');
    expect(document.querySelector('[data-task-meta]')?.textContent).toBe('queueanalyticspriority90attempt4 of 4');
    // No label blocks in capitals.
    expect(document.body.innerHTML).not.toContain('uppercase');
  });

  it('renders the actions and the control at the end of the title line that the view gives', () => {
    render(
      <AttemptPanel
        detail={retried()}
        traceTemplate={null}
        titleEnd={<button type="button">Close detail</button>}
        actions={<p>Managed by workflow 8c2d52fd.</p>}
      />
    );

    expect(screen.getByRole('button', { name: 'Close detail' }).closest('header')).not.toBeNull();
    expect(screen.getByText('Managed by workflow 8c2d52fd.')).not.toBeNull();
  });
});

describe('AttemptPanel errors', () => {
  it('shows identical errors once, labeled with the attempt range, with no second code chip', () => {
    renderPanel(retried());

    const errors = screen.getByRole('region', { name: 'Errors' });
    expect(within(errors).getAllByText(/brera-knitwear/)).toHaveLength(1);
    expect(errors.querySelector('[data-error-attempts]')?.textContent).toBe('attempt 1–4');
    // The code is in the title, so the group does not repeat it.
    expect(within(errors).queryByText('SUPPLIER_TIMEOUT')).toBeNull();
  });

  it('mutes the prefix of an unhandled exception: the cause follows it', () => {
    renderPanel(retried());

    const message = document.querySelector('[data-error-message]');
    expect(message?.firstElementChild?.textContent).toBe('Unhandled exception in task sync_supplier_feed: ');
    expect(message?.firstElementChild?.className).toContain('text-muted-foreground');
  });

  it('clamps a long message to 3 lines, with more and less', () => {
    const heights = { scrollHeight: 120, clientHeight: 54 };
    const scroll = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'scrollHeight');
    const client = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'clientHeight');
    Object.defineProperty(HTMLElement.prototype, 'scrollHeight', { configurable: true, get: () => heights.scrollHeight });
    Object.defineProperty(HTMLElement.prototype, 'clientHeight', { configurable: true, get: () => heights.clientHeight });
    try {
      renderPanel(retried());

      const message = document.querySelector('[data-error-message]');
      expect(message?.className).toContain('line-clamp-3');
      fireEvent.click(screen.getByRole('button', { name: 'more' }));
      expect(message?.className).not.toContain('line-clamp-3');
      expect(screen.getByRole('button', { name: 'less' }).getAttribute('aria-expanded')).toBe('true');
    } finally {
      Object.defineProperty(HTMLElement.prototype, 'scrollHeight', scroll ?? { configurable: true, value: 0 });
      Object.defineProperty(HTMLElement.prototype, 'clientHeight', client ?? { configurable: true, value: 0 });
    }
  });

  it('has no more button for a message that fits', () => {
    renderPanel(retried());

    expect(screen.queryByRole('button', { name: 'more' })).toBeNull();
  });

  it('shows the failure of the task that no attempt has, labeled task', () => {
    renderPanel(task([normalAttempt(1, 0, 'FAILED')], { failed_reason: 'expired before its next attempt' }));

    const errors = screen.getByRole('region', { name: 'Errors' });
    expect(errors.textContent).toContain('task');
    expect(errors.textContent).toContain('expired before its next attempt');
  });
});

describe('AttemptPanel timing', () => {
  it('gives the duration with its start → end clock times', () => {
    renderPanel(retried());

    const headline = document.querySelector('[data-headline]');
    expect(headline?.querySelector('[data-total]')?.textContent).toBe('1m 3.0s');
    expect(headline?.textContent).toContain(`${clockSecondText(ms(0))} → ${clockSecondText(ms(63.006))}`);
  });

  it('draws the strip in time order, with each attempt number and its outcome circle', () => {
    renderPanel(task([normalAttempt(1, 0, 'FAILED'), normalAttempt(2, 10, 'COMPLETED')], { status: 'COMPLETED' }));

    const tags = [...document.querySelectorAll('[data-attempt-tag]')];
    expect(tags.map(tag => tag.textContent)).toEqual(['1', '2']);
    expect(tags.map(tag => tag.querySelector('[data-outcome]')?.getAttribute('data-outcome'))).toEqual([
      'failed',
      'completed',
    ]);
    // The running parts and the retry wait are wide enough to draw; the
    // engine steps of milliseconds are not, and the bars show them.
    expect([...document.querySelectorAll('[data-segment]')].map(segment => segment.getAttribute('data-segment'))).toEqual([
      'running',
      'retry_wait',
      'running',
    ]);
  });

  it('names every fill and mark in the key, with the totals over the attempts', () => {
    renderPanel(retried());

    const key = screen.getByRole('list', { name: 'Key' });
    expect([...key.querySelectorAll('[data-key-phase]')].map(item => item.textContent)).toEqual([
      'queue wait12.0ms',
      'slot wait8.00ms',
      'running12.0s',
      'finalizing4.00ms',
      'retry wait51.0s',
    ]);
    expect([...key.querySelectorAll('[data-key-mark]')].map(item => item.textContent)).toEqual([
      'failed',
      'wait',
      'work',
    ]);
  });

  it('shows what a mark is, its duration and its start → end on hover', () => {
    renderPanel(retried());

    pointAt(document.querySelector('[data-segment="retry_wait"]') as Element);

    expect(tooltip().textContent).toContain('retry wait');
    expect(tooltip().textContent).toContain('attempt 1 → 2');
    expect(tooltip().textContent).toContain('6.99s');
    expect(tooltip().textContent).toContain(`${clockText(ms(3.006))} → ${clockText(ms(10))}`);
    expect(tooltip().textContent).toContain('record of a failed attempt → due time of the next');
  });

  it('writes every duration at the bars with one text weight', () => {
    renderPanel(retried());

    const values = [...document.querySelectorAll('[data-bar-value]')];
    expect(values.length).toBeGreaterThan(0);
    expect(new Set(values.map(value => value.className)).size).toBe(1);
    expect(values[0]?.className).not.toMatch(/font-(semibold|bold)/);
  });

  it('marks no long step: no warning, no note and no sentence about a long time', () => {
    renderPanel(task([timedAttempt(1, 0, { ...NORMAL_TIMES, queueMs: 7_200 }, 'COMPLETED')], { status: 'COMPLETED' }));

    expect(screen.queryAllByRole('note')).toHaveLength(0);
    expect(document.body.textContent).not.toMatch(/long step|concurrency limit|usual cause/);
    expect(document.body.innerHTML).not.toContain('marked');
  });
});

describe('AttemptPanel grouping of the bars', () => {
  const groups = (): string[] =>
    [...document.querySelectorAll('[data-bar-group]')].map(group => group.getAttribute('data-bar-group') ?? '');

  it('groups the bars by phase, one row per attempt', () => {
    renderPanel(retried());

    expect(groups()).toEqual(['enqueued', 'claimed', 'running', 'finalizing', 'retry_wait']);
    const running = document.querySelector('[data-bar-group="running"]');
    expect([...(running?.querySelectorAll('[data-bar-owner]') ?? [])].map(row => row.getAttribute('data-bar-owner'))).toEqual([
      '1',
      '2',
      '3',
      '4',
    ]);
  });

  it('groups the bars by attempt, with the retry wait between two attempts', () => {
    renderPanel(retried());

    fireEvent.click(screen.getByRole('button', { name: 'by attempt' }));

    expect(groups()).toEqual(['attempt 1', '1→2', 'attempt 2', '2→3', 'attempt 3', '3→4', 'attempt 4']);
    expect(screen.getByRole('button', { name: 'by attempt' }).getAttribute('aria-pressed')).toBe('true');
  });

  it('keeps the choice for the next task', () => {
    const first = renderPanel(retried());
    fireEvent.click(screen.getByRole('button', { name: 'by attempt' }));
    expect(window.localStorage.getItem(GROUPING_KEY)).toBe('"attempt"');
    first.unmount();

    renderPanel(task([normalAttempt(1, 0, 'FAILED'), normalAttempt(2, 10, 'FAILED')]));

    expect(groups()[0]).toBe('attempt 1');
    expect(screen.getByRole('button', { name: 'by attempt' }).getAttribute('aria-pressed')).toBe('true');
  });

  it('reads a stored value that is not a grouping as by phase', () => {
    window.localStorage.setItem(GROUPING_KEY, '"sideways"');

    renderPanel(retried());

    expect(groups()[0]).toBe('enqueued');
  });

  it('greys the switch out for one attempt, keeps it in its place, and says why', () => {
    renderPanel(single());

    const group = screen.getByRole('group', { name: 'Grouping of the bars' });
    const byAttempt = within(group).getByRole('button', { name: 'by attempt' });
    expect(byAttempt.getAttribute('aria-disabled')).toBe('true');
    expect(byAttempt.hasAttribute('disabled')).toBe(false);

    fireEvent.click(byAttempt);
    expect(byAttempt.getAttribute('aria-pressed')).toBe('false');
    expect(window.localStorage.getItem(GROUPING_KEY)).toBeNull();

    pointAt(group);
    expect(tooltip().textContent).toBe('one attemptboth groupings show the same bars');
  });
});

describe('AttemptPanel details', () => {
  it('opens the exact times, the worker host, the task id, the trace link and the date', () => {
    const detail = task([{ ...normalAttempt(1, 0, 'COMPLETED'), worker_hostname: 'worker-17.internal' }], {
      status: 'COMPLETED',
      trace_id: '0af7651916cd43dd8448eb211c80319c',
      task_id: 'task-42',
    });
    renderPanel(detail, 'https://grafana.example.com/explore?traceId={trace_id}');

    const details = screen.getByRole('region', { name: 'Details' });
    const toggle = within(details).getByRole('button', { name: 'details' });
    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    expect(details.querySelector('[hidden]')).not.toBeNull();

    fireEvent.click(toggle);

    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(details.textContent).toContain(`${clockText(ms(0.005))} → ${clockText(ms(3.005))}`);
    expect(details.textContent).toContain('worker-17.internal');
    expect(details.textContent).toContain('task-42');
    expect(within(details).getByRole('link').getAttribute('href')).toBe(
      'https://grafana.example.com/explore?traceId=0af7651916cd43dd8448eb211c80319c'
    );
  });

  it('shows the trace id as text with no template, and no trace row for a task with no trace', () => {
    const { unmount } = renderPanel(task([normalAttempt(1, 0, 'COMPLETED')], { trace_id: 'abc' }));
    fireEvent.click(screen.getByRole('button', { name: 'details' }));
    expect(screen.queryByRole('link')).toBeNull();
    expect(screen.getByRole('region', { name: 'Details' }).textContent).toContain('traceabc');
    unmount();

    renderPanel(task([normalAttempt(1, 0, 'COMPLETED')]));
    fireEvent.click(screen.getByRole('button', { name: 'details' }));
    expect(screen.getByRole('region', { name: 'Details' }).textContent).not.toContain('trace');
  });
});

describe('AttemptPanel live attempt', () => {
  it('ends with the attempt in progress of a running task, on a one-second clock', () => {
    renderPanel(
      task([normalAttempt(1, 0, 'FAILED')], { status: 'RUNNING', retry_count: 1, enqueued_at: at(10), started_at: at(12) })
    );

    const live = document.querySelector('[data-live-attempt]');
    expect(live?.textContent).toMatch(new RegExp(`^attempt 2 · running · since ${clockText(ms(12)).replace('.', '\\.')}`));
    expect(document.querySelector('[data-task-meta]')?.textContent).toContain('attempt2 of 4');
  });

  it('says that no attempt is recorded for a task with none', () => {
    renderPanel(task([], { status: 'CANCELLED', retry_count: 0 }));

    expect(screen.getByText('no attempt recorded')).not.toBeNull();
  });
});
