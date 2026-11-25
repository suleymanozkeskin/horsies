import { render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { FloatingTooltip } from '@/components/workflows/mark-tip';
import {
  nodeEndText,
  nodeEndWithCodeText,
  NodeTooltipBody,
  type NodeFocus,
} from '@/components/workflows/node-tooltip';
import type { NodeErrorCode } from '@/hooks/use-workflow-task';
import { clockText } from '@/lib/clock-text';
import { instantMs } from '@/lib/time-text';
import {
  at,
  CASCADE_NODES,
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  nth,
  workflowNode,
} from '@/test/workflow-fixtures';
import type { WorkflowNode } from '@/types/workflows';

const READ_MS = instantMs('2026-11-22T12:01:40Z');
const COMPLETED_AT = { kind: 'terminal', status: 'COMPLETED', at: at(12) } as const;

function renderBody(
  node: WorkflowNode,
  focus: NodeFocus,
  options: { onChain?: boolean; facts?: string[]; failure?: NodeErrorCode } = {}
): ReturnType<typeof render> {
  return render(
    <NodeTooltipBody
      node={node}
      drawAtMs={READ_MS}
      membership={options.onChain === true ? 'on_chain' : 'off_chain'}
      focus={focus}
      pause={{ kind: 'not_in_pause' }}
      facts={options.facts ?? []}
      failure={options.failure ?? { kind: 'none' }}
    />
  );
}

const RETRIED = workflowNode(0, 'extract', 'COMPLETED', EXACT_RETRIED_NODE);

afterEach(() => {
  window.innerWidth = 1024;
  window.innerHeight = 768;
});

describe('nodeEndText', () => {
  it('writes the end of a node as its status and its time of day', () => {
    expect(nodeEndText(COMPLETED_AT)).toBe(`completed at ${clockText(instantMs(at(12)))}`);
    expect(nodeEndText({ kind: 'not_terminal' })).toBe('not ended');
  });

  it('adds the error code of the task when the node has one', () => {
    const failedAt = { kind: 'terminal', status: 'FAILED', at: at(5) } as const;

    expect(nodeEndWithCodeText(failedAt, { kind: 'code', code: 'PSP_UNAVAILABLE' })).toBe(
      `failed at ${clockText(instantMs(at(5)))}, error code PSP_UNAVAILABLE`
    );
    expect(nodeEndWithCodeText(failedAt, { kind: 'none' })).toBe(nodeEndText(failedAt));
    expect(nodeEndWithCodeText(failedAt, { kind: 'not_loaded' })).toBe(nodeEndText(failedAt));
  });
});

describe('NodeTooltipBody on the node as a whole', () => {
  it('names the node with its sign, its status and its time without dependency wait', () => {
    const { container } = renderBody(RETRIED, { kind: 'node' });

    expect(container.querySelector('[data-outcome="completed"]')).not.toBeNull();
    expect(container.textContent).toContain('extract');
    expect(container.textContent).toContain('completed');
    expect(container.textContent).toContain('10.0s');
  });

  it('gives the time of each kind of the node, its count and its end', () => {
    const { container } = renderBody(RETRIED, { kind: 'node' });

    expect(container.textContent).toContain('earlier claims1.00s');
    expect(container.textContent).toContain('retry wait1.00s');
    expect(container.textContent).toContain('2 attempts');
    expect(container.textContent).toContain(`end: completed at ${clockText(instantMs(at(12)))}`);
    expect(container.querySelector('[data-focus]')).toBeNull();
  });

  it('says that a node is on the longest chain, and lists the facts of the view', () => {
    const { container } = renderBody(RETRIED, { kind: 'node' }, {
      onChain: true,
      facts: ['Tolerant join (dashed border): the node runs even when a dependency failed.'],
    });

    expect(container.textContent).toContain('2 attempts · on the longest chain');
    expect(container.textContent).toContain('Tolerant join (dashed border)');
    expect(container.textContent).not.toMatch(/critical/i);
  });

  it('gives the coverage text of a node with no segments, and a dash as its time', () => {
    const { container } = renderBody(workflowNode(3, 'broken', 'RUNNING', CLOCK_ORDER_NODE), { kind: 'node' });

    expect(container.textContent).toContain('timing inconsistent: Clock order: claim');
    expect(container.textContent).toContain('—');
  });

  it('shows a dash as the time of a node that did not run', () => {
    const { container } = renderBody(nth(CASCADE_NODES, 3), { kind: 'node' });

    expect(container.textContent).toContain('—');
    expect(container.textContent).toContain('skipped at');
  });
});

describe('NodeTooltipBody on a mark', () => {
  it('names a segment: its kind, its node and source, its time, its start → end and what it is', () => {
    const { container } = renderBody(RETRIED, { kind: 'segment', segmentIndex: 3 });
    const focus = container.querySelector('[data-focus="segment"]');

    expect(focus?.textContent).toContain('earlier claims');
    expect(focus?.textContent).toContain('extract · task 1 · attempt 1');
    expect(focus?.textContent).toContain('1.00s');
    expect(focus?.textContent).toContain(`${clockText(instantMs(at(3)))} → ${clockText(instantMs(at(4)))}`);
    expect(focus?.textContent).toContain('first claim → the claim that ran the attempt');
    expect(focus?.textContent).toContain('1 earlier claim');
    // The mark alone: no summary of the node.
    expect(container.textContent).not.toContain('2 attempts');
  });

  it('names the end marker: end of the node, its status, the time with milliseconds, the error code', () => {
    const failed = { ...RETRIED, node_status: 'FAILED' };
    const timing = { ...EXACT_RETRIED_NODE, endpoint: { kind: 'terminal', status: 'FAILED', at: at(12) } } as const;
    const { container } = renderBody({ ...failed, timing }, { kind: 'endpoint' }, {
      failure: { kind: 'code', code: 'TASK_TIMEOUT' },
    });
    const focus = container.querySelector('[data-focus="endpoint"]');

    expect(focus?.textContent).toContain('end of extract');
    expect(focus?.textContent).toContain('failed');
    expect(focus?.textContent).toContain(clockText(instantMs(at(12))));
    expect(focus?.textContent).toMatch(/\.\d{3}/);
    expect(focus?.textContent).toContain('error code TASK_TIMEOUT');
  });

  it('names a bar of the log cell: its kind, its node, its total, its count and its longest interval', () => {
    const { container } = renderBody(RETRIED, { kind: 'lane', phase: 'claimed' });
    const focus = container.querySelector('[data-focus="lane"]');

    expect(focus?.textContent).toContain('slot wait');
    expect(focus?.textContent).toContain('3.00s');
    expect(focus?.textContent).toContain('2 intervals · longest 2.00s');
  });

  it('shows nothing for a segment that a refetch removed', () => {
    const { container } = renderBody(RETRIED, { kind: 'segment', segmentIndex: 99 });

    expect(container.textContent).toBe('');
  });
});

describe('FloatingTooltip', () => {
  it('is a child of the document body, so a blurred panel does not shift it', () => {
    const { container } = render(
      <div style={{ backdropFilter: 'blur(8px)' }}>
        <FloatingTooltip id="tip" xPx={100} yPx={50}>
          text
        </FloatingTooltip>
      </div>
    );

    const tooltip = screen.getByRole('tooltip');
    expect(tooltip.parentElement).toBe(document.body);
    expect(container.querySelector('[role="tooltip"]')).toBeNull();
    expect(tooltip.style.left).toBe('112px');
    expect(tooltip.style.top).toBe('62px');
  });

  it('stays in the window: left of the right edge, and above the pointer near the bottom', () => {
    window.innerWidth = 1_000;
    window.innerHeight = 800;
    render(
      <FloatingTooltip id="tip" xPx={990} yPx={790}>
        text
      </FloatingTooltip>
    );

    const tooltip = screen.getByRole('tooltip');
    // 1,000 less the widest box (320) less the offset (12).
    expect(tooltip.style.left).toBe('668px');
    expect(tooltip.style.top).toBe('');
    expect(tooltip.style.bottom).toBe('22px');
  });
});
