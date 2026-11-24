import { render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import {
  FloatingTooltip,
  nodeEndText,
  nodeEndWithCodeText,
  NodeTooltipBody,
  type NodeFocus,
} from '@/components/workflows/node-tooltip';
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
  options: { onPath?: boolean; facts?: string[] } = {}
): ReturnType<typeof render> {
  return render(
    <NodeTooltipBody
      node={node}
      drawAtMs={READ_MS}
      membership={options.onPath === true ? 'on_path' : 'off_path'}
      focus={focus}
      pause={{ kind: 'not_in_pause' }}
      facts={options.facts ?? []}
      failure={{ kind: 'none' }}
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

describe('NodeTooltipBody', () => {
  it('names the node, its status and its time without dependency wait', () => {
    const { container } = renderBody(RETRIED, { kind: 'node' });

    expect(container.textContent).toContain('extract');
    expect(container.textContent).toContain('completed');
    expect(container.textContent).toContain('node time 10.0s');
  });

  it('gives each phase of the node as a number and the end of the node', () => {
    const { container } = renderBody(RETRIED, { kind: 'node' });

    const figures = screen.getByRole('list', { name: 'Time of extract by phase' });
    expect(figures.querySelectorAll('li')).toHaveLength(8);
    expect(container.textContent).toContain(`End of the node: completed at ${clockText(instantMs(at(12)))}.`);
    expect(container.querySelector('[data-focus]')).toBeNull();
  });

  it('says that a node is on the critical path, and lists the facts of the view', () => {
    const { container } = renderBody(RETRIED, { kind: 'node' }, {
      onPath: true,
      facts: ['Tolerant join (dashed border): the node runs even when a dependency failed.'],
    });

    expect(container.textContent).toContain('On the critical path');
    expect(container.textContent).toContain('Tolerant join (dashed border)');
  });

  it('does not repeat the end or the critical path under the focus line that says it', () => {
    const { container } = renderBody(RETRIED, { kind: 'endpoint' }, { onPath: true });
    expect(container.textContent?.match(/End of the node/g)).toHaveLength(1);

    const line = renderBody(RETRIED, { kind: 'critical_line' }, { onPath: true });
    expect(line.container.textContent?.match(/On the critical path/g)).toHaveLength(1);
  });

  it('shows no focus line for a segment that a refetch removed', () => {
    const { container } = renderBody(RETRIED, { kind: 'segment', segmentIndex: 99 });

    expect(container.querySelector('[data-focus]')).toBeNull();
  });

  it('gives the coverage text of a node with no segments', () => {
    const { container } = renderBody(workflowNode(3, 'broken', 'RUNNING', CLOCK_ORDER_NODE), { kind: 'node' });

    expect(container.textContent).toContain('timing inconsistent: Clock order: claim');
    expect(container.textContent).toContain('node time —');
  });

  it('shows a dash as the time of a node that did not run', () => {
    const { container } = renderBody(nth(CASCADE_NODES, 3), { kind: 'node' });

    expect(container.textContent).toContain('node time —');
    expect(container.textContent).toContain('skipped at');
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
