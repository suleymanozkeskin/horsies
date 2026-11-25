// The shared tooltip of the run table: the mark of a node row, the mark of
// the run row, or a link of the longest chain.

import { clockText } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import type { WorkflowNode, WorkflowRunDetail } from '@/types/workflows';

import { FloatingTooltip, PhaseSwatch, TipHead, TipNote, TipTimes } from './mark-tip';
import { nodeLabel, NodeTooltip, PauseLine, SegmentTip } from './node-tooltip';
import { phaseLabel, phaseMeaning, type TimingKind } from './phase';
import { chainMembership, nodeEnd, timedSegments, type LongestChain } from './run-timing';
import { RunTip } from './run-summary';
import type { TimelineFrame } from './timeline-frame';
import type { Inspection, RunFocus } from './timeline-inspection';
import { durationText } from './timing-text';

/** The time of one kind along the longest chain. */
function ChainLaneTip({ chain, phase }: { chain: LongestChain; phase: TimingKind }) {
  const total = chain.kind === 'chain' ? chain.phases.find(candidate => candidate.key === phase) : undefined;
  if (total === undefined) {
    return null;
  }
  return (
    <div data-focus="lane" className="flex flex-col gap-0.5">
      <TipHead
        mark={<PhaseSwatch kind={phase} />}
        name={phaseLabel(phase)}
        context="longest chain"
        value={durationText(total.ms, total.open)}
      />
      <TipTimes text={`${total.count} ${total.count === 1 ? 'interval' : 'intervals'} · longest ${formatMilliseconds(total.longestMs)}`} />
      <TipNote text={phaseMeaning(phase)} />
    </div>
  );
}

function RunFocusTip({
  focus,
  detail,
  nodes,
  chain,
  frame,
  drawAtMs,
}: {
  focus: RunFocus;
  detail: WorkflowRunDetail;
  nodes: readonly WorkflowNode[];
  chain: LongestChain;
  frame: TimelineFrame;
  drawAtMs: number;
}) {
  switch (focus.kind) {
    case 'run':
      return frame.kind === 'composed' ? (
        <RunTip
          detail={detail}
          chain={chain}
          startMs={frame.axis.startMs}
          spanMs={frame.axis.spanMs}
          open={detail.timing.kind === 'composed' && detail.timing.open_segments}
        />
      ) : null;
    case 'segment': {
      const node = nodes.find(candidate => candidate.task_index === focus.taskIndex);
      const segment = node === undefined ? undefined : timedSegments(node.timing)[focus.segmentIndex];
      // A refetch can remove the segment under the cursor.
      return node === undefined || segment === undefined ? null : (
        <SegmentTip owner={nodeLabel(node)} segment={segment} drawAtMs={drawAtMs} />
      );
    }
    case 'lane':
      return <ChainLaneTip chain={chain} phase={focus.phase} />;
    default:
      return assertNever(focus, 'run focus');
  }
}

/** The tooltip of a link: the two nodes and the time at which the first
 * ended and the next could start. */
function LinkTip({ from, to, drawAtMs }: { from: WorkflowNode; to: WorkflowNode; drawAtMs: number }) {
  const end = nodeEnd(from.timing, drawAtMs);
  return (
    <div data-focus="chain_link" className="flex flex-col gap-0.5">
      <TipHead
        mark={<span className="mx-[5px] inline-block h-[11px] w-[1.5px] shrink-0 rounded-[1px]" style={{ background: 'var(--longest-chain)', opacity: 0.75 }} aria-hidden />}
        name="longest chain"
      />
      <TipTimes
        text={`${nodeLabel(from)} → ${nodeLabel(to)}${end.kind === 'measured' ? ` · ${clockText(end.endMs)}` : ''}`}
      />
      <TipNote text={`${nodeLabel(to)} starts when ${nodeLabel(from)} ends`} />
    </div>
  );
}

/** The shared tooltip of the table, at the pointer or under the keyboard
 * cursor. */
export function RunTableTooltip({
  id,
  detail,
  inspection,
  nodes,
  chain,
  frame,
  drawAtMs,
}: {
  id: string;
  detail: WorkflowRunDetail;
  inspection: Inspection;
  nodes: readonly WorkflowNode[];
  chain: LongestChain;
  frame: TimelineFrame;
  drawAtMs: number;
}) {
  switch (inspection.kind) {
    case 'none':
      return null;
    case 'row': {
      const node = nodes.find(candidate => candidate.task_index === inspection.taskIndex);
      // A refetch can remove the node under the cursor.
      return node === undefined ? null : (
        <FloatingTooltip id={id} xPx={inspection.xPx} yPx={inspection.yPx}>
          <NodeTooltip
            workflowId={detail.run.id}
            node={node}
            drawAtMs={drawAtMs}
            membership={chainMembership(chain, node.task_index)}
            focus={inspection.focus}
            pause={inspection.pause}
            facts={[]}
          />
        </FloatingTooltip>
      );
    }
    case 'run':
      return (
        <FloatingTooltip id={id} xPx={inspection.xPx} yPx={inspection.yPx}>
          <RunFocusTip focus={inspection.focus} detail={detail} nodes={nodes} chain={chain} frame={frame} drawAtMs={drawAtMs} />
          <PauseLine pause={inspection.pause} drawAtMs={drawAtMs} />
        </FloatingTooltip>
      );
    case 'link': {
      const from = nodes.find(node => node.task_index === inspection.fromIndex);
      const to = nodes.find(node => node.task_index === inspection.toIndex);
      return from === undefined || to === undefined ? null : (
        <FloatingTooltip id={id} xPx={inspection.xPx} yPx={inspection.yPx}>
          <LinkTip from={from} to={to} drawAtMs={drawAtMs} />
        </FloatingTooltip>
      );
    }
    default:
      return assertNever(inspection, 'inspection');
  }
}
