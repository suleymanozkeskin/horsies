// The links of the longest chain in the run table: a vertical line at the
// end of each node of the chain, from its row to the row of the next node of
// the chain, which starts when this node ends.

import { assertNever } from '@/lib/exhaustive';
import type { WorkflowNode } from '@/types/workflows';

import { nodeEnd, type LongestChain } from './run-timing';
import type { TimelineFrame } from './timeline-frame';
import type { Inspection } from './timeline-inspection';
import { pointPct } from './timeline-geometry';
import { LINEAR_LEFT_PX, LINEAR_RIGHT_PX } from './timeline-layout';

/** A link ends this far from the center of each of its two rows, so it does
 * not cross the marks of the rows. */
const LINK_INSET_PX = 7;

/** One link: from the end of one node of the chain to the row of the next. */
export interface ChainLink {
  fromIndex: number;
  toIndex: number;
  fromRow: number;
  toRow: number;
  /** The end of the first node: the start of the next. */
  atMs: number;
}

/** The links of the chain between the rows of `nodes` (in row order). */
export function chainLinksOf(chain: LongestChain, nodes: readonly WorkflowNode[], drawAtMs: number): ChainLink[] {
  switch (chain.kind) {
    case 'no_timing':
      return [];
    case 'chain': {
      const rowOf = new Map(nodes.map((node, row) => [node.node_index, row]));
      return chain.taskIndices.slice(1).flatMap((toIndex, step) => {
        const fromIndex = chain.taskIndices[step];
        const fromNode = nodes.find(node => node.node_index === fromIndex);
        const fromRow = fromIndex === undefined ? undefined : rowOf.get(fromIndex);
        const toRow = rowOf.get(toIndex);
        if (fromIndex === undefined || fromNode === undefined || fromRow === undefined || toRow === undefined) {
          return [];
        }
        const end = nodeEnd(fromNode.timing, drawAtMs);
        return end.kind === 'measured' ? [{ fromIndex, toIndex, fromRow, toRow, atMs: end.endMs }] : [];
      });
    }
    default:
      return assertNever(chain, 'longest chain');
  }
}

/** The links over the linear cells of the rows. `runPx`: the height of the
 * run row above the first node row. */
export function ChainLinks({
  links,
  frame,
  rowPx,
  runPx,
  onInspect,
}: {
  links: readonly ChainLink[];
  frame: TimelineFrame;
  rowPx: number;
  runPx: number;
  onInspect: (inspection: Inspection) => void;
}) {
  switch (frame.kind) {
    case 'unavailable':
      return null;
    case 'composed': {
      const centerPx = (row: number): number => runPx + row * rowPx + rowPx / 2;
      return (
        <div className="pointer-events-none absolute inset-y-0 z-[3]" style={{ left: LINEAR_LEFT_PX, right: LINEAR_RIGHT_PX }}>
          {links.map(link => {
            const top = Math.min(centerPx(link.fromRow), centerPx(link.toRow)) + LINK_INSET_PX;
            const height = Math.abs(centerPx(link.toRow) - centerPx(link.fromRow)) - 2 * LINK_INSET_PX;
            return height <= 0 ? null : (
              <span
                key={`${link.fromIndex}-${link.toIndex}`}
                data-chain-link={`${link.fromIndex}-${link.toIndex}`}
                className="pointer-events-auto absolute -ml-[4.5px] w-[9px] cursor-default"
                style={{ left: `${pointPct(link.atMs, frame.axis)}%`, top, height }}
                onPointerMove={event =>
                  onInspect({ kind: 'link', fromIndex: link.fromIndex, toIndex: link.toIndex, xPx: event.clientX, yPx: event.clientY })
                }
                onPointerLeave={() => onInspect({ kind: 'none' })}
              >
                <span
                  className="absolute inset-y-0 left-[3.75px] w-[1.5px] rounded-[1px]"
                  style={{ background: 'var(--longest-chain)', opacity: 0.75 }}
                />
              </span>
            );
          })}
        </div>
      );
    }
    default:
      return assertNever(frame, 'timeline frame');
  }
}
