import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { assertNever } from '@/lib/exhaustive';
import { formatInstant, formatMilliseconds } from '@/lib/format-duration';
import type { NodeTiming, TimingSegment } from '@/types/timing';

import { phaseFill, phaseLabel, type PhaseKey } from './phase';
import { PhaseStrip } from './phase-strip';
import {
  nodeTimingIsOpen,
  nodeWallTime,
  phaseComposition,
  segmentSpan,
} from './timeline-geometry';
import {
  endpointText,
  nodeCoverageText,
  segmentDurationText,
  segmentPhaseText,
  segmentSourceText,
  wallTimeText,
} from './timing-text';

/** Consecutive segments of one source: the node, a task, or one attempt. */
export interface SegmentGroup {
  title: string;
  segments: TimingSegment[];
}

/** The segments in time order, cut where their source changes. A retry
 * starts a new group (`task 1 · attempt 2`), and so does a new task of the
 * node (`task 2 · attempt 1`). */
export function groupSegments(segments: readonly TimingSegment[]): SegmentGroup[] {
  const groups: SegmentGroup[] = [];
  for (const segment of segments) {
    const title = segmentSourceText(segment.source);
    const last = groups[groups.length - 1];
    if (last !== undefined && last.title === title) {
      last.segments.push(segment);
    } else {
      groups.push({ title, segments: [segment] });
    }
  }
  return groups;
}

function SectionLabel({ text }: { text: string }) {
  return (
    <span className="text-xs uppercase tracking-wide text-muted-foreground">{text}</span>
  );
}

/** The phase timing of a node of a run, read from the run query, which the
 * graph and the timeline share. Nothing while the run is not loaded. */
export function NodeTimingSection({
  workflowId,
  taskIndex,
}: {
  workflowId: string;
  taskIndex: number;
}) {
  const { detail, readClock } = useWorkflowRun(workflowId);
  const node = detail?.nodes.find(candidate => candidate.task_index === taskIndex);
  if (node === undefined || readClock === undefined) {
    return null;
  }
  return <NodeTimingView timing={node.timing} readClock={readClock} />;
}

/** Coverage, wall time, time per phase, and the segments by task and
 * attempt. An open segment grows on the one-second clock. */
export function NodeTimingView({
  timing,
  readClock,
}: {
  timing: NodeTiming;
  readClock: ReadClock;
}) {
  const drawAtMs = useDrawTime(readClock, readClock.open && nodeTimingIsOpen(timing));
  return (
    <section aria-label="Phase timing" className="flex flex-col gap-2">
      <SectionLabel text="Phase timing" />
      <p className="text-xs text-muted-foreground">{nodeCoverageText(timing)}</p>
      <PhaseStrip timing={timing} drawAtMs={drawAtMs} />
      <TimingBody timing={timing} drawAtMs={drawAtMs} />
    </section>
  );
}

function TimingBody({ timing, drawAtMs }: { timing: NodeTiming; drawAtMs: number }) {
  switch (timing.kind) {
    case 'unavailable':
    case 'inconsistent':
      return null;
    case 'exact':
    case 'partial': {
      const wall = nodeWallTime(timing.segments, timing.endpoint, drawAtMs);
      const shares = phaseComposition(timing.segments, drawAtMs);
      return (
        <>
          <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-xs">
            <dt className="text-muted-foreground">wall time</dt>
            <dd className="font-mono">{wallTimeText(wall)}</dd>
            <dt className="text-muted-foreground">end</dt>
            <dd>{endpointText(timing.endpoint)}</dd>
            {shares.map(share => (
              <PhaseTotal key={share.key} phaseKey={share.key} ms={share.ms} />
            ))}
          </dl>
          <SegmentList segments={timing.segments} drawAtMs={drawAtMs} />
        </>
      );
    }
    default:
      return assertNever(timing, 'node timing');
  }
}

function PhaseTotal({ phaseKey, ms }: { phaseKey: PhaseKey; ms: number }) {
  return (
    <>
      <dt className="inline-flex items-center gap-1.5 text-muted-foreground">
        <span
          className="inline-block h-2 w-3 rounded-sm"
          style={{ background: phaseFill(phaseKey) }}
          aria-hidden
        />
        {phaseLabel(phaseKey)}
      </dt>
      <dd className="font-mono">{formatMilliseconds(ms)}</dd>
    </>
  );
}

function SegmentList({
  segments,
  drawAtMs,
}: {
  segments: readonly TimingSegment[];
  drawAtMs: number;
}) {
  return (
    <div className="flex flex-col gap-2">
      {groupSegments(segments).map((group, groupIndex) => (
        <div key={groupIndex} className="rounded-md border border-border p-2">
          <span className="text-10 uppercase tracking-wide text-muted-foreground">
            {group.title}
          </span>
          <ol className="mt-1 flex flex-col gap-0.5">
            {group.segments.map((segment, index) => {
              const span = segmentSpan(segment, drawAtMs);
              return (
                <li key={index} className="flex items-center gap-2 text-xs">
                  <span
                    className="inline-block h-2 w-3 shrink-0 rounded-sm"
                    style={{ background: phaseFill(segment.phase.kind) }}
                    aria-hidden
                  />
                  <span className="min-w-0 flex-1 truncate" title={segmentPhaseText(segment.phase)}>
                    {segmentPhaseText(segment.phase)}
                  </span>
                  <span className="shrink-0 font-mono text-10 text-muted-foreground">
                    {formatInstant(span.startMs)}
                  </span>
                  <span className="w-20 shrink-0 text-right font-mono text-10">
                    {segmentDurationText(segment.end, span.endMs - span.startMs)}
                  </span>
                </li>
              );
            })}
          </ol>
        </div>
      ))}
    </div>
  );
}
