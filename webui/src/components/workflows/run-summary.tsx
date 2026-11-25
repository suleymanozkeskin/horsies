import { useEffect, type ReactNode } from 'react';

import { StatusChip } from '@/components/ui/status-chip';
import { Hint } from '@/components/ui/hint';
import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { clockSecondText, clockText, dateText } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { statusLabel } from '@/lib/status-utils';
import type { ComposedRunTiming } from '@/types/timing';
import type { WorkflowRunDetail, WorkflowStatus } from '@/types/workflows';

import { ShownTipBox, TipHead, TipNote, TipPhaseList, TipTimes, useMarkTips, type MarkTips } from './mark-tip';
import { OutcomeCap } from './outcome-sign';
import type { TimingKind } from './phase';
import { longestChain, pausedTime, type LongestChain, type PausedTime } from './run-timing';
import { nodeStatusCounts } from './status';
import { runAxis } from './timeline-geometry';
import { keyEntryId, TimingKey, type KeyEntry, type KeyLight, type KeyPhase } from './timing-key';
import { durationText, unavailableReasonText } from './timing-text';

const nodeLabelOf = (detail: WorkflowRunDetail, taskIndex: number): string => {
  const node = detail.nodes.find(candidate => candidate.task_index === taskIndex);
  return node === undefined ? `#${taskIndex}` : (node.node_id ?? node.task_name);
};

/** The nodes of a longest chain in order: `a → b → e`. */
export const chainNodesText = (detail: WorkflowRunDetail, taskIndices: readonly number[]): string =>
  taskIndices.map(index => nodeLabelOf(detail, index)).join(' → ');

/** The key entries of the longest chain: each kind of its phases without its
 * dependency wait, with its total. They add up to the duration of a run that
 * had no pause. */
export function chainKeyPhases(chain: LongestChain): KeyPhase[] {
  switch (chain.kind) {
    case 'chain':
      return chain.phases.map(total => ({
        phase: total.key,
        total: { kind: 'total', ms: total.ms, context: 'on the longest chain' },
      }));
    case 'no_timing':
      return [];
    default:
      return assertNever(chain, 'longest chain');
  }
}

/** The status counts of the run; each toggles a filter that fades the nodes
 * of the other statuses. */
function StatusFilter({
  detail,
  active,
  onToggle,
}: {
  detail: WorkflowRunDetail;
  active: ReadonlySet<string>;
  onToggle: (status: WorkflowStatus) => void;
}) {
  const filtering = active.size > 0;
  return (
    <span className="inline-flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
      <span>
        {detail.nodes.length} {detail.nodes.length === 1 ? 'node' : 'nodes'}
      </span>
      {nodeStatusCounts(detail.nodes.map(node => node.node_status)).map(({ status, count }) => {
        const on = active.has(status);
        return (
          <Hint
            key={status}
            text={
              on
                ? `The view shows the ${statusLabel(status)} nodes. Click to show all nodes.`
                : `${count} ${count === 1 ? 'node is' : 'nodes are'} ${statusLabel(status)}. Click to fade the other nodes.`
            }
          >
            <button
              type="button"
              onClick={() => onToggle(status)}
              aria-pressed={on}
              className="inline-flex items-center gap-1.25 rounded px-1 hover:text-foreground"
              style={{ opacity: filtering && !on ? 0.45 : 1 }}
            >
              <OutcomeCap status={status} sizePx={11} />
              <span className="font-mono text-foreground tabular-nums">{count}</span>
              {statusLabel(status)}
            </button>
          </Hint>
        );
      })}
    </span>
  );
}

/** The run as a whole, for the tooltip of its duration: its status, its
 * time, its start → end, the phases of its longest chain, and the chain. */
export function RunTip({
  detail,
  chain,
  startMs,
  spanMs,
  open,
}: {
  detail: WorkflowRunDetail;
  chain: LongestChain;
  startMs: number;
  spanMs: number;
  open: boolean;
}) {
  return (
    <>
      <TipHead
        mark={<OutcomeCap status={detail.run.status} sizePx={12} />}
        name="run"
        context={statusLabel(detail.run.status)}
        value={durationText(spanMs, open)}
      />
      <TipTimes text={`${clockText(startMs)} → ${open ? 'open' : clockText(startMs + spanMs)}`} />
      {chain.kind === 'chain' && chain.phases.length > 0 && (
        <TipPhaseList items={chain.phases.map(total => ({ kind: total.key, ms: total.ms }))} />
      )}
      {chain.kind === 'chain' && (
        <TipNote text={`the phases along the longest chain: ${chainNodesText(detail, chain.taskIndices)}`} />
      )}
    </>
  );
}

/** The pause time of a run, after the phases of the key. */
function PausedItem({ paused, tips }: { paused: PausedTime; tips: MarkTips<KeyLight> }) {
  switch (paused.kind) {
    case 'not_paused':
      return null;
    case 'paused':
      return (
        <li
          tabIndex={0}
          data-run-paused
          className="inline-flex cursor-default items-center gap-[5px] whitespace-nowrap rounded-[3px] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
          {...tips.bind({
            id: 'run:paused',
            content: (
              <>
                <TipHead name="run paused" value={durationText(paused.ms, paused.open)} />
                <TipNote text="the time the run was paused; tasks that run go on, and no new task starts" />
              </>
            ),
            highlight: { kind: 'all' },
          })}
        >
          <span className="inline-block h-[9px] w-3 shrink-0 rounded-[2px] border border-border" style={{ background: 'var(--pause-band)' }} aria-hidden />
          <span>paused</span>
          <span className="font-mono text-11 text-muted-foreground tabular-nums">{durationText(paused.ms, paused.open)}</span>
        </li>
      );
    default:
      return assertNever(paused, 'paused time');
  }
}

/** What a key entry of the summary lights: the marks of its kind. */
function keyEntryLight(entry: KeyEntry): KeyLight {
  switch (entry.kind) {
    case 'phase':
      return { kind: 'phase', phase: entry.entry.phase };
    case 'mark':
      return { kind: 'all' };
    default:
      return assertNever(entry, 'key entry');
  }
}

/** The light that the shown tooltip of a key asks for. */
export function shownLight(tips: MarkTips<KeyLight>): KeyLight {
  return tips.shown.kind === 'shown' ? tips.shown.tip.highlight : { kind: 'all' };
}

/** Report the light of the shown key entry to the view, once per change. */
export function useReportLight(light: KeyLight, onIsolate: (light: KeyLight) => void): void {
  const lit: TimingKind | 'all' = light.kind === 'phase' ? light.phase : 'all';
  useEffect(() => {
    onIsolate(lit === 'all' ? { kind: 'all' } : { kind: 'phase', phase: lit });
  }, [lit, onIsolate]);
}

/**
 * The run summary over the graph and the timeline. Its first line: the
 * status of the run, its date and its start → end, and the status counts
 * that filter the nodes. Its second line, the key with totals: the duration
 * of the run, the time of each phase of its longest chain (they add up to
 * the duration of a run with no pause), and the pause time. A key entry
 * lights the marks of its kind (`onIsolate`).
 */
export function RunTimingSummary({
  workflowId,
  statusFilter,
  onToggleStatus,
  isolation,
  onIsolate,
}: {
  workflowId: string;
  statusFilter: ReadonlySet<string>;
  onToggleStatus: (status: WorkflowStatus) => void;
  isolation: KeyLight;
  onIsolate: (light: KeyLight) => void;
}) {
  const { detail, readClock } = useWorkflowRun(workflowId);
  if (detail === undefined || readClock === undefined) {
    return null;
  }
  return (
    <RunTimingSummaryView
      detail={detail}
      readClock={readClock}
      statusFilter={statusFilter}
      onToggleStatus={onToggleStatus}
      isolation={isolation}
      onIsolate={onIsolate}
    />
  );
}

interface SummaryViewProps {
  detail: WorkflowRunDetail;
  readClock: ReadClock;
  statusFilter: ReadonlySet<string>;
  onToggleStatus: (status: WorkflowStatus) => void;
  isolation: KeyLight;
  onIsolate: (light: KeyLight) => void;
}

function HeaderLine({ children, filter }: { children: ReactNode; filter: ReactNode }) {
  return (
    <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1">
      {children}
      <span className="ml-auto">{filter}</span>
    </div>
  );
}

export function RunTimingSummaryView(props: SummaryViewProps) {
  const { detail, readClock, statusFilter, onToggleStatus } = props;
  const drawAtMs = useDrawTime(readClock, readClock.open);
  const timing = detail.timing;
  const filter = <StatusFilter detail={detail} active={statusFilter} onToggle={onToggleStatus} />;
  switch (timing.kind) {
    case 'unavailable':
      return (
        <section aria-label="Run summary" className="flex flex-col gap-1.5 border-b border-hairline pb-3">
          <HeaderLine filter={filter}>
            <StatusChip status={detail.run.status} />
            <span className="text-xs text-muted-foreground">
              Timing unavailable. {unavailableReasonText(timing.reason)}
            </span>
          </HeaderLine>
        </section>
      );
    case 'composed':
      return <ComposedSummary {...props} timing={timing} drawAtMs={drawAtMs} filter={filter} />;
    default:
      return assertNever(timing, 'run timing');
  }
}

function ComposedSummary({
  detail,
  timing,
  drawAtMs,
  filter,
  isolation,
  onIsolate,
}: SummaryViewProps & { timing: ComposedRunTiming; drawAtMs: number; filter: ReactNode }) {
  const tips = useMarkTips<KeyLight>();
  useReportLight(shownLight(tips), onIsolate);
  const chain = longestChain(detail.nodes, detail.edges, drawAtMs);
  const axis = runAxis(timing, drawAtMs);
  const open = timing.open_segments;
  const lead = (
    <li
      tabIndex={0}
      className="mr-0.5 inline-flex cursor-default items-center gap-1.5 rounded-[3px] text-xs text-muted-foreground focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
      {...tips.bind({
        id: 'run:duration',
        content: <RunTip detail={detail} chain={chain} startMs={axis.startMs} spanMs={axis.spanMs} open={open} />,
        highlight: { kind: 'all' },
      })}
    >
      <span>duration</span>
      <b data-run-duration className="font-mono text-13 font-semibold text-foreground tabular-nums">
        {durationText(axis.spanMs, open)}
      </b>
    </li>
  );
  return (
    <section aria-label="Run summary" className="flex flex-col gap-2 border-b border-hairline pb-3">
      <HeaderLine filter={filter}>
        <StatusChip status={detail.run.status} />
        <span data-run-range className="font-mono text-11 text-muted-foreground">
          {dateText(axis.startMs)} · {clockSecondText(axis.startMs)} → {open ? 'now' : clockSecondText(axis.startMs + axis.spanMs)}
        </span>
      </HeaderLine>
      <TimingKey
        lead={lead}
        trail={<PausedItem paused={pausedTime(timing, drawAtMs)} tips={tips} />}
        phases={chainKeyPhases(chain)}
        marks={[]}
        light={isolation}
        bind={(entry, content) => tips.bind({ id: keyEntryId(entry), content, highlight: keyEntryLight(entry) })}
      />
      <ShownTipBox tips={tips} />
    </section>
  );
}
