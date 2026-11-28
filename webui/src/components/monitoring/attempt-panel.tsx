// The attempt panel: one task with all its attempts, used by the task detail
// and by the workflow node panel. Two scales, each with one job: the strip
// (linear) shows the order and the share of each attempt, phase and retry
// wait; the bars (log) show the size of each time.

import type { ReactNode } from 'react';

import { StatusChip } from '@/components/ui/status-chip';
import {
  ShownTipBox,
  TipHead,
  TipNote,
  TipTimes,
  useMarkTips,
  type MarkTips,
} from '@/components/workflows/mark-tip';
import { keyEntryId, TimingKey, type KeyEntry, type KeyMark } from '@/components/workflows/timing-key';
import { useLocalStorage } from '@/hooks/use-local-storage';
import { useNow } from '@/hooks/use-now';
import { clockSecondTextFrom } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';
import type { TaskDetail } from '@/types/tasks';

import { AttemptBars } from './attempt-bars';
import { AttemptDetails } from './attempt-details';
import { AttemptErrors } from './attempt-errors';
import {
  barGroupingOf,
  clockFrom,
  DEFAULT_BAR_GROUPING,
  errorGroups,
  keyLightOf,
  panelBase,
  panelKeyPhases,
  panelModel,
  panelOutcomes,
  panelSpan,
  taskOnlyFailure,
  type BarGrouping,
  type PanelBase,
  type PanelHighlight,
  type PanelModel,
} from './attempt-panel-model';
import { attemptStory, type AllowedAttempts, type LiveAttempt } from './attempt-story';
import { AttemptStrip } from './attempt-strip';
import { spanText } from './attempt-tips';
import { ErrorChip } from './error-chip';

/** The browser key of the grouping of the bars: the choice of the reader
 * holds for every task. */
const BAR_GROUPING_KEY = 'attempt-bars-grouping';
/** The clock of an attempt in progress, for its times so far. */
const LIVE_TICK_MS = 1_000;

/** `attempt 3 of 5`, or nothing for a task with no attempt. */
function attemptOfText(attemptCount: number, allowed: AllowedAttempts): string | null {
  switch (allowed.kind) {
    case 'limit':
      return attemptCount === 0 ? null : `${attemptCount} of ${allowed.count}`;
    case 'no_task_row':
      return null;
    default:
      return assertNever(allowed, 'allowed attempts');
  }
}

function Meta({ label, children }: { label: string; children: ReactNode }) {
  return (
    <span>
      <span className="mr-1.25 text-muted-foreground">{label}</span>
      {children}
    </span>
  );
}

/** What the attempt in progress does at `nowMs`, in short words. */
export function liveText(live: LiveAttempt, nowMs: number, base: PanelBase): string {
  const head = `attempt ${live.number} · `;
  switch (live.kind) {
    case 'pending':
      return live.dueMs > nowMs
        ? `${head}retry wait · due ${clockFrom(base, live.dueMs)} · in ${formatMilliseconds(live.dueMs - nowMs)}`
        : `${head}queue wait · since ${clockFrom(base, live.dueMs)} · ${formatMilliseconds(nowMs - live.dueMs)} so far`;
    case 'claimed':
      return `${head}claimed`;
    case 'running':
      return `${head}running · since ${clockFrom(base, live.startedMs)} · ${formatMilliseconds(nowMs - live.startedMs)} so far`;
    case 'no_times':
      return `${head}${statusLabel(live.status)} · no times in the task row`;
    default:
      return assertNever(live, 'live attempt');
  }
}

/** The attempt in progress, on a one-second clock. */
function LiveLine({ live, base }: { live: LiveAttempt; base: PanelBase }) {
  const nowMs = useNow(LIVE_TICK_MS);
  return (
    <p data-live-attempt className="m-0 text-xs text-muted-foreground">
      {liveText(live, nowMs, base)}
    </p>
  );
}

function Headline({ fromMs, toMs, spanMs, tips }: { fromMs: number; toMs: number; spanMs: number; tips: MarkTips<PanelHighlight> }) {
  return (
    <div
      tabIndex={0}
      data-headline
      className="flex cursor-default flex-wrap items-baseline gap-x-2 gap-y-0.5 rounded-sm focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
      {...tips.bind({
        id: 'headline',
        content: (
          <>
            <TipHead name="duration" value={formatMilliseconds(spanMs)} />
            <TipTimes text={spanText(fromMs, fromMs, toMs)} />
            <TipNote text="first due time → last record" />
          </>
        ),
        highlight: { kind: 'none' },
      })}
    >
      <span className="text-xs text-muted-foreground">duration</span>
      <b data-total className="font-mono text-lg leading-tight font-semibold tabular-nums">
        {formatMilliseconds(spanMs)}
      </b>
      <span className="ml-auto font-mono text-11 whitespace-nowrap text-muted-foreground">
        {clockSecondTextFrom(fromMs, fromMs)} → {clockSecondTextFrom(fromMs, toMs)}
      </span>
    </div>
  );
}

/** What a key entry lights: the marks of its kind, or nothing for a mark. */
function keyEntryHighlight(entry: KeyEntry): PanelHighlight {
  switch (entry.kind) {
    case 'phase':
      return { kind: 'phase', phase: entry.entry.phase };
    case 'mark':
      return { kind: 'none' };
    default:
      return assertNever(entry, 'key entry');
  }
}

/** The timing section: the headline, the strip, the key with its totals,
 * the bars, and the attempt in progress. */
function TimingSection({
  model,
  tips,
  grouping,
  onGrouping,
}: {
  model: PanelModel;
  tips: MarkTips<PanelHighlight>;
  grouping: BarGrouping;
  onGrouping: (grouping: BarGrouping) => void;
}) {
  const span = panelSpan(model.intervals);
  const highlight: PanelHighlight = tips.shown.kind === 'shown' ? tips.shown.tip.highlight : { kind: 'none' };
  switch (span.kind) {
    case 'no_times':
      return (
        <section aria-label="Timing" className="flex flex-col gap-2 border-t border-hairline pt-3">
          {model.live.kind === 'live' ? (
            <LiveLine live={model.live.attempt} base={{ kind: 'no_times' }} />
          ) : (
            <p className="m-0 text-xs text-muted-foreground">
              {model.attempts.length === 0 ? 'no attempt recorded' : 'no time recorded'}
            </p>
          )}
        </section>
      );
    case 'span': {
      const marks: KeyMark[] = [
        ...panelOutcomes(model.attempts).map((status): KeyMark => ({ kind: 'outcome', status })),
        { kind: 'pattern', pattern: 'wait' },
        { kind: 'pattern', pattern: 'work' },
      ];
      return (
        <section aria-label="Timing" className="flex flex-col gap-2.25 border-t border-hairline pt-3">
          <Headline fromMs={span.fromMs} toMs={span.toMs} spanMs={span.spanMs} tips={tips} />
          <AttemptStrip model={model} fromMs={span.fromMs} spanMs={span.spanMs} highlight={highlight} bind={tips.bind} />
          <TimingKey
            phases={panelKeyPhases(model.intervals, model.attempts.length)}
            marks={marks}
            light={keyLightOf(highlight)}
            bind={(entry, content) => tips.bind({ id: keyEntryId(entry), content, highlight: keyEntryHighlight(entry) })}
          />
          <AttemptBars
            model={model}
            grouping={grouping}
            onGrouping={onGrouping}
            baseMs={span.fromMs}
            highlight={highlight}
            bind={tips.bind}
          />
          {model.live.kind === 'live' && <LiveLine live={model.live.attempt} base={{ kind: 'at', ms: span.fromMs }} />}
        </section>
      );
    }
    default:
      return assertNever(span, 'panel span');
  }
}

/**
 * One task with its attempts: a title line (name, status, error code), a
 * quiet meta line, the actions of the view (`actions`), the errors grouped,
 * the timing on two scales with its key, and the details, closed. Every mark
 * has a tooltip. `titleEnd`: a control at the end of the title line (the
 * close button of the task detail). `traceTemplate`: the trace link template
 * of the server, or null for none.
 */
export function AttemptPanel({
  detail,
  traceTemplate,
  titleEnd,
  actions,
}: {
  detail: TaskDetail;
  traceTemplate: string | null;
  titleEnd?: ReactNode;
  actions?: ReactNode;
}) {
  const leaf = detail.task_row;
  const story = attemptStory(detail.attempts, leaf);
  const model = panelModel(story);
  const tips = useMarkTips<PanelHighlight>();
  const [stored, setStored] = useLocalStorage<unknown>(BAR_GROUPING_KEY, DEFAULT_BAR_GROUPING);
  const attemptOf = attemptOfText(story.attemptCount, story.allowed);
  return (
    <div
      className="@container flex flex-col gap-3.5 text-13 leading-normal"
      onKeyDown={event => {
        if (event.key === 'Escape') {
          tips.hide();
        }
      }}
    >
      <header className="flex items-start gap-2">
        <div className="flex min-w-0 flex-1 flex-wrap items-center gap-x-2 gap-y-1.5">
          <h3 className="m-0 mr-auto min-w-0 font-mono text-sm leading-snug font-semibold wrap-anywhere">{detail.task_name}</h3>
          <StatusChip status={leaf.status} />
          {leaf.error_code !== null && <ErrorChip code={leaf.error_code} category={detail.error_category} />}
        </div>
        {titleEnd}
      </header>
      <p data-task-meta className="-mt-2 mb-0 flex flex-wrap gap-x-3.5 gap-y-0.5 text-xs">
        <Meta label="queue">{detail.queue}</Meta>
        <Meta label="priority">{detail.priority}</Meta>
        {attemptOf !== null && <Meta label="attempt">{attemptOf}</Meta>}
      </p>
      {actions}
      <AttemptErrors
        groups={errorGroups(model.attempts)}
        taskName={detail.task_name}
        titleCode={leaf.error_code}
        taskFailure={taskOnlyFailure(leaf, model.attempts)}
      />
      <TimingSection model={model} tips={tips} grouping={barGroupingOf(stored)} onGrouping={setStored} />
      <AttemptDetails
        key={leaf.task_id}
        model={model}
        leaf={leaf}
        base={panelBase(panelSpan(model.intervals))}
        traceTemplate={traceTemplate}
      />
      <ShownTipBox tips={tips} />
    </div>
  );
}
