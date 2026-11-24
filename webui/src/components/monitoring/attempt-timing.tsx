import { Info, TriangleAlert } from 'lucide-react';

import { phaseFill, phaseLabel } from '@/components/workflows/phase';
import { PhaseBar, type PhaseBarPart } from '@/components/workflows/phase-bar';
import { useNow } from '@/hooks/use-now';
import { clockText, clockTextFrom, dateLineText } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusColorVar, statusLabel } from '@/lib/status-utils';
import type { LeafTask, TaskAttempt } from '@/types/workflows';

import {
  attemptStory,
  storyTotals,
  type AllowedAttempts,
  type AttemptNote,
  type AttemptPhase,
  type AttemptPhaseDetail,
  type LiveAttempt,
  type RecordedAttempt,
  type StoryItem,
} from './attempt-story';

/** The live clock of a task in progress, for its "so far" times. */
const LIVE_TICK_MS = 1_000;

/** What a phase of an attempt holds beyond its name: `placed +1ms`, `1
 * earlier claim`, or nothing. */
export function attemptPhaseDetailText(phase: AttemptPhase): string | null {
  const detail: AttemptPhaseDetail = phase.detail;
  switch (detail.kind) {
    case 'none':
      return null;
    case 'earlier_claims':
      return detail.count === 1 ? '1 earlier claim' : `${detail.count} earlier claims`;
    case 'placed':
      return `placed +${formatMilliseconds(detail.placedAtMs - phase.startMs)}`;
    case 'placement_not_observed':
      return 'placement not observed';
    case 'claims_not_counted':
      return 'claims not counted';
    case 'worker_lost':
      return 'worker lost';
    default:
      return assertNever(detail, 'attempt phase detail');
  }
}

/** The phase name with what it holds: `slot wait · placed +1ms`. */
export function attemptPhaseText(phase: AttemptPhase): string {
  const detail = attemptPhaseDetailText(phase);
  return detail === null ? phaseLabel(phase.key) : `${phaseLabel(phase.key)} · ${detail}`;
}

/** What a note says, and how loud it is. */
export interface NoteView {
  tone: 'warning' | 'info';
  text: string;
}

export function attemptNoteView(note: AttemptNote, base: StoryBase): NoteView {
  switch (note.kind) {
    case 'claimed_more_than_once':
      return {
        tone: 'warning',
        text: `Claimed ${note.count} times: a lease expired or a claim was released before the claim that ran the attempt.`,
      };
    case 'claims_not_counted':
      return {
        tone: 'info',
        text: 'Claims not counted: the attempt began before the lifecycle record, so earlier claims can hide in its queue wait.',
      };
    case 'worker_lost':
      return {
        tone: 'warning',
        text: `Worker lost: stale recovery recorded the attempt. The last sign of life was at ${clockIn(base, note.lastSeenMs)}.`,
      };
    case 'cancelled':
      return { tone: 'info', text: 'Cancelled: an operator cancelled the task while this attempt ran.' };
    case 'placement_not_observed':
      return {
        tone: 'info',
        text: 'Placement not observed: the worker did not report when a child took the task.',
      };
    case 'not_captured':
      return {
        tone: 'info',
        text: 'Phase times not captured: the attempt was recorded before the lifecycle record.',
      };
    default:
      return assertNever(note, 'attempt note');
  }
}

/** `3 of 5 allowed`, or nothing for a single attempt of a task with no
 * retry. */
export function allowedText(attemptCount: number, allowed: AllowedAttempts): string | null {
  switch (allowed.kind) {
    case 'limit':
      return attemptCount === 1 && allowed.count === 1
        ? null
        : `${attemptCount} of ${allowed.count} allowed`;
    case 'no_task_row':
      return null;
    default:
      return assertNever(allowed, 'allowed attempts');
  }
}

/** The day of the story: the date line shows the day of its first time,
 * and a time on another day carries its day offset. A story of one attempt in
 * progress with no time (claimed) has none. */
export type StoryBase = { kind: 'at'; ms: number } | { kind: 'no_times' };

function liveBase(live: LiveAttempt): StoryBase {
  switch (live.kind) {
    case 'pending':
      return { kind: 'at', ms: live.dueMs };
    case 'running':
      return { kind: 'at', ms: live.startedMs };
    case 'claimed':
    case 'no_times':
      return { kind: 'no_times' };
    default:
      return assertNever(live, 'live attempt');
  }
}

function storyBase(items: readonly StoryItem[]): StoryBase {
  for (const item of items) {
    switch (item.kind) {
      case 'attempt':
        if (item.attempt.span.kind === 'span') {
          return { kind: 'at', ms: item.attempt.span.startMs };
        }
        break;
      case 'retry_wait':
        return { kind: 'at', ms: item.startMs };
      case 'live':
        return liveBase(item.attempt);
      default:
        assertNever(item, 'story item');
    }
  }
  return { kind: 'no_times' };
}

/** A time of day in the story: with its day offset from the story day. */
function clockIn(base: StoryBase, epochMs: number): string {
  switch (base.kind) {
    case 'at':
      return clockTextFrom(base.ms, epochMs);
    case 'no_times':
      return clockText(epochMs);
    default:
      return assertNever(base, 'story base');
  }
}

const spanText = (base: StoryBase, startMs: number, endMs: number): string =>
  `${clockIn(base, startMs)} → ${clockIn(base, endMs)}`;

/** The tooltip of a bar part: `running 4.9s (09:14:26.248 → 09:14:31.148)`. */
const partTitle = (base: StoryBase, text: string, startMs: number, endMs: number): string =>
  `${text} ${formatMilliseconds(endMs - startMs)} (${spanText(base, startMs, endMs)})`;

/** The parts of the story bar: every phase of every attempt, and each retry
 * wait, in time order. */
function storyParts(items: readonly StoryItem[], base: StoryBase): PhaseBarPart[] {
  return items.flatMap((item): PhaseBarPart[] => {
    switch (item.kind) {
      case 'attempt':
        return item.attempt.phases.map(phase => ({
          key: phase.key,
          ms: phase.endMs - phase.startMs,
          title: partTitle(
            base,
            `Attempt ${item.attempt.number} · ${attemptPhaseText(phase)}`,
            phase.startMs,
            phase.endMs
          ),
        }));
      case 'retry_wait':
        return [
          {
            key: 'retry_wait',
            ms: item.endMs - item.startMs,
            title: partTitle(base, 'retry wait', item.startMs, item.endMs),
          },
        ];
      case 'live':
        return [];
      default:
        return assertNever(item, 'story item');
    }
  });
}

function SectionLabel({ text }: { text: string }) {
  return <span className="text-xs uppercase tracking-wide text-muted-foreground">{text}</span>;
}

/**
 * The attempts of one task as one story: a date line, a bar over every
 * attempt and retry wait (with more than one attempt), then each attempt with
 * its answer, its phase bar, its phase table, its unusual facts and its error,
 * a retry wait between two attempts, and the attempt in progress of a live
 * task. Used by the workflow node panel and the task detail.
 */
export function AttemptTiming({
  attempts,
  leaf,
  isLoading,
}: {
  attempts: readonly TaskAttempt[];
  leaf: LeafTask | null;
  isLoading: boolean;
}) {
  const story = attemptStory(attempts, leaf);
  const allowed = allowedText(story.attemptCount, story.allowed);
  const base = storyBase(story.items);
  return (
    <section aria-label="Attempts" className="flex flex-col gap-2">
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-0.5">
        <span className="inline-flex items-baseline gap-2">
          <SectionLabel text="Attempts" />
          {allowed !== null && <span className="text-xs text-muted-foreground">{allowed}</span>}
        </span>
        {base.kind === 'at' && (
          <span className="font-mono text-10 text-muted-foreground">{dateLineText(base.ms)}</span>
        )}
      </div>
      {isLoading && story.items.length === 0 ? (
        <p className="text-xs text-muted-foreground">Loading…</p>
      ) : story.items.length === 0 ? (
        <p className="text-xs text-muted-foreground">No attempts recorded yet.</p>
      ) : (
        <StoryBody items={story.items} base={base} />
      )}
    </section>
  );
}

function StoryBody({ items, base }: { items: readonly StoryItem[]; base: StoryBase }) {
  const recordedCount = items.filter(item => item.kind === 'attempt').length;
  return (
    <>
      {recordedCount > 1 && <StorySummary items={items} base={base} />}
      <ol className="flex flex-col divide-y divide-hairline">
        {items.map((item, index) => (
          <li key={index} className="py-2">
            <StoryEntry item={item} base={base} />
          </li>
        ))}
      </ol>
    </>
  );
}

/** The whole story in one line and one bar: its time, the running time and
 * the retry wait of every attempt. */
function StorySummary({ items, base }: { items: readonly StoryItem[]; base: StoryBase }) {
  const totals = storyTotals(items);
  switch (totals.kind) {
    case 'no_times':
      return null;
    case 'measured': {
      const runningMs = totals.byPhase.get('running') ?? 0;
      const retryMs = totals.byPhase.get('retry_wait') ?? 0;
      const text =
        `${formatMilliseconds(totals.spanMs)} in all · running ${formatMilliseconds(runningMs)} · ` +
        `retry wait ${formatMilliseconds(retryMs)}`;
      return (
        <div className="flex flex-col gap-1">
          <span className="text-xs" title="From the first due time to the last record">
            {text}
          </span>
          <PhaseBar parts={storyParts(items, base)} label={`All attempts: ${text}`} size="regular" />
        </div>
      );
    }
    default:
      return assertNever(totals, 'story totals');
  }
}

function StoryEntry({ item, base }: { item: StoryItem; base: StoryBase }) {
  switch (item.kind) {
    case 'attempt':
      return <AttemptBlock attempt={item.attempt} base={base} />;
    case 'retry_wait':
      return <RetryGap startMs={item.startMs} endMs={item.endMs} base={base} />;
    case 'live':
      return <LiveBlock attempt={item.attempt} base={base} />;
    default:
      return assertNever(item, 'story item');
  }
}

/** The retry delay between two attempts: a striped gap with its time. */
function RetryGap({ startMs, endMs, base }: { startMs: number; endMs: number; base: StoryBase }) {
  return (
    <div className="flex items-center gap-2 pl-3 text-xs" data-story="retry_wait">
      <span
        className="h-4 w-1.5 shrink-0 rounded-sm"
        style={{ background: phaseFill('retry_wait') }}
        aria-hidden
      />
      <span className="font-mono font-semibold tabular-nums">{formatMilliseconds(endMs - startMs)}</span>
      <span className="text-muted-foreground">retry wait</span>
      <span className="ml-auto font-mono text-10 text-muted-foreground">
        {spanText(base, startMs, endMs)}
      </span>
    </div>
  );
}

function OutcomeChip({ outcome }: { outcome: string }) {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-hairline-strong px-2 py-0.5 text-xs font-medium">
      <span className="size-2 rounded-full" style={{ background: statusColorVar(outcome) }} aria-hidden />
      {statusLabel(outcome)}
    </span>
  );
}

/** One recorded attempt: the answer first (outcome, error code, run time,
 * when), then where its time went. */
function AttemptBlock({ attempt, base }: { attempt: RecordedAttempt; base: StoryBase }) {
  return (
    <article aria-label={`Attempt ${attempt.number}`} className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        <span className="text-xs font-semibold">Attempt {attempt.number}</span>
        <OutcomeChip outcome={attempt.outcome} />
        {attempt.errorCode !== null && (
          <span className="font-mono text-11 text-muted-foreground">{attempt.errorCode}</span>
        )}
        <RunTimeText attempt={attempt} />
      </div>
      <AttemptWhen attempt={attempt} base={base} />
      {attempt.phases.length > 0 && <AttemptPhases attempt={attempt} base={base} />}
      {attempt.notes.map((note, index) => (
        <NoteLine key={index} view={attemptNoteView(note, base)} />
      ))}
      {attempt.message !== null && (
        <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-glass-inset p-2 font-mono text-11 leading-snug text-foreground">
          {attempt.message}
        </pre>
      )}
    </article>
  );
}

function RunTimeText({ attempt }: { attempt: RecordedAttempt }) {
  switch (attempt.runTime.kind) {
    case 'measured':
      return (
        <span className="ml-auto text-xs" title="The time the task code ran">
          ran <span className="font-mono font-semibold">{formatMilliseconds(attempt.runTime.ms)}</span>
        </span>
      );
    case 'not_started':
      return null;
    default:
      return assertNever(attempt.runTime, 'run time');
  }
}

function AttemptWhen({ attempt, base }: { attempt: RecordedAttempt; base: StoryBase }) {
  const span = attempt.span;
  switch (span.kind) {
    case 'no_times':
      return null;
    case 'span':
      return (
        <div className="flex min-w-0 items-baseline gap-2 text-10 text-muted-foreground">
          <span className="shrink-0 whitespace-nowrap font-mono">
            {spanText(base, span.startMs, span.endMs)}
          </span>
          <span className="shrink-0 whitespace-nowrap font-mono">
            {formatMilliseconds(span.endMs - span.startMs)} in all
          </span>
          {attempt.host !== null && (
            <span className="ml-auto min-w-0 truncate font-mono" title={attempt.host}>
              {attempt.host}
            </span>
          )}
        </div>
      );
    default:
      return assertNever(span, 'attempt span');
  }
}

/** The phase bar of an attempt and its table: duration first, then the
 * phase, then its time span. */
function AttemptPhases({ attempt, base }: { attempt: RecordedAttempt; base: StoryBase }) {
  const parts: PhaseBarPart[] = attempt.phases.map(phase => ({
    key: phase.key,
    ms: phase.endMs - phase.startMs,
    title: partTitle(base, attemptPhaseText(phase), phase.startMs, phase.endMs),
  }));
  return (
    <>
      <PhaseBar
        parts={parts}
        label={`Attempt ${attempt.number}: ${parts
          .map(part => `${phaseLabel(part.key)} ${formatMilliseconds(part.ms)}`)
          .join(', ')}`}
        size="regular"
      />
      <table className="w-full text-xs">
        <tbody>
          {attempt.phases.map((phase, index) => (
            <tr key={index} data-phase={phase.key}>
              <td className="w-16 py-px pr-2 text-right align-top font-mono font-semibold tabular-nums">
                {formatMilliseconds(phase.endMs - phase.startMs)}
              </td>
              <td className="py-px pr-2 align-top">
                <span className="inline-flex flex-wrap items-center gap-x-1.5">
                  <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
                    <span
                      className="inline-block h-2.5 w-3 shrink-0 rounded-sm"
                      style={{ background: phaseFill(phase.key) }}
                      aria-hidden
                    />
                    {phaseLabel(phase.key)}
                  </span>
                  <PhaseDetail phase={phase} />
                </span>
              </td>
              <td className="py-px text-right align-top font-mono text-10 whitespace-nowrap text-muted-foreground">
                {spanText(base, phase.startMs, phase.endMs)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}

function PhaseDetail({ phase }: { phase: AttemptPhase }) {
  const text = attemptPhaseDetailText(phase);
  return text === null ? null : (
    <span className="whitespace-nowrap text-10 text-muted-foreground">{text}</span>
  );
}

function NoteLine({ view }: { view: NoteView }) {
  switch (view.tone) {
    case 'warning':
      return (
        <p className="flex items-start gap-1.5 text-xs" role="note">
          <TriangleAlert className="mt-px size-3.5 shrink-0" style={{ color: 'var(--warning-dark)' }} aria-hidden />
          <span>{view.text}</span>
        </p>
      );
    case 'info':
      return (
        <p className="flex items-start gap-1.5 text-xs text-muted-foreground" role="note">
          <Info className="mt-px size-3.5 shrink-0" aria-hidden />
          <span>{view.text}</span>
        </p>
      );
    default:
      return assertNever(view.tone, 'note tone');
  }
}

/** What the attempt in progress does at `nowMs`, in words. */
export function liveText(live: LiveAttempt, nowMs: number, base: StoryBase): string {
  switch (live.kind) {
    case 'pending':
      return live.dueMs > nowMs
        ? `Waits for its retry: due at ${clockIn(base, live.dueMs)}, in ${formatMilliseconds(live.dueMs - nowMs)}.`
        : `Waits in the queue since ${clockIn(base, live.dueMs)}: ${formatMilliseconds(nowMs - live.dueMs)} so far.`;
    case 'claimed':
      return 'A worker claimed it; the task code did not start.';
    case 'running':
      return `Running since ${clockIn(base, live.startedMs)}: ${formatMilliseconds(nowMs - live.startedMs)} so far.`;
    case 'no_times':
      return `${statusLabel(live.status)}; the task row holds no time for it.`;
    default:
      return assertNever(live, 'live attempt');
  }
}

/** The attempt in progress, on a one-second clock. */
function LiveBlock({ attempt, base }: { attempt: LiveAttempt; base: StoryBase }) {
  const nowMs = useNow(LIVE_TICK_MS);
  return (
    <article aria-label={`Attempt ${attempt.number} in progress`} className="flex flex-col gap-1">
      <span className="text-xs font-semibold">Attempt {attempt.number} · in progress</span>
      <span className="text-xs text-muted-foreground">{liveText(attempt, nowMs, base)}</span>
    </article>
  );
}
