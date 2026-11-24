import { Hint } from '@/components/ui/hint';
import { NoteLine, type NoteView } from '@/components/ui/note-line';
import { phaseLabel, phaseMeaning, phaseWeight } from '@/components/workflows/phase';
import { phaseFigure, TimeFigures, type TimeFigure } from '@/components/workflows/time-figures';
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
  type ChildTook,
  type LiveAttempt,
  type RecordedAttempt,
  type StoryItem,
} from './attempt-story';

/** The live clock of a task in progress, for its "so far" times. */
const LIVE_TICK_MS = 1_000;

/** The label of a phase of an attempt. The time from the first claim to the
 * claim that ran the attempt is `earlier claims`, so the two parts of a slot
 * wait have two names. The time after the last sign of life of a lost worker
 * is `worker lost`, the words of its note. */
export function attemptPhaseLabel(phase: AttemptPhase): string {
  switch (phase.detail.kind) {
    case 'earlier_claims':
      return 'earlier claims';
    case 'worker_lost':
      return 'worker lost';
    case 'none':
    case 'placed':
    case 'placement_not_observed':
    case 'claims_not_counted':
      return phaseLabel(phase.key);
    default:
      return assertNever(phase.detail, 'attempt phase detail');
  }
}

/** What a phase of an attempt is, for its hint. */
function attemptPhaseMeaning(phase: AttemptPhase): string {
  switch (phase.detail.kind) {
    case 'earlier_claims':
      return 'From the first claim of the attempt to the claim that ran it: claims that lost their lease or were released.';
    case 'claims_not_counted':
      return 'The claims of this attempt were not counted, so earlier claims can hide in this queue wait.';
    case 'worker_lost':
      return 'From the last sign of life of the worker to the record that recovery wrote.';
    case 'none':
    case 'placed':
    case 'placement_not_observed':
      return phaseMeaning(phase.key);
    default:
      return assertNever(phase.detail, 'attempt phase detail');
  }
}

/** The figures of an attempt: one for each of its phases, in record order. */
export function attemptFigures(attempt: RecordedAttempt): TimeFigure[] {
  return attempt.phases.map((phase, index) => {
    const ms = phase.endMs - phase.startMs;
    return {
      id: `${index}:${phase.key}`,
      label: attemptPhaseLabel(phase),
      ms,
      open: false,
      weight: phaseWeight(phase.key, ms),
      meaning: attemptPhaseMeaning(phase),
    };
  });
}

const sentenceCase = (text: string): string => text.charAt(0).toUpperCase() + text.slice(1);

/** The two parts of a long slot wait: the wait for a child, then the start
 * on the child. */
function childTookText(childTook: ChildTook, waitMs: number): string {
  switch (childTook.kind) {
    case 'after':
      return (
        `A child took the task after ${formatMilliseconds(childTook.ms)}; ` +
        `the start on the child took ${formatMilliseconds(waitMs - childTook.ms)}.`
      );
    case 'not_recorded':
      return 'The record does not show when a child took the task.';
    default:
      return assertNever(childTook, 'child took');
  }
}

export function attemptNoteView(note: AttemptNote, base: StoryBase): NoteView {
  switch (note.kind) {
    case 'long_step':
      return {
        tone: 'warning',
        text: `${sentenceCase(phaseLabel(note.key))} ${formatMilliseconds(note.ms)}. ${note.tells}`,
      };
    case 'long_slot_wait':
      return {
        tone: 'warning',
        text:
          `${sentenceCase(phaseLabel('claimed'))} ${formatMilliseconds(note.ms)}. ${note.tells} ` +
          childTookText(note.childTook, note.ms),
      };
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
        text: `Worker lost: recovery recorded the attempt. The last sign of life was at ${clockIn(base, note.lastSeenMs)}; the time after it is the time recovery needed to find the loss.`,
      };
    case 'cancelled':
      return {
        tone: 'info',
        text: 'Cancelled: an operator cancelled the task while this attempt ran. The end of the attempt is the time of the cancel.',
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

function SectionLabel({ text }: { text: string }) {
  return <span className="text-xs uppercase tracking-wide text-muted-foreground">{text}</span>;
}

/**
 * The attempts of one task as one story: a date line, the time of all
 * attempts by phase (with more than one attempt), then each attempt with its
 * outcome, its time, the time of each of its phases as a number, its unusual
 * facts, its error, and where and when it ran. A retry wait is between two
 * attempts, and a live task ends with its attempt in progress. Used by the
 * workflow node panel and the task detail.
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
        <p className="text-xs text-muted-foreground">No attempt is recorded.</p>
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
      {recordedCount > 1 && <StoryTotalsBox items={items} recordedCount={recordedCount} />}
      <ol className="flex flex-col gap-1.5">
        {items.map((item, index) => (
          <li key={index}>
            <StoryEntry item={item} base={base} />
          </li>
        ))}
      </ol>
    </>
  );
}

/** All recorded attempts as one total: the time from the first due time to
 * the last record, and the time of each phase over the attempts and the
 * retry waits. */
function StoryTotalsBox({
  items,
  recordedCount,
}: {
  items: readonly StoryItem[];
  recordedCount: number;
}) {
  const totals = storyTotals(items);
  switch (totals.kind) {
    case 'no_times':
      return null;
    case 'measured':
      return (
        <div
          data-story="totals"
          className="flex flex-col gap-1.5 rounded-md border border-border p-2.5"
        >
          <div className="flex items-baseline justify-between gap-2">
            <span className="text-xs font-semibold">{recordedCount} attempts</span>
            <Hint text="From the due time of the first attempt to the record of the last attempt.">
              <span className="font-mono text-sm font-semibold tabular-nums">
                {formatMilliseconds(totals.spanMs)}
              </span>
            </Hint>
          </div>
          <TimeFigures
            figures={totals.totals.map(phaseFigure)}
            layout="stacked"
            label={`Time of ${recordedCount} attempts by phase`}
          />
        </div>
      );
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

/** The retry delay between two attempts: a planned wait, with its time. */
function RetryGap({ startMs, endMs, base }: { startMs: number; endMs: number; base: StoryBase }) {
  return (
    <div
      className="flex flex-wrap items-baseline gap-x-2 px-2.5 py-0.5 text-xs"
      data-story="retry_wait"
    >
      <Hint text={phaseMeaning('retry_wait')} className="inline-flex items-baseline gap-2">
        <span className="text-muted-foreground">{phaseLabel('retry_wait')}</span>
        <span className="font-mono tabular-nums">{formatMilliseconds(endMs - startMs)}</span>
      </Hint>
      <span className="ml-auto font-mono text-10 text-muted-foreground">
        {spanText(base, startMs, endMs)}
      </span>
    </div>
  );
}

function OutcomeChip({ outcome }: { outcome: string }) {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-border px-2 py-0.5 text-xs font-medium">
      <span className="size-2 rounded-full" style={{ background: statusColorVar(outcome) }} aria-hidden />
      {statusLabel(outcome)}
    </span>
  );
}

/** One recorded attempt, in the order of the reader's questions: how it
 * ended and how long it took, where its time went, what was unusual, why it
 * failed, then when and where it ran. */
function AttemptBlock({ attempt, base }: { attempt: RecordedAttempt; base: StoryBase }) {
  const color = statusColorVar(attempt.outcome);
  const figures = attemptFigures(attempt);
  return (
    <article
      aria-label={`Attempt ${attempt.number}`}
      className="flex flex-col gap-2 rounded-md border border-border p-2.5"
      style={{ background: `color-mix(in oklab, ${color} 5%, var(--card))` }}
    >
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        <span className="text-xs font-semibold">Attempt {attempt.number}</span>
        <OutcomeChip outcome={attempt.outcome} />
        {attempt.errorCode !== null && (
          <span className="font-mono text-11 text-muted-foreground">{attempt.errorCode}</span>
        )}
        <AttemptTime attempt={attempt} />
      </div>
      {figures.length > 0 && (
        <TimeFigures figures={figures} layout="stacked" label={`Phases of attempt ${attempt.number}`} />
      )}
      {attempt.notes.map((note, index) => (
        <NoteLine key={index} view={attemptNoteView(note, base)} />
      ))}
      {attempt.message !== null && (
        <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-words rounded bg-glass-field p-2 font-mono text-11 leading-snug text-foreground">
          {attempt.message}
        </pre>
      )}
      <AttemptWhere attempt={attempt} base={base} />
    </article>
  );
}

/** The time of the attempt: from its first recorded time to its record. */
function AttemptTime({ attempt }: { attempt: RecordedAttempt }) {
  const span = attempt.span;
  switch (span.kind) {
    case 'no_times':
      return null;
    case 'span':
      return (
        <Hint
          text={
            attempt.phases.length > 0
              ? 'The time of the attempt: from its due time to its record.'
              : 'The time of the attempt: from its start to its record.'
          }
          className="ml-auto"
        >
          <span data-attempt-time className="font-mono text-sm font-semibold tabular-nums">
            {formatMilliseconds(span.endMs - span.startMs)}
          </span>
        </Hint>
      );
    default:
      return assertNever(span, 'attempt span');
  }
}

/** When and where the attempt ran: its time span and its worker host, for a
 * search in the logs. The host wraps; it is never cut. */
function AttemptWhere({ attempt, base }: { attempt: RecordedAttempt; base: StoryBase }) {
  const span = attempt.span;
  if (span.kind === 'no_times' && attempt.host === null) {
    return null;
  }
  return (
    <div
      data-attempt-where
      className="flex flex-wrap gap-x-2 gap-y-0.5 font-mono text-10 text-muted-foreground"
    >
      {span.kind === 'span' && (
        <span className="whitespace-nowrap">{spanText(base, span.startMs, span.endMs)}</span>
      )}
      {attempt.host !== null && <span className="break-all">on {attempt.host}</span>}
    </div>
  );
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
    <article
      aria-label={`Attempt ${attempt.number} in progress`}
      className="flex flex-col gap-1 rounded-md border border-border p-2.5"
    >
      <span className="text-xs font-semibold">Attempt {attempt.number} · in progress</span>
      <span className="text-xs text-muted-foreground">{liveText(attempt, nowMs, base)}</span>
    </article>
  );
}
