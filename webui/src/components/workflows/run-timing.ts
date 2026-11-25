// Run-level timing from the segments of every node: the longest chain and
// its phases (the split of the run duration), the time of a node without its
// dependency wait, the node with the longest such time, the pause time of a
// run, and the counts of a node that the normal case does not have. Pure: no
// React, no DOM. Every function takes the draw time (`drawAtMs`), to which an
// open segment runs.

import { assertNever } from '@/lib/exhaustive';
import { instantMs } from '@/lib/time-text';
import type { ComposedRunTiming, NodeTiming, TimingSegment } from '@/types/timing';
import type { WorkflowEdge, WorkflowNode } from '@/types/workflows';

import type { TimingKind } from './phase';
import {
  pauseSpan,
  phaseComposition,
  phaseTotals,
  segmentInterval,
  segmentSpan,
  type PhaseInterval,
  type PhaseTotal,
} from './timeline-geometry';

/** The segments of a node whose timing has them. */
export function timedSegments(timing: NodeTiming): readonly TimingSegment[] {
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return timing.segments;
    case 'unavailable':
    case 'inconsistent':
      return [];
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The end of a node: the latest of its segment ends and its endpoint, the
 * same end as its wall time. */
export type NodeEnd = { kind: 'measured'; endMs: number } | { kind: 'no_timing' };

export function nodeEnd(timing: NodeTiming, drawAtMs: number): NodeEnd {
  switch (timing.kind) {
    case 'unavailable':
    case 'inconsistent':
      return { kind: 'no_timing' };
    case 'exact':
    case 'partial': {
      let endMs = Number.NEGATIVE_INFINITY;
      for (const segment of timing.segments) {
        endMs = Math.max(endMs, segmentSpan(segment, drawAtMs).endMs);
      }
      switch (timing.endpoint.kind) {
        case 'terminal':
          endMs = Math.max(endMs, instantMs(timing.endpoint.at));
          break;
        case 'not_terminal':
          break;
        default:
          return assertNever(timing.endpoint, 'node endpoint');
      }
      return Number.isFinite(endMs) ? { kind: 'measured', endMs } : { kind: 'no_timing' };
    }
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The time of a node that is not dependency wait. */
export function activeMs(segments: readonly TimingSegment[], drawAtMs: number): number {
  let total = 0;
  for (const segment of segments) {
    if (segment.phase.kind !== 'pending') {
      const span = segmentSpan(segment, drawAtMs);
      total += Math.max(span.endMs - span.startMs, 0);
    }
  }
  return total;
}

/**
 * The longest chain of a run: the chain of nodes whose end times decided the
 * end of the run. It ends at the node that ended last; each step goes to the
 * dependency that ended last, the one that released the node. `phases` is
 * the time of the chain without its dependency wait, by kind: each node of
 * the chain starts when the node before it ends, so the phases add up to the
 * duration of a run that had no pause. `lengthMs` is their sum.
 */
export type LongestChain =
  | { kind: 'chain'; taskIndices: number[]; lengthMs: number; phases: PhaseTotal[] }
  | { kind: 'no_timing' };

interface TimedNode {
  node: WorkflowNode;
  endMs: number;
  activeMs: number;
}

/** The later end wins; on a tie, the node with more time that is not
 * dependency wait, then the lower task index. A skip cascade ends many nodes
 * at one instant, and the node that ran is the cause. */
function decides(candidate: TimedNode, best: TimedNode | undefined): boolean {
  if (best === undefined) {
    return true;
  }
  if (candidate.endMs !== best.endMs) {
    return candidate.endMs > best.endMs;
  }
  if (candidate.activeMs !== best.activeMs) {
    return candidate.activeMs > best.activeMs;
  }
  return candidate.node.task_index < best.node.task_index;
}

function timedNodes(nodes: readonly WorkflowNode[], drawAtMs: number): Map<number, TimedNode> {
  const timed = new Map<number, TimedNode>();
  for (const node of nodes) {
    const end = nodeEnd(node.timing, drawAtMs);
    switch (end.kind) {
      case 'measured':
        timed.set(node.task_index, {
          node,
          endMs: end.endMs,
          activeMs: activeMs(timedSegments(node.timing), drawAtMs),
        });
        break;
      case 'no_timing':
        break;
      default:
        assertNever(end, 'node end');
    }
  }
  return timed;
}

function dependenciesByNode(edges: readonly WorkflowEdge[]): Map<number, number[]> {
  const dependencies = new Map<number, number[]>();
  for (const edge of edges) {
    const list = dependencies.get(edge.to_index) ?? [];
    list.push(edge.from_index);
    dependencies.set(edge.to_index, list);
  }
  return dependencies;
}

export function longestChain(
  nodes: readonly WorkflowNode[],
  edges: readonly WorkflowEdge[],
  drawAtMs: number
): LongestChain {
  const timed = timedNodes(nodes, drawAtMs);
  let last: TimedNode | undefined;
  for (const candidate of timed.values()) {
    last = decides(candidate, last) ? candidate : last;
  }
  if (last === undefined) {
    return { kind: 'no_timing' };
  }
  const dependencies = dependenciesByNode(edges);
  const chain: TimedNode[] = [last];
  const visited = new Set<number>([last.node.task_index]);
  // A walk in a DAG visits each node once at most: the node count bounds it.
  let current = last;
  for (let step = 0; step < nodes.length; step += 1) {
    let release: TimedNode | undefined;
    for (const index of dependencies.get(current.node.task_index) ?? []) {
      const candidate = timed.get(index);
      if (candidate !== undefined && !visited.has(index) && decides(candidate, release)) {
        release = candidate;
      }
    }
    if (release === undefined) {
      break;
    }
    visited.add(release.node.task_index);
    chain.unshift(release);
    current = release;
  }
  const segments = chain
    .flatMap(entry => timedSegments(entry.node.timing))
    .filter(segment => segment.phase.kind !== 'pending');
  const phases = phaseComposition(segments, drawAtMs);
  return {
    kind: 'chain',
    taskIndices: chain.map(entry => entry.node.task_index),
    lengthMs: phases.reduce((sum, total) => sum + total.ms, 0),
    phases,
  };
}

/** Whether a node is on the longest chain of its run. */
export type ChainMembership = 'on_chain' | 'off_chain';

export function chainMembership(chain: LongestChain, taskIndex: number): ChainMembership {
  switch (chain.kind) {
    case 'chain':
      return chain.taskIndices.includes(taskIndex) ? 'on_chain' : 'off_chain';
    case 'no_timing':
      return 'off_chain';
    default:
      return assertNever(chain, 'longest chain');
  }
}

/** The active span of a node: from the end of its dependency wait (its
 * first segment that is not `pending`) to its end. */
export type ActiveSpan = { kind: 'span'; startMs: number; endMs: number } | { kind: 'no_active_time' };

export function activeSpan(timing: NodeTiming, drawAtMs: number): ActiveSpan {
  const first = timedSegments(timing).find(segment => segment.phase.kind !== 'pending');
  const end = nodeEnd(timing, drawAtMs);
  if (first === undefined || end.kind === 'no_timing') {
    return { kind: 'no_active_time' };
  }
  return { kind: 'span', startMs: segmentSpan(first, drawAtMs).startMs, endMs: end.endMs };
}

/** The time of a node without its dependency wait: the sum of its segments
 * that are not `pending`. This is the time the node needed after it could
 * start. A wall time holds the dependency wait, which is the place of the
 * node in the DAG. */
export type NodeTime =
  | { kind: 'measured'; ms: number; open: boolean }
  /** The node has dependency wait only: it waits, or it was skipped. */
  | { kind: 'not_started' }
  | { kind: 'no_timing' };

export function nodeTime(timing: NodeTiming, drawAtMs: number): NodeTime {
  switch (timing.kind) {
    case 'unavailable':
    case 'inconsistent':
      return { kind: 'no_timing' };
    case 'exact':
    case 'partial': {
      const own = timing.segments.filter(segment => segment.phase.kind !== 'pending');
      return own.length === 0
        ? { kind: 'not_started' }
        : {
            kind: 'measured',
            ms: activeMs(own, drawAtMs),
            open: own.some(segment => segment.end.kind === 'open'),
          };
    }
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The dependency wait of a node: the sum of its `pending` segments. */
export type DependencyWait =
  | { kind: 'waited'; ms: number; open: boolean }
  | { kind: 'none' };

export function dependencyWait(timing: NodeTiming, drawAtMs: number): DependencyWait {
  const waits = timedSegments(timing).filter(segment => segment.phase.kind === 'pending');
  if (waits.length === 0) {
    return { kind: 'none' };
  }
  let ms = 0;
  for (const segment of waits) {
    const span = segmentSpan(segment, drawAtMs);
    ms += Math.max(span.endMs - span.startMs, 0);
  }
  return { kind: 'waited', ms, open: waits.some(segment => segment.end.kind === 'open') };
}

/** The dependency of a node that ended last: the node it waited for. */
export type LastDependency = { kind: 'node'; node: WorkflowNode } | { kind: 'none' };

export function lastDependency(
  nodes: readonly WorkflowNode[],
  edges: readonly WorkflowEdge[],
  taskIndex: number,
  drawAtMs: number
): LastDependency {
  const timed = timedNodes(nodes, drawAtMs);
  let last: TimedNode | undefined;
  for (const index of dependenciesByNode(edges).get(taskIndex) ?? []) {
    const candidate = timed.get(index);
    if (candidate !== undefined && decides(candidate, last)) {
      last = candidate;
    }
  }
  return last === undefined ? { kind: 'none' } : { kind: 'node', node: last.node };
}

/** The node with the longest time without dependency wait. */
export type LongestNodeTime =
  | { kind: 'node'; node: WorkflowNode; ms: number }
  | { kind: 'no_timing' };

export function longestNodeTime(nodes: readonly WorkflowNode[], drawAtMs: number): LongestNodeTime {
  let longest: LongestNodeTime = { kind: 'no_timing' };
  for (const node of nodes) {
    const time = nodeTime(node.timing, drawAtMs);
    switch (time.kind) {
      case 'measured':
        if (longest.kind === 'no_timing' || time.ms > longest.ms) {
          longest = { kind: 'node', node, ms: time.ms };
        }
        break;
      case 'not_started':
      case 'no_timing':
        break;
      default:
        assertNever(time, 'node time');
    }
  }
  return longest;
}

/** One part of the wall time of a node: a phase of the node itself
 * (dependency wait, ready, the wait for a child start, a child run,
 * advancing), or its task: every segment of its tasks and attempts, retry
 * waits included. */
export type NodePart =
  | { kind: 'phase'; total: PhaseTotal }
  | { kind: 'task'; ms: number; open: boolean; tasks: number; attempts: number };

/** The parts of a node in the order they first appear; each phase of the
 * node is summed over its segments. */
export function nodeParts(segments: readonly TimingSegment[], drawAtMs: number): NodePart[] {
  const order: (TimingKind | 'task')[] = [];
  const nodeIntervals = new Map<TimingKind, PhaseInterval[]>();
  const tasks = new Set<number>();
  const attempts = new Set<string>();
  let taskMs = 0;
  let taskOpen = false;
  for (const segment of segments) {
    const interval = segmentInterval(segment, drawAtMs);
    const source = segment.source;
    switch (source.kind) {
      case 'node': {
        const list = nodeIntervals.get(interval.key);
        if (list === undefined) {
          order.push(interval.key);
          nodeIntervals.set(interval.key, [interval]);
        } else {
          list.push(interval);
        }
        break;
      }
      case 'task':
      case 'attempt':
        if (tasks.size === 0) {
          order.push('task');
        }
        tasks.add(source.generation);
        attempts.add(
          source.kind === 'attempt' ? `${source.generation}:${source.attempt}` : `${source.generation}:task`
        );
        taskMs += Math.max(interval.endMs - interval.startMs, 0);
        taskOpen = taskOpen || interval.open;
        break;
      default:
        assertNever(source, 'segment source');
    }
  }
  return order.flatMap((key): NodePart[] => {
    if (key === 'task') {
      return [{ kind: 'task', ms: taskMs, open: taskOpen, tasks: tasks.size, attempts: attempts.size }];
    }
    return phaseTotals(nodeIntervals.get(key) ?? []).map(total => ({ kind: 'phase', total }));
  });
}

/** The time a run was paused: the sum of its pause intervals. An open pause
 * runs to the draw time. */
export type PausedTime = { kind: 'paused'; ms: number; open: boolean } | { kind: 'not_paused' };

export function pausedTime(timing: ComposedRunTiming, drawAtMs: number): PausedTime {
  if (timing.pauses.length === 0) {
    return { kind: 'not_paused' };
  }
  let ms = 0;
  for (const pause of timing.pauses) {
    const span = pauseSpan(pause, drawAtMs);
    ms += Math.max(span.endMs - span.startMs, 0);
  }
  return { kind: 'paused', ms, open: timing.pauses.some(pause => pause.end.kind === 'open') };
}

/** How many times the task of a node ran: more than one task (a task was
 * replaced), one task with its attempt count (1 or more), or no task. */
export type NodeRunCount =
  | { kind: 'tasks'; count: number }
  | { kind: 'attempts'; count: number }
  | { kind: 'no_task' };

/** The run count of a node from the sources of its segments. One task with
 * no attempt row yet counts as one attempt. */
export function nodeRunCount(segments: readonly TimingSegment[]): NodeRunCount {
  const tasks = new Set<number>();
  const attempts = new Set<string>();
  for (const segment of segments) {
    const source = segment.source;
    switch (source.kind) {
      case 'node':
        break;
      case 'task':
        tasks.add(source.generation);
        break;
      case 'attempt':
        tasks.add(source.generation);
        attempts.add(`${source.generation}:${source.attempt}`);
        break;
      default:
        assertNever(source, 'segment source');
    }
  }
  if (tasks.size > 1) {
    return { kind: 'tasks', count: tasks.size };
  }
  return tasks.size === 0 ? { kind: 'no_task' } : { kind: 'attempts', count: Math.max(attempts.size, 1) };
}

/** A count of a node that the normal case does not have: more than one
 * attempt, or more than one task. */
export type NodeFact = { kind: 'attempts'; count: number } | { kind: 'tasks'; count: number };

/** The counts of a node that the normal case does not have, from its
 * segments: its task count (more than one), else its attempt count (more
 * than one). */
export function nodeFacts(segments: readonly TimingSegment[]): NodeFact[] {
  const count = nodeRunCount(segments);
  switch (count.kind) {
    case 'tasks':
      return [count];
    case 'attempts':
      return count.count > 1 ? [count] : [];
    case 'no_task':
      return [];
    default:
      return assertNever(count, 'node run count');
  }
}

/** The run count in words: `3 attempts`, `1 attempt`, `2 tasks`, `no task`. */
export function runCountText(count: NodeRunCount): string {
  switch (count.kind) {
    case 'tasks':
      return `${count.count} tasks`;
    case 'attempts':
      return count.count === 1 ? '1 attempt' : `${count.count} attempts`;
    case 'no_task':
      return 'no task';
    default:
      return assertNever(count, 'node run count');
  }
}
