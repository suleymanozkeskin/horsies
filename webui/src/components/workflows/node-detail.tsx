import { Layers } from 'lucide-react';

import { RESIDUAL_COPY } from '@/actions/copy';
import { AttemptPanel } from '@/components/monitoring/attempt-panel';
import { Hint } from '@/components/ui/hint';
import { StatusChip } from '@/components/ui/status-chip';
import { useMeta } from '@/hooks/use-meta';
import { useTask } from '@/hooks/use-task';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { useWorkflowTask } from '@/hooks/use-workflow-task';
import { assertNever } from '@/lib/exhaustive';
import type { TaskRow, WorkflowNode, WorkflowNodeDetail } from '@/types/workflows';

import { ChainBadge, NodeTimingSection } from './node-timing';
import { residualState } from './residual';
import { chainMembership, longestChain, type ChainMembership } from './run-timing';

interface NodeDetailContentProps {
  workflowId: string;
  /** Status of the run this node belongs to — decides the residual badge. */
  runStatus: string;
  node: WorkflowNode;
  onOpenSubworkflow: (subWorkflowId: string, label: string) => void;
}

/** What the node panel shows of the task of a node: the attempt panel of its
 * task, a line for a node with no task yet, or nothing for a sub-workflow
 * node (its child run is its work) and a skipped node (it never has an
 * attempt). */
type NodeTaskView =
  | { kind: 'task'; leaf: TaskRow }
  | { kind: 'no_task' }
  | { kind: 'none' };

function nodeTaskView(node: WorkflowNode, detail: WorkflowNodeDetail | undefined): NodeTaskView {
  if (detail === undefined || detail.is_sub_workflow) {
    return { kind: 'none' };
  }
  if (node.node_status === 'SKIPPED' && detail.attempts.length === 0) {
    return { kind: 'none' };
  }
  return detail.task_row === null ? { kind: 'no_task' } : { kind: 'task', leaf: detail.task_row };
}

/** Whether the node is on the longest chain of its run, from the run read
 * that the graph and the timeline share; or `not_loaded` before that read. */
type ChainRead = { kind: 'loaded'; membership: ChainMembership } | { kind: 'not_loaded' };

function useChainMembership(workflowId: string, taskIndex: number): ChainRead {
  const { detail, readClock } = useWorkflowRun(workflowId);
  return detail === undefined || readClock === undefined
    ? { kind: 'not_loaded' }
    : {
        kind: 'loaded',
        membership: chainMembership(longestChain(detail.nodes, detail.edges, readClock.asOfMs), taskIndex),
      };
}

/** The state of a task read that holds no task. */
function taskReadText(isError: boolean, isLoading: boolean): string {
  if (isError) {
    return 'Could not load the task of this node.';
  }
  return isLoading ? 'Loading…' : 'Task not found.';
}

/** The attempt panel of the task of a node, from the task read (it holds the
 * queue, the priority and the error category that the node read does not). */
function NodeTask({ taskId }: { taskId: string }) {
  const { detail, isLoading, isError } = useTask(taskId);
  const { meta } = useMeta();
  if (detail === undefined) {
    return <p className="m-0 text-xs text-muted-foreground">{taskReadText(isError, isLoading)}</p>;
  }
  return <AttemptPanel detail={detail} traceTemplate={meta?.trace_link_template ?? null} />;
}

/** Detail body for one node: its status and its marks, the one node timing
 * summary, the node error, and the attempt panel of its task. Chrome-less —
 * the surrounding panel owns the header/close/resize and the inline padding.
 * A node error that an attempt shows as its message is not shown twice. */
export function NodeDetailContent({
  workflowId,
  runStatus,
  node,
  onOpenSubworkflow,
}: NodeDetailContentProps) {
  const label = node.node_id ?? node.task_name ?? `#${node.node_index}`;
  const drillId = node.is_sub_workflow ? node.sub_workflow_id : null;
  const { detail } = useWorkflowTask(workflowId, node.node_index);
  const chainRead = useChainMembership(workflowId, node.node_index);

  const leaf = detail?.task_row ?? null;
  const nodeError = detail?.node_error;
  const residual = residualState(runStatus, node.node_status, leaf?.status ?? null);
  const repeatsAttemptMessage =
    detail?.attempts.some(
      attempt => (attempt.error_message ?? attempt.failed_reason) === nodeError
    ) ?? false;
  const task = nodeTaskView(node, detail);

  return (
    <div className="flex flex-col gap-4 py-4">
      <div className="flex flex-col gap-1.5">
        <span className="inline-flex flex-wrap items-center gap-2">
          <StatusChip status={node.node_status} />
          {chainRead.kind === 'loaded' && <ChainBadge membership={chainRead.membership} />}
          {residual !== null && (
            <span className="rounded-full border border-hairline-strong px-2 py-0.5 text-10 uppercase text-muted-foreground">
              {RESIDUAL_COPY[residual].badge}
            </span>
          )}
          {node.allow_failed_deps && (
            <Hint text="Tolerant join: the node runs even when a dependency failed. The graph draws it with a dashed border.">
              <span className="rounded-full border border-dashed border-hairline-strong px-2 py-0.5 text-10 text-muted-foreground">
                tolerant join
              </span>
            </Hint>
          )}
          {task.kind !== 'task' && (
            <Hint text={`The task of the node: ${node.task_name}.`} className="min-w-0">
              <span className="block min-w-0 break-all font-mono text-xs text-muted-foreground">
                {node.task_name}
              </span>
            </Hint>
          )}
        </span>
        {residual !== null && (
          <p className="text-xs text-muted-foreground">{RESIDUAL_COPY[residual].tooltip}</p>
        )}
      </div>

      <NodeTimingSection workflowId={workflowId} taskIndex={node.node_index} />

      {drillId && (
        <button
          type="button"
          onClick={() => onOpenSubworkflow(drillId, label)}
          className="inline-flex items-center justify-center gap-2 rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary-hover"
        >
          <Layers className="size-4" />
          Open subworkflow
        </button>
      )}

      {nodeError && !repeatsAttemptMessage && (
        <div>
          <span className="text-xs uppercase tracking-wide text-muted-foreground">
            Node error
          </span>
          <pre className="mt-1 max-h-40 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-glass-inset p-2 font-mono text-11 leading-snug">
            {nodeError}
          </pre>
        </div>
      )}

      <NodeTaskSection task={task} />
    </div>
  );
}

function NodeTaskSection({ task }: { task: NodeTaskView }) {
  switch (task.kind) {
    case 'task':
      return (
        <div className="border-t border-hairline pt-4">
          <NodeTask taskId={task.leaf.task_id} />
        </div>
      );
    case 'no_task':
      return <p className="m-0 text-xs text-muted-foreground">no attempt recorded</p>;
    case 'none':
      return null;
    default:
      return assertNever(task, 'node task view');
  }
}
