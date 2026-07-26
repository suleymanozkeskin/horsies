import { describe, expect, it } from 'vitest';

import {
  entityOf,
  executingNodeCount,
  isTaskActionSettled,
  isWorkflowActionSettled,
  type SettleContext,
} from '@/actions/settle';
import type { TaskDetail } from '@/types/tasks';
import type { WorkflowNode, WorkflowRunDetail } from '@/types/workflows';

const leaf = (status: string, retryCount: number) => ({
  task_id: 't1',
  status,
  error_code: null,
  failed_reason: null,
  retry_count: retryCount,
  max_retries: 3,
  enqueued_at: null,
  started_at: null,
  completed_at: null,
  failed_at: null,
  queue_s: null,
  exec_s: null,
  worker_hostname: null,
  good_until: null,
});

const taskDetail = (status: string, retryCount = 0): TaskDetail => ({
  leaf: leaf(status, retryCount),
  task_name: 'demo',
  queue_name: 'default',
  priority: 0,
  is_workflow_task: false,
  error_category: null,
  attempts: [],
  workflow_id: null,
  workflow_task_index: null,
});

const node = (taskIndex: number, nodeStatus: string): WorkflowNode => ({
  task_index: taskIndex,
  node_id: null,
  task_name: 'demo',
  node_status: nodeStatus,
  is_subworkflow: false,
  sub_workflow_id: null,
  allow_failed_deps: false,
  started_at: null,
  completed_at: null,
  exec_s: null,
  child_total: null,
  child_failed: null,
});

const runDetail = (
  status: string,
  nodes: WorkflowNode[] = []
): WorkflowRunDetail => ({
  run: {
    id: 'w1',
    name: 'demo',
    definition_key: null,
    status,
    created_at: null,
    completed_at: null,
    wall_s: null,
  },
  nodes,
  edges: [],
  failed_count: 0,
  failed_indices: [],
});

const context = (overrides: Partial<SettleContext> = {}): SettleContext => ({
  wasStatus: null,
  retryCountBefore: null,
  ...overrides,
});

describe('task cancel settling', () => {
  it('settles as soon as the row reads CANCELLED', () => {
    expect(
      isTaskActionSettled('task-cancel', taskDetail('CANCELLED'), context())
    ).toBe(true);
  });

  it('does not settle while the row still reads RUNNING', () => {
    expect(
      isTaskActionSettled('task-cancel', taskDetail('RUNNING'), context())
    ).toBe(false);
  });
});

describe('task retry settling', () => {
  it('settles when the status moved away from the pre-action one', () => {
    expect(
      isTaskActionSettled(
        'task-retry',
        taskDetail('PENDING'),
        context({ wasStatus: 'FAILED', retryCountBefore: 0 })
      )
    ).toBe(true);
  });

  it('settles on a retry_count jump even when the status came back the same', () => {
    // A fast task can fail again into FAILED between two refetches; the reset
    // moved retry_count to MAX(attempt), which is what proves the retry landed.
    expect(
      isTaskActionSettled(
        'task-retry',
        taskDetail('FAILED', 2),
        context({ wasStatus: 'FAILED', retryCountBefore: 1 })
      )
    ).toBe(true);
  });

  it('does not settle when nothing observable changed', () => {
    expect(
      isTaskActionSettled(
        'task-retry',
        taskDetail('FAILED', 1),
        context({ wasStatus: 'FAILED', retryCountBefore: 1 })
      )
    ).toBe(false);
  });
});

describe('workflow settling', () => {
  it('pause settles at PAUSED, regardless of still-executing nodes', () => {
    expect(
      isWorkflowActionSettled(
        'workflow-pause',
        runDetail('PAUSED', [node(0, 'RUNNING')])
      )
    ).toBe(true);
  });

  it('resume settles at RUNNING', () => {
    expect(isWorkflowActionSettled('workflow-resume', runDetail('RUNNING'))).toBe(
      true
    );
    expect(isWorkflowActionSettled('workflow-resume', runDetail('PAUSED'))).toBe(
      false
    );
  });

  it('cancel needs CANCELLED and no schedulable nodes left', () => {
    expect(
      isWorkflowActionSettled(
        'workflow-cancel',
        runDetail('CANCELLED', [node(0, 'SKIPPED'), node(1, 'COMPLETED')])
      )
    ).toBe(true);
  });

  it.each(['PENDING', 'READY', 'ENQUEUED'])(
    'cancel does not settle while a node is still %s',
    status => {
      expect(
        isWorkflowActionSettled(
          'workflow-cancel',
          runDetail('CANCELLED', [node(0, status)])
        )
      ).toBe(false);
    }
  );

  it('cancel settles with draining nodes — draining is normal, not a failure', () => {
    expect(
      isWorkflowActionSettled(
        'workflow-cancel',
        runDetail('CANCELLED', [node(0, 'RUNNING')])
      )
    ).toBe(true);
  });
});

describe('draining count', () => {
  it('counts only nodes that are still executing', () => {
    expect(
      executingNodeCount(
        runDetail('CANCELLED', [
          node(0, 'RUNNING'),
          node(1, 'RUNNING'),
          node(2, 'SKIPPED'),
        ])
      )
    ).toBe(2);
  });
});

describe('entity routing', () => {
  it('routes each action to the surface that owns it', () => {
    expect(entityOf('task-cancel')).toBe('task');
    expect(entityOf('task-retry')).toBe('task');
    expect(entityOf('workflow-pause')).toBe('workflow');
    expect(entityOf('workflow-resume')).toBe('workflow');
    expect(entityOf('workflow-cancel')).toBe('workflow');
  });
});
