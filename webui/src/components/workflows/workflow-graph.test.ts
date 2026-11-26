import { describe, expect, it } from 'vitest';

import { graphNodeFacts } from '@/components/workflows/workflow-graph';
import { EXACT_RETRIED_NODE, workflowNode } from '@/test/workflow-fixtures';
import type { WorkflowNode } from '@/types/workflows';

const node = (patch: Partial<WorkflowNode>): WorkflowNode => ({
  ...workflowNode(0, 'pick_pack', 'COMPLETED', EXACT_RETRIED_NODE),
  task_name: 'pick_pack',
  ...patch,
});

describe('graphNodeFacts', () => {
  it('has no fact for a plain task node whose label is its task name', () => {
    expect(graphNodeFacts(node({}), 'COMPLETED')).toEqual([]);
  });

  it('names the task of a node whose label is a node id', () => {
    expect(graphNodeFacts(node({ node_id: 'reserve_stock_1', task_name: 'reserve_stock' }), 'COMPLETED')).toEqual([
      'Task: reserve_stock.',
    ]);
  });

  it('says what the dashed border of a tolerant join is', () => {
    expect(graphNodeFacts(node({ allow_failed_dependencies: true }), 'COMPLETED')).toEqual([
      'Tolerant join (dashed border): the node runs even when a dependency failed.',
    ]);
  });

  it('says what the icon and the count of a sub-workflow node are, and how to open it', () => {
    expect(
      graphNodeFacts(
        node({ is_subworkflow: true, sub_workflow_id: 'child-1', child_total: 3, child_failed: 1 }),
        'COMPLETED'
      )
    ).toEqual(['Sub-workflow: 3 child nodes, 1 failed. Double-click to open it.']);
    // A child run that is not there yet has no count and nothing to open.
    expect(graphNodeFacts(node({ is_subworkflow: true }), 'RUNNING')).toEqual(['Sub-workflow.']);
  });

  it('says what the badge of a node that runs on under a cancelled run is', () => {
    const facts = graphNodeFacts(node({ node_status: 'RUNNING' }), 'CANCELLED');

    expect(facts).toHaveLength(1);
    expect(facts[0]).toContain('was executing when the workflow was cancelled');
  });
});
