//! The read of the workflow node of one task by its id: the workflow id,
//! the node index and the root workflow.
//!
//! The span of an attempt that the worker ended is made in the handler
//! child from the `AttemptEnded` event. The worker does not know the
//! workflow node of a workflow task: the start write of the child reads it
//! and returns it to the child only. Such an attempt is rare (a timeout
//! kill, a blocked-loop kill, a lost child), so the handler child reads the
//! node with this one statement, only for an attempt of a workflow task
//! that the worker ended. The claim and the start pay nothing for it.
//!
//! The read is bounded: the wait for a pooled connection is at most
//! [`TASK_WORKFLOW_NODE_CHECKOUT_WAIT`], and the statement gets its cancel
//! request after [`TASK_WORKFLOW_NODE_STATEMENT_BOUND`]. A statement that is
//! cancelled ends within the cancel request bound and the cancel grace of
//! the statement layer ([`TASK_WORKFLOW_NODE_READ_END`]).

use std::time::Duration;

use uuid::Uuid;

use crate::broker::claims::{WorkflowNodeIndex, MAX_WORKFLOW_NODE_INDEX};
use crate::broker::core_rows::{column, StatementLog};
use crate::broker::error::BrokerError;
use crate::broker::postgres::PostgresBroker;
use crate::db::{
    CheckoutClass, CorePool, NotSentCause, ParameterType, Row, StatementBound, StatementFailure,
    TypedStatement, CANCEL_GRACE, CANCEL_REQUEST_BOUND,
};

/// The longest wait of the read for a pooled connection.
pub const TASK_WORKFLOW_NODE_CHECKOUT_WAIT: Duration = Duration::from_secs(1);

/// The longest run of the statement of the read before its cancel request.
pub const TASK_WORKFLOW_NODE_STATEMENT_BOUND: Duration = Duration::from_secs(1);

/// The longest time of the read: the wait for a connection, the statement,
/// and the end of a statement cancelled at its bound (the cancel request and
/// the wait for its reply).
pub const TASK_WORKFLOW_NODE_READ_END: Duration = TASK_WORKFLOW_NODE_CHECKOUT_WAIT
    .saturating_add(TASK_WORKFLOW_NODE_STATEMENT_BOUND)
    .saturating_add(CANCEL_REQUEST_BOUND)
    .saturating_add(CANCEL_GRACE);

/// The node that links task `$1` (`ix_horsies_workflow_tasks_task_id`, one
/// row at most), with its workflow by primary key: the workflow id, the node
/// index, and the root of the tree, `coalesce(root_workflow_id, id)` as the
/// start write reads it. Reads only; locks nothing.
static TASK_WORKFLOW_NODE: TypedStatement = TypedStatement::new(
    "task_workflow_node",
    "SELECT n.workflow_id, n.task_index, \
            coalesce(w.root_workflow_id, w.id) AS root_workflow_id \
     FROM horsies_workflow_tasks n \
     JOIN horsies_workflows w ON w.id = n.workflow_id \
     WHERE n.task_id = $1",
    &[ParameterType::Uuid],
);

/// What the read found for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskWorkflowNode {
    /// The node at `node_index` of the workflow `workflow_id` links the
    /// task. `root_workflow_id` is the top-level workflow of its tree: the
    /// workflow itself for a top-level workflow.
    Linked {
        workflow_id: Uuid,
        node_index: WorkflowNodeIndex,
        root_workflow_id: Uuid,
    },
    /// No workflow node links the task now: a plain task, or a node whose
    /// workflow was deleted.
    NotLinked,
}

impl PostgresBroker {
    /// The workflow node that links the task `task_id`
    /// ([`TaskWorkflowNode`]): one statement on the main core pool, checked
    /// out as batch work, within [`TASK_WORKFLOW_NODE_READ_END`] (see the
    /// module docs). Reads only; installs no schema.
    ///
    /// Errors: [`BrokerError::PoolSettings`] (nothing sent);
    /// [`BrokerError::Statement`]: no connection within
    /// [`TASK_WORKFLOW_NODE_CHECKOUT_WAIT`] (nothing sent), a refusal or a
    /// cancel at [`TASK_WORKFLOW_NODE_STATEMENT_BOUND`]; transient when its
    /// cause may clear. [`BrokerError::RowDecode`] and
    /// [`BrokerError::ClaimContract`] for rows outside the statement's
    /// contract (two rows, a node index outside 0 to
    /// [`MAX_WORKFLOW_NODE_INDEX`]).
    pub async fn read_task_workflow_node(
        &self,
        task_id: Uuid,
    ) -> Result<TaskWorkflowNode, BrokerError> {
        let open = self.core_pools_for_read()?;
        read_task_workflow_node(open.main(), self.statement_log(), task_id).await
    }
}

/// [`PostgresBroker::read_task_workflow_node`] on `pool`.
pub(crate) async fn read_task_workflow_node(
    pool: &CorePool,
    log: StatementLog,
    task_id: Uuid,
) -> Result<TaskWorkflowNode, BrokerError> {
    let statement = &TASK_WORKFLOW_NODE;
    let bound = statement.bind().value(&task_id).finish();
    log.log(statement);
    let mut connection = pool
        .checkout_within(CheckoutClass::Batch, TASK_WORKFLOW_NODE_CHECKOUT_WAIT)
        .await
        .map_err(|cause| {
            BrokerError::Statement(StatementFailure::NotSent {
                statement: statement.name(),
                cause: NotSentCause::Checkout(cause),
            })
        })?;
    let rows = connection
        .query(
            &bound,
            StatementBound::CancelAfter(TASK_WORKFLOW_NODE_STATEMENT_BOUND),
        )
        .await?;
    let wire = rows
        .iter()
        .map(node_columns)
        .collect::<Result<Vec<_>, BrokerError>>()?;
    decode_task_workflow_node(&wire).map_err(BrokerError::ClaimContract)
}

/// The columns of one row: the workflow id, the stored node index, the
/// root workflow id.
fn node_columns(row: &Row) -> Result<(Uuid, i32, Uuid), BrokerError> {
    let statement = &TASK_WORKFLOW_NODE;
    Ok((
        column::<Uuid>(row, statement, "workflow_id")?,
        column::<i32>(row, statement, "task_index")?,
        column::<Uuid>(row, statement, "root_workflow_id")?,
    ))
}

/// The node of the rows of the read. Pure.
///
/// Errors: the contract breach as text: more than one row (the task id of a
/// node is unique), or a node index outside 0 to
/// [`MAX_WORKFLOW_NODE_INDEX`] (a workflow start writes each index from a
/// spec of at most `MAX_WORKFLOW_NODES` nodes).
pub(crate) fn decode_task_workflow_node(
    rows: &[(Uuid, i32, Uuid)],
) -> Result<TaskWorkflowNode, String> {
    match rows {
        [] => Ok(TaskWorkflowNode::NotLinked),
        [(workflow_id, stored_index, root_workflow_id)] => {
            let node_index = u32::try_from(*stored_index)
                .ok()
                .and_then(|index| WorkflowNodeIndex::try_from(index).ok())
                .ok_or_else(|| {
                    format!(
                        "task_workflow_node read node index {stored_index}, outside 0 to \
                         {MAX_WORKFLOW_NODE_INDEX}"
                    )
                })?;
            Ok(TaskWorkflowNode::Linked {
                workflow_id: *workflow_id,
                node_index,
                root_workflow_id: *root_workflow_id,
            })
        }
        more => Err(format!(
            "task_workflow_node read {} rows for one task; the task id of a node is unique",
            more.len()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(value: u32) -> WorkflowNodeIndex {
        WorkflowNodeIndex::try_from(value).unwrap()
    }

    #[test]
    fn no_row_is_not_linked_and_one_row_is_its_node() {
        let workflow_id = Uuid::from_u128(0xf007);
        let root_workflow_id = Uuid::from_u128(0xf001);
        assert_eq!(
            decode_task_workflow_node(&[]),
            Ok(TaskWorkflowNode::NotLinked)
        );
        for stored in [0, 7, MAX_WORKFLOW_NODE_INDEX] {
            assert_eq!(
                decode_task_workflow_node(&[(
                    workflow_id,
                    i32::try_from(stored).unwrap(),
                    root_workflow_id
                )]),
                Ok(TaskWorkflowNode::Linked {
                    workflow_id,
                    node_index: index(stored),
                    root_workflow_id,
                })
            );
        }
    }

    #[test]
    fn two_rows_or_an_index_outside_its_bound_break_the_contract() {
        let row = (Uuid::from_u128(1), 0, Uuid::from_u128(1));
        assert!(decode_task_workflow_node(&[row, row]).is_err());
        let over = i32::try_from(MAX_WORKFLOW_NODE_INDEX + 1).unwrap();
        for stored in [-1, over] {
            assert!(
                decode_task_workflow_node(&[(Uuid::from_u128(1), stored, Uuid::from_u128(1))])
                    .is_err(),
                "{stored}"
            );
        }
    }

    #[test]
    fn the_read_ends_within_its_bounds() {
        assert_eq!(TASK_WORKFLOW_NODE_READ_END, Duration::from_secs(12));
    }

    /// No server listens on port 1: the read fails with nothing sent,
    /// within the checkout wait.
    #[tokio::test]
    async fn a_read_with_no_server_fails_with_nothing_sent_within_its_wait() {
        let pool = crate::test_support::sql::TestPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies");
        let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
        let started = std::time::Instant::now();

        let read = broker.read_task_workflow_node(Uuid::from_u128(7)).await;

        assert!(
            matches!(
                read,
                Err(BrokerError::Statement(StatementFailure::NotSent { .. }))
            ),
            "{read:?}"
        );
        assert!(started.elapsed() < TASK_WORKFLOW_NODE_READ_END);
    }
}
