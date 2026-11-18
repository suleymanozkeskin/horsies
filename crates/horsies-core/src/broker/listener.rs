//! The task queue NOTIFY. Every LISTEN goes through the broker's listener
//! (`process_listener`).

use uuid::Uuid;

use crate::broker::core_rows::StatementLog;
use crate::db::{
    CheckoutClass, CorePool, ParameterType, StatementBound, StatementFailure, TypedStatement,
};

/// The wake of a task queue: `$1` the channel `task_queue_<queue>`, `$2` the
/// task id as text.
static NOTIFY_TASK_QUEUE: TypedStatement = TypedStatement::new(
    "notify_task_queue",
    "SELECT pg_notify($1, $2)",
    &[ParameterType::Text, ParameterType::Text],
);

/// Send a task UUID as a text payload to its queue notification channel:
/// one statement in autocommit on `pool`, a per-task write
/// ([`CheckoutClass::Reserved`]). Writes nothing else.
///
/// Errors: the failed statement; nothing was sent, or the outcome is
/// unknown.
pub(crate) async fn notify_task_queue(
    pool: &CorePool,
    statement_log: StatementLog,
    queue_name: &str,
    task_id: Uuid,
) -> Result<(), StatementFailure> {
    let channel = format!("task_queue_{queue_name}");
    let payload = task_id.to_string();
    let statement = &NOTIFY_TASK_QUEUE;
    let bound = statement.bind().value(&channel).value(&payload).finish();
    statement_log.log(statement);
    let _notified = pool
        .execute(
            CheckoutClass::Reserved,
            &bound,
            StatementBound::ConnectionTimeouts,
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use uuid::Uuid;

    use super::notify_task_queue;
    use crate::broker::core_rows::StatementLog;
    use crate::test_support::sql::TestListener;

    #[tokio::test]
    async fn task_queue_notification_sends_uuid_as_text() {
        let pool = crate::test_support::migrated_pool().await;
        let core_pool = crate::broker::pools::core_pool_for_test_url(pool.url());
        let queue_name = format!("notify_{}", Uuid::new_v4().simple());
        let channel = format!("task_queue_{queue_name}");
        let task_id = Uuid::new_v4();
        let mut listener = TestListener::connect_with(&pool).await.unwrap();
        listener.listen(&channel).await.unwrap();

        notify_task_queue(&core_pool, StatementLog::Off, &queue_name, task_id)
            .await
            .unwrap();

        let notification = tokio::time::timeout(Duration::from_secs(2), listener.recv())
            .await
            .expect("task queue notification timeout")
            .unwrap();
        assert_eq!(notification.channel(), channel);
        assert_eq!(notification.payload(), task_id.to_string());
    }
}
