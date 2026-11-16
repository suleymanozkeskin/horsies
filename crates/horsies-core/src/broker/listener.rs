//! The task queue NOTIFY. Every LISTEN goes through the broker's listener
//! (`process_listener`).

use sqlx::postgres::PgPool;

use crate::broker::error::BrokerError;

/// Send a task UUID as a text payload to its queue notification channel.
pub(crate) async fn notify_task_queue(
    pool: &PgPool,
    queue_name: &str,
    task_id: uuid::Uuid,
) -> Result<(), BrokerError> {
    let channel = format!("task_queue_{queue_name}");
    let payload = task_id.to_string();
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(channel)
        .bind(payload)
        .execute(pool)
        .await
        .map_err(BrokerError::Database)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqlx::postgres::PgListener;
    use uuid::Uuid;

    use super::notify_task_queue;

    #[tokio::test]
    async fn task_queue_notification_sends_uuid_as_text() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let queue_name = format!("notify_{}", Uuid::new_v4().simple());
        let channel = format!("task_queue_{queue_name}");
        let task_id = Uuid::new_v4();
        let mut listener = PgListener::connect_with(&pool).await.unwrap();
        listener.listen(&channel).await.unwrap();

        notify_task_queue(&pool, &queue_name, task_id)
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
