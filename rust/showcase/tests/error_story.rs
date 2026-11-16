use std::env;
use std::sync::Arc;
use std::time::Duration;

use acme_showcase::app::build_story_app_for_url;
use acme_showcase::tasks::promotions::{LoyaltyArgs, PromotionArgs};
use acme_showcase::{simulate, tuning};
use horsies::{TaskResult, Worker, WorkerConfig};

fn id_for(rate: f64, label: &str, avoid: Option<(&str, f64)>) -> String {
    (0..100_000)
        .map(|index| format!("s2-db-{index}"))
        .find(|id| {
            simulate::draw(rate, &[id, label])
                && avoid.is_none_or(|(other_label, other_rate)| {
                    !simulate::draw(other_rate, &[id, other_label])
                })
        })
        .expect("seeded population contains the requested draw")
}

fn result_code<T>(result: TaskResult<T>) -> String {
    match result {
        TaskResult::Err(error) => error
            .error_code
            .expect("worker error has a code")
            .to_string(),
        TaskResult::Ok(_) => panic!("expected a task error"),
    }
}

#[tokio::test]
async fn three_error_codes_cross_the_public_worker_api() {
    let Ok(url) = env::var("ACME_DATABASE_URL") else {
        return;
    };

    let (app, handles) = build_story_app_for_url(&url).expect("story app");
    let (config, registry, workflow_registry, broker) =
        app.into_parts().await.expect("broker and migrations");
    let mut worker_config = WorkerConfig {
        queues: vec!["fulfillment".into(), "analytics".into()],
        concurrency: 2,
        ..WorkerConfig::default()
    };
    worker_config.apply_queue_config(&config);
    let worker = Worker::new(
        Arc::clone(&broker),
        Arc::new(registry),
        Arc::new(workflow_registry),
        config,
        worker_config,
    )
    .expect("worker");
    let stop = worker.cancel_token();
    let worker_join = tokio::spawn(async move { worker.run().await });

    let bundle_id = id_for(tuning::PROMOTION_BUNDLE_BUG_RATE, "bundle-bug", None);
    let size_id = id_for(
        tuning::PROMOTION_SIZE_CODE_BUG_RATE,
        "size-code",
        Some(("bundle-bug", tuning::PROMOTION_BUNDLE_BUG_RATE)),
    );
    let loyalty_id = id_for(tuning::LOYALTY_ENGINE_BUG_RATE, "lifetime-bug", None);

    let bundle = handles
        .apply
        .send(PromotionArgs {
            order_id: bundle_id,
        })
        .await
        .expect("enqueue bundle task");
    let size = handles
        .apply
        .send(PromotionArgs { order_id: size_id })
        .await
        .expect("enqueue size task");
    let loyalty = handles
        .loyalty
        .send(LoyaltyArgs {
            customer_id: loyalty_id,
            order_id: "story-order".into(),
        })
        .await
        .expect("enqueue loyalty task");

    assert_eq!(
        result_code(bundle.get(Some(Duration::from_secs(20))).await),
        "UNHANDLED_ERROR"
    );
    assert_eq!(
        result_code(size.get(Some(Duration::from_secs(20))).await),
        "DATA_CORRUPTION"
    );
    assert_eq!(
        result_code(loyalty.get(Some(Duration::from_secs(20))).await),
        "LOYALTY_ENGINE_BUG"
    );

    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), worker_join)
        .await
        .expect("worker stops");
}
