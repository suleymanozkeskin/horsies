//! The three error codes of the promotion story, through a worker of the
//! `acme` binary: a panic, a mapped size code, and a caught panic.

mod support;

use acme_showcase::app::build_app_for_url;
use acme_showcase::tasks::promotions::{apply_promotions, compute_loyalty_points};
use acme_showcase::{simulate, tuning};
use horsies::{ReadError, SchemaMode, TaskOutcome, UserErrorCode, WaitLimit};
use serial_test::serial;

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

/// The code of a read that must give a failed task.
fn failed_code<T, E>(read: Result<TaskOutcome<T, E>, ReadError>) -> String
where
    T: std::fmt::Debug,
    E: UserErrorCode,
{
    match read {
        Ok(TaskOutcome::Failed(error)) => error.code().text(),
        other => panic!("expected a task error, got {other:?}"),
    }
}

#[tokio::test]
#[serial]
async fn three_error_codes_cross_the_worker() {
    let Some((settings, _store)) = support::database().await else {
        return;
    };
    let showcase = build_app_for_url(settings.sqlx_url(), SchemaMode::MigrateAndValidate)
        .expect("the showcase app");
    let _worker = support::WorkerProcess::start(&showcase).await;

    let bundle_id = id_for(tuning::PROMOTION_BUNDLE_BUG_RATE, "bundle-bug", None);
    let size_id = id_for(
        tuning::PROMOTION_SIZE_CODE_BUG_RATE,
        "size-code",
        Some(("bundle-bug", tuning::PROMOTION_BUNDLE_BUG_RATE)),
    );
    let loyalty_id = id_for(tuning::LOYALTY_ENGINE_BUG_RATE, "lifetime-bug", None);

    let bundle = showcase
        .tasks
        .apply_promotions
        .send(apply_promotions::Input {
            order_id: bundle_id,
        })
        .await
        .expect("send the bundle task");
    let size = showcase
        .tasks
        .apply_promotions
        .send(apply_promotions::Input { order_id: size_id })
        .await
        .expect("send the size task");
    let loyalty = showcase
        .tasks
        .compute_loyalty_points
        .send(compute_loyalty_points::Input {
            customer_id: loyalty_id,
            order_id: "story-order".to_owned(),
        })
        .await
        .expect("send the loyalty task");

    let limit = WaitLimit::parse(Some(support::RESULT_WAIT)).expect("the wait limit");
    assert_eq!(failed_code(bundle.get(limit).await), "UNHANDLED_EXCEPTION");
    assert_eq!(failed_code(size.get(limit).await), "DATA_CORRUPTION");
    assert_eq!(failed_code(loyalty.get(limit).await), "LOYALTY_ENGINE_BUG");
}
