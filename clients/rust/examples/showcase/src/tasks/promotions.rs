//! Promotion, loyalty and price tasks, including the three-code error story:
//!
//! - a bundle pricing bug panics, and the worker child records the panic as
//!   `UNHANDLED_EXCEPTION`;
//! - a corrupt size code maps to `DATA_CORRUPTION` in the task's own code;
//! - the loyalty engine panics inside a `catch_unwind` that the task owns,
//!   which returns `LOYALTY_ENGINE_BUG`.

use std::panic::{catch_unwind, AssertUnwindSafe};

use horsies::{task, Horsies, HorsiesError, TaskError, TaskFunction, TaskRuntime};

use crate::domain::{
    AcmeCode, CacheWarm, LoyaltyPoints, OrderLine, PricePush, PriceUpdate, PromotionOutcome,
    SearchPrewarm,
};
use crate::{simulate, tuning};

use super::{runtime, task_store};

/// The functions of the promotion tasks that the scenarios send through.
pub struct PromotionTasks {
    pub apply_promotions: TaskFunction<apply_promotions::Input, PromotionOutcome, AcmeCode>,
    pub compute_loyalty_points:
        TaskFunction<compute_loyalty_points::Input, LoyaltyPoints, AcmeCode>,
    pub update_price: TaskFunction<update_price::Input, PriceUpdate, AcmeCode>,
}

/// The task error of a size code that no size has.
pub fn map_size_code_error(size_code: &str) -> TaskError<AcmeCode> {
    TaskError::custom(
        AcmeCode::DataCorruption,
        format!("unknown promotion size code {size_code}"),
    )
    .with_data(serde_json::json!({ "size_code": size_code }))
}

/// The price multiplier of a size code, in percent.
fn size_multiplier(size_code: &str) -> Result<i32, TaskError<AcmeCode>> {
    match size_code {
        "XS" => Ok(90),
        "S" => Ok(95),
        "M" => Ok(100),
        "L" => Ok(105),
        "XL" => Ok(110),
        other => Err(map_size_code_error(other)),
    }
}

/// Compute the bundle discount from order lines.
///
/// A bundled order whose lines are all below the price floor has no earning
/// units. The final division is the demo's pricing bug: it panics on a zero
/// divisor, and the worker child records the panic as `UNHANDLED_EXCEPTION`.
pub fn bundle_discount_cents(lines: &[OrderLine]) -> i32 {
    let bundled = lines
        .iter()
        .filter(|line| line.quantity >= tuning::BUNDLE_MIN_QUANTITY)
        .collect::<Vec<_>>();
    if bundled.is_empty() {
        return 0;
    }
    let pot_cents = bundled
        .iter()
        .map(|line| line.line_total_cents())
        .sum::<i32>()
        / tuning::BUNDLE_POT_DIVISOR;
    let earning_units = bundled
        .iter()
        .filter(|line| line.unit_price_cents >= tuning::BUNDLE_PRICE_FLOOR_CENTS)
        .map(|line| line.quantity)
        .sum::<i32>();
    pot_cents / earning_units
}

/// Price a clearance bundle for the orders that the bug rate draws: the
/// division of [`bundle_discount_cents`] panics for them.
fn bundle_pricing_bug(order_id: &str) {
    if simulate::draw(tuning::PROMOTION_BUNDLE_BUG_RATE, &[order_id, "bundle-bug"]) {
        let lines = [OrderLine {
            line_no: 1,
            sku: "clearance-bundle".to_owned(),
            size_code: "M".to_owned(),
            quantity: tuning::BUNDLE_MIN_QUANTITY,
            unit_price_cents: tuning::CLEARANCE_PRICE_CENTS,
        }];
        let _discount = bundle_discount_cents(&lines);
    }
}

/// The promotions of `order_id`.
///
/// Errors: `DATA_CORRUPTION` for the orders that the size-code bug rate
/// draws. Panics for the orders that the bundle bug rate draws.
pub fn promotion_outcome(order_id: &str) -> Result<PromotionOutcome, TaskError<AcmeCode>> {
    bundle_pricing_bug(order_id);
    if simulate::draw(
        tuning::PROMOTION_SIZE_CODE_BUG_RATE,
        &[order_id, "size-code"],
    ) {
        return Err(match size_multiplier(tuning::CORRUPT_SIZE_CODE) {
            Err(error) => error,
            Ok(_multiplier) => map_size_code_error(tuning::CORRUPT_SIZE_CODE),
        });
    }
    Ok(PromotionOutcome {
        order_id: order_id.to_owned(),
        discount_cents: 0,
        applied_codes: vec![tuning::PROMOTION_CODES[0].to_owned()],
    })
}

/// The loyalty points that `order_id` earns `customer_id`.
///
/// Errors: `LOYALTY_ENGINE_BUG` for the customers that the engine bug rate
/// draws: the engine panics, and this function catches the panic.
pub fn loyalty_points(
    customer_id: &str,
    order_id: &str,
) -> Result<LoyaltyPoints, TaskError<AcmeCode>> {
    let computation = catch_unwind(AssertUnwindSafe(|| {
        if simulate::draw(
            tuning::LOYALTY_ENGINE_BUG_RATE,
            &[customer_id, "lifetime-bug"],
        ) {
            panic!("loyalty tier row is a bare label");
        }
        let lifetime = simulate::integer(
            0,
            i64::from(tuning::LOYALTY_LIFETIME_MAX),
            &[customer_id, "lifetime"],
        );
        LoyaltyPoints {
            customer_id: customer_id.to_owned(),
            order_id: order_id.to_owned(),
            points: i32::try_from(lifetime).unwrap_or(i32::MAX) * tuning::LOYALTY_POINTS_PER_EURO,
            tier: "standard".to_owned(),
        }
    }));
    computation.map_err(|panic| {
        let detail = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("loyalty engine panic");
        TaskError::custom(AcmeCode::LoyaltyEngineBug, detail)
    })
}

/// The list price of `sku` in the demo catalog's range.
fn list_price_cents(sku: &str) -> i32 {
    let price = simulate::integer(
        i64::from(tuning::MIN_PRICE_CENTS),
        i64::from(tuning::MAX_PRICE_CENTS),
        &[sku, "price"],
    );
    i32::try_from(price).unwrap_or(tuning::MAX_PRICE_CENTS)
}

/// Apply the promotions of an order. A refused promotion fails the order
/// and releases its reservations.
#[task(
    "apply_promotions",
    queue = "fulfillment",
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn apply_promotions(
    rt: TaskRuntime,
    order_id: String,
) -> Result<PromotionOutcome, TaskError<AcmeCode>> {
    let refused = match promotion_outcome(&order_id) {
        Ok(outcome) => return Ok(outcome),
        Err(refused) => refused,
    };
    match task_store(&rt).await {
        Ok(store) => runtime::compensate_failed_order(&store, &order_id).await,
        Err(unavailable) => {
            eprintln!("order {order_id} was not compensated: {unavailable}");
        }
    }
    Err(refused)
}

/// Compute the loyalty points of an order.
#[task(
    "compute_loyalty_points",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn compute_loyalty_points(
    customer_id: String,
    order_id: String,
) -> Result<LoyaltyPoints, TaskError<AcmeCode>> {
    loyalty_points(&customer_id, &order_id)
}

/// Push a campaign price to the CDN.
#[task(
    "publish_cdn",
    queue = "fulfillment",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn publish_cdn(
    campaign_id: String,
    sku: String,
) -> Result<PricePush, TaskError<AcmeCode>> {
    Ok(PricePush {
        price_cents: list_price_cents(&sku),
        sku,
        target: format!("cdn:{campaign_id}"),
    })
}

/// Push a campaign price to the origin.
#[task(
    "publish_origin",
    queue = "fulfillment",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn publish_origin(
    campaign_id: String,
    sku: String,
) -> Result<PricePush, TaskError<AcmeCode>> {
    Ok(PricePush {
        price_cents: list_price_cents(&sku),
        sku,
        target: format!("origin:{campaign_id}"),
    })
}

/// Warm the search index of a campaign.
#[task(
    "prewarm_search",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn prewarm_search(campaign_id: String) -> Result<SearchPrewarm, TaskError<AcmeCode>> {
    let documents = simulate::integer(100, 5_000, &[&campaign_id, "documents"]);
    Ok(SearchPrewarm {
        documents: i32::try_from(documents).unwrap_or(i32::MAX),
        index_name: format!("products-{campaign_id}"),
    })
}

/// Warm the edge cache of a campaign.
#[task(
    "warm_cache_edge",
    queue = "fulfillment",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn warm_cache_edge(campaign_id: String) -> Result<CacheWarm, TaskError<AcmeCode>> {
    let keys_warmed = simulate::integer(50, 2_000, &[&campaign_id, "keys"]);
    Ok(CacheWarm {
        target: campaign_id,
        keys_warmed: i32::try_from(keys_warmed).unwrap_or(i32::MAX),
    })
}

/// Mark a SKU down by the flash-sale discount.
#[task(
    "update_price",
    queue = "analytics",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = crate::tasks::crash_retry(),
    auto_retry_for = ["WORKER_CRASHED"]
)]
pub async fn update_price(
    sku: String,
    campaign_id: String,
) -> Result<PriceUpdate, TaskError<AcmeCode>> {
    let was_cents = list_price_cents(&sku);
    let discount = match campaign_id.is_empty() {
        true => 0,
        false => tuning::FLASH_SALE_DISCOUNT_PERCENT,
    };
    Ok(PriceUpdate {
        sku,
        was_cents,
        now_cents: was_cents * (100 - discount) / 100,
    })
}

/// Register the promotion tasks on `app`.
///
/// Errors: the first registration refusal.
pub fn register(app: &mut Horsies) -> Result<PromotionTasks, HorsiesError> {
    let apply_promotions = apply_promotions::register(app)?;
    let compute_loyalty_points = compute_loyalty_points::register(app)?;
    let _publish_cdn = publish_cdn::register(app)?;
    let _publish_origin = publish_origin::register(app)?;
    let _prewarm_search = prewarm_search::register(app)?;
    let _warm_cache_edge = warm_cache_edge::register(app)?;
    let update_price = update_price::register(app)?;
    Ok(PromotionTasks {
        apply_promotions,
        compute_loyalty_points,
        update_price,
    })
}

#[cfg(test)]
mod tests {
    use horsies::TaskErrorCode;

    use super::*;

    fn id_for(rate: f64, label: &str) -> String {
        (0..100_000)
            .map(|index| format!("s2-{index}"))
            .find(|id| simulate::draw(rate, &[id, label]))
            .expect("seeded population contains a draw")
    }

    #[test]
    fn size_mapping_is_data_corruption() {
        assert_eq!(
            map_size_code_error("XXL").code(),
            &TaskErrorCode::Custom(AcmeCode::DataCorruption)
        );
    }

    #[test]
    fn promotion_size_draw_returns_data_corruption() {
        let order_id = (0..100_000)
            .map(|index| format!("s2-size-{index}"))
            .find(|id| {
                simulate::draw(tuning::PROMOTION_SIZE_CODE_BUG_RATE, &[id, "size-code"])
                    && !simulate::draw(tuning::PROMOTION_BUNDLE_BUG_RATE, &[id, "bundle-bug"])
            })
            .expect("seeded population contains a non-panicking size-code draw");
        let error = promotion_outcome(&order_id).expect_err("size-code draw");
        assert_eq!(
            error.code(),
            &TaskErrorCode::Custom(AcmeCode::DataCorruption)
        );
    }

    #[test]
    fn loyalty_panic_is_task_owned_loyalty_code() {
        let customer_id = id_for(tuning::LOYALTY_ENGINE_BUG_RATE, "lifetime-bug");
        let error = loyalty_points(&customer_id, "order").expect_err("bug draw");
        assert_eq!(
            error.code(),
            &TaskErrorCode::Custom(AcmeCode::LoyaltyEngineBug)
        );
    }

    #[test]
    fn bundle_bug_is_a_real_panic() {
        let id = id_for(tuning::PROMOTION_BUNDLE_BUG_RATE, "bundle-bug");
        let panic = std::panic::catch_unwind(|| bundle_pricing_bug(&id));
        assert!(panic.is_err());
    }

    #[test]
    fn bundle_discount_panics_only_when_no_units_earn_a_share() {
        let safe = [OrderLine {
            line_no: 1,
            sku: "sku".into(),
            size_code: "M".into(),
            quantity: 2,
            unit_price_cents: tuning::BUNDLE_PRICE_FLOOR_CENTS,
        }];
        assert_eq!(
            bundle_discount_cents(&safe),
            2 * tuning::BUNDLE_PRICE_FLOOR_CENTS / 10 / 2
        );
        let bug = [OrderLine {
            unit_price_cents: tuning::CLEARANCE_PRICE_CENTS,
            ..safe[0].clone()
        }];
        assert!(std::panic::catch_unwind(|| bundle_discount_cents(&bug)).is_err());
    }
}
