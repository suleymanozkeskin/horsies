//! Promotion and loyalty tasks, including the three-code error story.

use std::panic::{catch_unwind, AssertUnwindSafe};

use horsies::{async_task_fn, Horsies, HorsiesError, TaskError, TaskFunction};
use serde::{Deserialize, Serialize};

use crate::domain::{
    LoyaltyPoints, OrderLine, PromotionOutcome, DATA_CORRUPTION, LOYALTY_ENGINE_BUG,
};
use crate::{simulate, tuning};

use super::{fixed_options, register_json, QUEUE_ANALYTICS, QUEUE_FULFILLMENT};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionArgs {
    pub order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoyaltyArgs {
    pub customer_id: String,
    pub order_id: String,
}

pub struct StoryTaskHandles {
    pub apply: TaskFunction<PromotionArgs, PromotionOutcome>,
    pub loyalty: TaskFunction<LoyaltyArgs, LoyaltyPoints>,
}

/// A task-owned mapping for the corrupted size-code path.
pub fn map_size_code_error(size_code: &str) -> TaskError {
    TaskError::new(
        DATA_CORRUPTION,
        format!("unknown promotion size code {size_code}"),
    )
}

fn size_multiplier(size_code: &str) -> Result<i32, TaskError> {
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
/// units. The final division is intentionally left as the demo's real pricing
/// bug; the worker converts its panic into `UNHANDLED_ERROR`.
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

fn bundle_pricing_bug(order_id: &str) {
    if simulate::draw(tuning::PROMOTION_BUNDLE_BUG_RATE, &[order_id, "bundle-bug"]) {
        let lines = [OrderLine {
            line_no: 1,
            sku: "clearance-bundle".to_owned(),
            size_code: "M".to_owned(),
            quantity: tuning::BUNDLE_MIN_QUANTITY,
            unit_price_cents: tuning::CLEARANCE_PRICE_CENTS,
        }];
        let _ = bundle_discount_cents(&lines);
    }
}

pub async fn apply_promotions(args: PromotionArgs) -> Result<PromotionOutcome, TaskError> {
    bundle_pricing_bug(&args.order_id);
    if simulate::draw(
        tuning::PROMOTION_SIZE_CODE_BUG_RATE,
        &[&args.order_id, "size-code"],
    ) {
        return match size_multiplier(tuning::CORRUPT_SIZE_CODE) {
            Err(error) => Err(error),
            Ok(_) => Err(map_size_code_error(tuning::CORRUPT_SIZE_CODE)),
        };
    }
    Ok(PromotionOutcome {
        order_id: args.order_id,
        discount_cents: 0,
        applied_codes: vec![tuning::PROMOTION_CODES[0].to_owned()],
    })
}

pub async fn compute_loyalty_points(args: LoyaltyArgs) -> Result<LoyaltyPoints, TaskError> {
    let computation = catch_unwind(AssertUnwindSafe(|| {
        if simulate::draw(
            tuning::LOYALTY_ENGINE_BUG_RATE,
            &[&args.customer_id, "lifetime-bug"],
        ) {
            panic!("loyalty tier row is a bare label");
        }
        let points = simulate::integer(
            0,
            tuning::LOYALTY_LIFETIME_MAX as i64,
            &[&args.customer_id, "lifetime"],
        ) as i32
            * tuning::LOYALTY_POINTS_PER_EURO;
        LoyaltyPoints {
            customer_id: args.customer_id.clone(),
            order_id: args.order_id.clone(),
            points,
            tier: "standard".to_owned(),
        }
    }));
    computation.map_err(|panic| {
        let detail = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("loyalty engine panic");
        TaskError::new(LOYALTY_ENGINE_BUG, detail)
    })
}

pub const TASK_NAMES: &[&str] = &[
    "apply_promotions",
    "compute_loyalty_points",
    "publish_cdn",
    "publish_origin",
];

pub fn register_story(app: &mut Horsies) -> Result<StoryTaskHandles, HorsiesError> {
    let apply = app
        .task::<PromotionArgs, PromotionOutcome>(
            "apply_promotions",
            async_task_fn!(apply_promotions, PromotionArgs),
        )?
        .queue(QUEUE_FULFILLMENT)
        .task_options(fixed_options())
        .finish()?;
    let loyalty = app
        .task::<LoyaltyArgs, LoyaltyPoints>(
            "compute_loyalty_points",
            async_task_fn!(compute_loyalty_points, LoyaltyArgs),
        )?
        .queue(QUEUE_ANALYTICS)
        .task_options(fixed_options())
        .finish()?;
    Ok(StoryTaskHandles { apply, loyalty })
}

pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _ = register_story(app)?;
    register_json(app, "publish_cdn", QUEUE_FULFILLMENT, fixed_options())?;
    register_json(app, "publish_origin", QUEUE_FULFILLMENT, fixed_options())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id_for(rate: f64, label: &str) -> String {
        (0..100_000)
            .map(|index| format!("s2-{index}"))
            .find(|id| simulate::draw(rate, &[id, label]))
            .expect("seeded population contains a draw")
    }

    #[tokio::test]
    async fn size_mapping_is_data_corruption() {
        assert_eq!(
            map_size_code_error("XXL").error_code.unwrap().to_string(),
            DATA_CORRUPTION
        );
    }

    #[tokio::test]
    async fn promotion_size_draw_returns_data_corruption() {
        let order_id = (0..100_000)
            .map(|index| format!("s2-size-{index}"))
            .find(|id| {
                simulate::draw(tuning::PROMOTION_SIZE_CODE_BUG_RATE, &[id, "size-code"])
                    && !simulate::draw(tuning::PROMOTION_BUNDLE_BUG_RATE, &[id, "bundle-bug"])
            })
            .expect("seeded population contains a non-panicking size-code draw");
        let error = apply_promotions(PromotionArgs { order_id })
            .await
            .expect_err("size-code draw");
        assert_eq!(error.error_code.unwrap().to_string(), DATA_CORRUPTION);
    }

    #[tokio::test]
    async fn loyalty_panic_is_task_owned_loyalty_code() {
        let customer_id = id_for(tuning::LOYALTY_ENGINE_BUG_RATE, "lifetime-bug");
        let error = compute_loyalty_points(LoyaltyArgs {
            customer_id,
            order_id: "order".into(),
        })
        .await
        .expect_err("bug draw");
        assert_eq!(error.error_code.unwrap().to_string(), LOYALTY_ENGINE_BUG);
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
