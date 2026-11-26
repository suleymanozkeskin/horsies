//! The fraud review: reconcile the disputed window, then refund the order.
//! A failed step pauses the workflow for an operator.

use horsies::{HorsiesError, OnError, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder};

use crate::domain::PaymentRefund;
use crate::tasks::payments::{reconcile_payments, refund_payment};

/// The disputed order of the definition.
pub const ORDER_ID: &str = "ACME-CHECK-0001";
/// The refunded amount.
pub const AMOUNT_CENTS: i32 = 4_900;

/// The fraud review workflow. Its result is the refund.
pub struct FraudReview;

impl WorkflowDefinition for FraudReview {
    type Output = PaymentRefund;
    type Params = ();

    fn name() -> &'static str {
        "fraud_review"
    }

    fn definition_key() -> &'static str {
        "acme.fraud_review.v1"
    }

    fn on_error() -> OnError {
        OnError::Pause
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let reconcile = builder.task(
            reconcile_payments::node()
                .node_id("reconcile_payments")
                .set(
                    reconcile_payments::fields::window(),
                    format!("dispute-{ORDER_ID}"),
                )?,
        )?;
        let refund = builder.task(
            refund_payment::node()
                .node_id("refund_payment")
                .set(refund_payment::fields::order_id(), ORDER_ID.to_owned())?
                .set(refund_payment::fields::amount_cents(), AMOUNT_CENTS)?
                .waits_for(reconcile),
        )?;
        Ok(WorkflowDefConfig::new().output(refund))
    }
}
