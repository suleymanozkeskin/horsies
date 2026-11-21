//! The win-back workflow: sweep the abandoned carts, then send the
//! win-back segment to them.

use horsies::{HorsiesError, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder};

use crate::domain::MarketingBlast;
use crate::tasks::analytics::abandoned_cart_sweep;
use crate::tasks::notify::winback_blast;
use crate::tuning;

/// The segment that the definition sends to.
pub const SEGMENT: &str = "CHECK-SEGMENT";

/// The win-back workflow. Its result is the send.
pub struct CustomerWinback;

impl WorkflowDefinition for CustomerWinback {
    type Output = MarketingBlast;
    type Params = ();

    fn name() -> &'static str {
        "customer_winback"
    }

    fn definition_key() -> &'static str {
        "acme.customer_winback.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let sweep = builder.task(
            abandoned_cart_sweep::node()?
                .node_id("abandoned_cart_sweep")
                .set(
                    abandoned_cart_sweep::params::older_than_minutes(),
                    tuning::ABANDONED_CART_AGE_MINUTES,
                )?,
        );
        let blast = builder.task(
            winback_blast::node()?
                .node_id("winback_blast")
                .set(winback_blast::params::segment(), SEGMENT.to_owned())?
                .arg_from(winback_blast::params::sweep(), sweep),
        );
        Ok(WorkflowDefConfig::new().output(blast))
    }
}
