//! The daily report: the sales rollup, then the abandoned-cart sweep, then
//! the win-back send to the swept carts.

use horsies::{HorsiesError, WorkflowDefinition, WorkflowDefinitionConfig, WorkflowSpecBuilder};

use crate::domain::AbandonedCartSweep;
use crate::tasks::analytics::{abandoned_cart_sweep, sales_rollup};
use crate::tasks::notify::winback_blast;
use crate::tuning;

/// The window that the definition reports.
pub const WINDOW: &str = "CHECK";

/// The daily report workflow. Its result is the sweep.
pub struct DailyReport;

impl WorkflowDefinition for DailyReport {
    type Output = AbandonedCartSweep;
    type Parameters = ();

    fn name() -> &'static str {
        "daily_report"
    }

    fn definition_key() -> &'static str {
        "acme.daily_report.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefinitionConfig, HorsiesError> {
        let rollup = builder.task(
            sales_rollup::node()
                .node_id("sales_rollup")
                .set(sales_rollup::params::window(), WINDOW.to_owned())?,
        )?;
        let sweep = builder.task(
            abandoned_cart_sweep::node()
                .node_id("abandoned_cart_sweep")
                .set(
                    abandoned_cart_sweep::params::older_than_minutes(),
                    tuning::ABANDONED_CART_AGE_MINUTES,
                )?
                .waits_for(rollup),
        )?;
        let _blast = builder.task(
            winback_blast::node()
                .node_id("winback_blast")
                .set(
                    winback_blast::params::segment(),
                    format!("winback-{WINDOW}"),
                )?
                .arg_from(winback_blast::params::sweep(), sweep),
        )?;
        Ok(WorkflowDefinitionConfig::new().output(sweep))
    }
}
