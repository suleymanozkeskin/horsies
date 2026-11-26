//! The supplier restock: read every supplier feed, then apply the stock
//! levels once a quorum of feeds completed. The feeds reach the update
//! through its workflow context. The workflow succeeds when the update
//! completed; a feed may fail.

use horsies::{
    HorsiesError, SuccessCase, SuccessPolicy, WorkflowDefConfig, WorkflowDefinition,
    WorkflowSpecBuilder,
};

use crate::domain::StockUpdate;
use crate::tasks::inventory::{sync_supplier_feed, update_stock_levels};
use crate::tuning;

/// The node id of the feed of `supplier`.
pub fn feed_node_id(supplier: &str) -> String {
    format!("feed_{}", supplier.replace('-', "_"))
}

/// The restock workflow. Its result is the stock update.
pub struct Restock;

impl WorkflowDefinition for Restock {
    type Output = StockUpdate;
    type Params = ();

    fn name() -> &'static str {
        "restock"
    }

    fn definition_key() -> &'static str {
        "acme.restock.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let mut feeds = Vec::new();
        let mut feed_ids = Vec::new();
        for supplier in tuning::SUPPLIERS {
            let node_id = feed_node_id(supplier);
            feeds.push(
                builder.task(sync_supplier_feed::node().node_id(node_id.clone()).set(
                    sync_supplier_feed::params::supplier(),
                    (*supplier).to_owned(),
                )?)?,
            );
            feed_ids.push(node_id);
        }
        let quorum = u32::try_from(tuning::RESTOCK_MIN_SUCCESSFUL_FEEDS.min(feeds.len()))
            .map_err(|_| HorsiesError::new("RESTOCK_MIN_SUCCESSFUL_FEEDS is above u32::MAX"))?;
        let update = builder.task(
            update_stock_levels::node()
                .node_id("update_stock_levels")
                .set(update_stock_levels::params::feed_node_ids(), feed_ids)?
                .waits_for_all(feeds.iter().copied())
                .workflow_ctx_from(feeds.iter().copied())
                .join_quorum(quorum),
        )?;
        let policy = SuccessPolicy::new(vec![SuccessCase::new([update])]).optional(feeds);
        Ok(WorkflowDefConfig::new()
            .output(update)
            .success_policy(policy))
    }
}
