//! The price sync: publish a price to the CDN and the origin, then warm the
//! edge cache.

use horsies::{HorsiesError, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder};

use crate::domain::CacheWarm;
use crate::tasks::promotions::{publish_cdn, publish_origin, warm_cache_edge};

/// The campaign that the definition publishes.
pub const CAMPAIGN_ID: &str = "SYNC-CHECK";
/// The SKU that the definition publishes.
pub const SKU: &str = "ACME-SKU-0001";

/// The price sync workflow. Its result is the edge cache warm-up.
pub struct PriceSync;

impl WorkflowDefinition for PriceSync {
    type Output = CacheWarm;
    type Params = ();

    fn name() -> &'static str {
        "price_sync"
    }

    fn definition_key() -> &'static str {
        "acme.price_sync.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let cdn = builder.task(
            publish_cdn::node()?
                .node_id("publish_cdn")
                .set(publish_cdn::params::campaign_id(), CAMPAIGN_ID.to_owned())?
                .set(publish_cdn::params::sku(), SKU.to_owned())?,
        );
        let origin = builder.task(
            publish_origin::node()?
                .node_id("publish_origin")
                .set(
                    publish_origin::params::campaign_id(),
                    CAMPAIGN_ID.to_owned(),
                )?
                .set(publish_origin::params::sku(), SKU.to_owned())?,
        );
        let warm = builder.task(
            warm_cache_edge::node()?
                .node_id("warm_cache_edge")
                .set(
                    warm_cache_edge::params::campaign_id(),
                    CAMPAIGN_ID.to_owned(),
                )?
                .waits_for(cdn)
                .waits_for(origin),
        );
        Ok(WorkflowDefConfig::new().output(warm))
    }
}
