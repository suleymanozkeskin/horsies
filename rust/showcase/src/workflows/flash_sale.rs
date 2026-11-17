//! The flash sale: publish the price to the CDN and the origin, warm the
//! search index, and warm the edge cache as soon as one publish completed.
//! The sale succeeds when either publish completed; the search warm-up may
//! fail.

use horsies::{
    HorsiesError, SuccessCase, SuccessPolicy, WorkflowDefConfig, WorkflowDefinition,
    WorkflowSpecBuilder,
};

use crate::domain::CacheWarm;
use crate::tasks::promotions::{prewarm_search, publish_cdn, publish_origin, warm_cache_edge};

/// The campaign that the definition publishes.
pub const CAMPAIGN_ID: &str = "FLASH-CHECK";
/// The SKU that the definition publishes.
pub const SKU: &str = "ACME-SKU-0001";

/// The flash sale workflow. Its result is the edge cache warm-up.
pub struct FlashSale;

impl WorkflowDefinition for FlashSale {
    type Output = CacheWarm;
    type Params = ();

    fn name() -> &'static str {
        "flash_sale"
    }

    fn definition_key() -> &'static str {
        "acme.flash_sale.v1"
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
        let search = builder.task(prewarm_search::node()?.node_id("prewarm_search").set(
            prewarm_search::params::campaign_id(),
            CAMPAIGN_ID.to_owned(),
        )?);
        let warm = builder.task(
            warm_cache_edge::node()?
                .node_id("warm_cache_edge")
                .set(
                    warm_cache_edge::params::campaign_id(),
                    CAMPAIGN_ID.to_owned(),
                )?
                .waits_for(cdn)
                .waits_for(origin)
                .join_any(),
        );
        let policy = SuccessPolicy::new(vec![SuccessCase::new([cdn]), SuccessCase::new([origin])])
            .optional([search]);
        Ok(WorkflowDefConfig::new().output(warm).success_policy(policy))
    }
}
