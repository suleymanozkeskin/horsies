//! The seasonal markdown: mark each SKU down, then roll the sales of the
//! campaign up.

use horsies::{HorsiesError, WorkflowDefinition, WorkflowDefinitionConfig, WorkflowSpecBuilder};

use crate::domain::SalesRollup;
use crate::tasks::analytics::sales_rollup;
use crate::tasks::promotions::update_price;

/// The campaign that the definition marks down.
pub const CAMPAIGN_ID: &str = "MARKDOWN-CHECK";
/// The SKUs that the definition marks down: `ACME-SKU-0001` to this one.
pub const LAST_SKU: usize = 6;

/// The seasonal markdown workflow. Its result is the rollup.
pub struct SeasonalMarkdown;

impl WorkflowDefinition for SeasonalMarkdown {
    type Output = SalesRollup;
    type Parameters = ();

    fn name() -> &'static str {
        "seasonal_markdown"
    }

    fn definition_key() -> &'static str {
        "acme.seasonal_markdown.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefinitionConfig, HorsiesError> {
        let mut markdowns = Vec::new();
        for (index, sku_number) in (1..=LAST_SKU).enumerate() {
            markdowns.push(
                builder.task(
                    update_price::node()
                        .node_id(format!("update_price_{index:02}"))
                        .set(
                            update_price::fields::sku(),
                            format!("ACME-SKU-{sku_number:04}"),
                        )?
                        .set(update_price::fields::campaign_id(), CAMPAIGN_ID.to_owned())?,
                )?,
            );
        }
        let rollup = builder.task(
            sales_rollup::node()
                .node_id("sales_rollup")
                .set(
                    sales_rollup::fields::window(),
                    format!("markdown-{CAMPAIGN_ID}"),
                )?
                .waits_for_all(markdowns),
        )?;
        Ok(WorkflowDefinitionConfig::new().output(rollup))
    }
}
