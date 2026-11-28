//! The Acme workflow definitions.
//!
//! Three definitions take params and are registered as templates
//! (`order_fulfillment`, `shipping`, `returns_review`); each has a check
//! case, so `check()` builds it. The other nine build one fixed spec each.

use horsies::{
    Horsies, HorsiesError, NoWorkflowOutput, WorkflowDefinition, WorkflowFunction, WorkflowTemplate,
};

use crate::domain::{
    AbandonedCartSweep, CacheWarm, MarketingBlast, Order, PaymentCapture, PaymentRefund,
    RestockDecision, SalesRollup, StockRelease, StockUpdate, TrackingSeed,
};

pub mod catalog_import;
pub mod customer_winback;
pub mod daily_report;
pub mod flash_sale;
pub mod fraud_review;
pub mod order_fulfillment;
pub mod price_sync;
pub mod restock;
pub mod returns_review;
pub mod seasonal_markdown;
pub mod shipping;
pub mod warehouse_transfer;

/// The name of every Acme workflow.
pub const ALL_WORKFLOW_NAMES: &[&str] = &[
    "order_fulfillment",
    "shipping",
    "returns_review",
    "catalog_import",
    "customer_winback",
    "daily_report",
    "flash_sale",
    "fraud_review",
    "price_sync",
    "restock",
    "seasonal_markdown",
    "warehouse_transfer",
];

/// The registered workflows, for the scenarios that start them.
#[derive(Clone)]
pub struct RegisteredWorkflows {
    pub order_fulfillment: WorkflowTemplate<Order, PaymentCapture>,
    pub shipping: WorkflowTemplate<shipping::ShippingParams, TrackingSeed>,
    pub returns_review: WorkflowTemplate<returns_review::ReturnsParams, RestockDecision>,
    pub catalog_import: WorkflowFunction<NoWorkflowOutput>,
    pub customer_winback: WorkflowFunction<MarketingBlast>,
    pub daily_report: WorkflowFunction<AbandonedCartSweep>,
    pub flash_sale: WorkflowFunction<CacheWarm>,
    pub fraud_review: WorkflowFunction<PaymentRefund>,
    pub price_sync: WorkflowFunction<CacheWarm>,
    pub restock: WorkflowFunction<StockUpdate>,
    pub seasonal_markdown: WorkflowFunction<SalesRollup>,
    pub warehouse_transfer: WorkflowFunction<StockRelease>,
}

impl RegisteredWorkflows {
    /// The name of each registered workflow, in field order.
    pub fn names(&self) -> Vec<&str> {
        vec![
            self.order_fulfillment.name(),
            self.shipping.name(),
            self.returns_review.name(),
            self.catalog_import.name(),
            self.customer_winback.name(),
            self.daily_report.name(),
            self.flash_sale.name(),
            self.fraud_review.name(),
            self.price_sync.name(),
            self.restock.name(),
            self.seasonal_markdown.name(),
            self.warehouse_transfer.name(),
        ]
    }
}

/// Register every Acme workflow on `app`, after the tasks: the definitions
/// build their nodes from the registered tasks. The shipping template comes
/// before the order template, whose sub-workflow node it is.
///
/// Errors: the first registration refusal.
pub fn register_all(app: &mut Horsies) -> Result<RegisteredWorkflows, HorsiesError> {
    let shipping = app.register_workflow_definition_with::<shipping::Shipping>()?;
    app.check_workflow_builder(
        shipping::Shipping::name(),
        |builder, params: &shipping::ShippingParams| {
            shipping::Shipping::build_with(builder, params.clone())
        },
    )
    .case(shipping::check_params())
    .register()?;

    let order_fulfillment =
        app.register_workflow_definition_with::<order_fulfillment::OrderFulfillment>()?;
    app.check_workflow_builder(
        order_fulfillment::OrderFulfillment::name(),
        |builder, order: &Order| {
            order_fulfillment::OrderFulfillment::build_with(builder, order.clone())
        },
    )
    .case(order_fulfillment::check_order())
    .register()?;

    let returns_review =
        app.register_workflow_definition_with::<returns_review::ReturnsReview>()?;
    app.check_workflow_builder(
        returns_review::ReturnsReview::name(),
        |builder, params: &returns_review::ReturnsParams| {
            returns_review::ReturnsReview::build_with(builder, params.clone())
        },
    )
    .case(returns_review::check_params())
    .register()?;

    Ok(RegisteredWorkflows {
        order_fulfillment,
        shipping,
        returns_review,
        catalog_import: app.register_workflow_definition::<catalog_import::CatalogImport>()?,
        customer_winback: app
            .register_workflow_definition::<customer_winback::CustomerWinback>()?,
        daily_report: app.register_workflow_definition::<daily_report::DailyReport>()?,
        flash_sale: app.register_workflow_definition::<flash_sale::FlashSale>()?,
        fraud_review: app.register_workflow_definition::<fraud_review::FraudReview>()?,
        price_sync: app.register_workflow_definition::<price_sync::PriceSync>()?,
        restock: app.register_workflow_definition::<restock::Restock>()?,
        seasonal_markdown: app
            .register_workflow_definition::<seasonal_markdown::SeasonalMarkdown>()?,
        warehouse_transfer: app
            .register_workflow_definition::<warehouse_transfer::WarehouseTransfer>()?,
    })
}
