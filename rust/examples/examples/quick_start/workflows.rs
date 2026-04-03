//! Workflow definitions for the shipping example.

use horsies::{
    Horsies, HorsiesError, OnError, TaskNode, WorkflowDefConfig, WorkflowDefinition,
    WorkflowFunction, WorkflowSpecBuilder,
};

use super::models::*;

/// Reusable order-processing workflow definition.
///
/// The workflow DAG:
///
/// ```text
///   validate_order
///       /    |    \
///      v     v     v
///  inventory cost  address
///       \    |    /
///        v   v   v
///      reserve_inventory
///            |
///            v
///      create_shipment
///            |
///            v
///      send_notification
/// ```
pub struct OrderProcessingWorkflow;

impl WorkflowDefinition for OrderProcessingWorkflow {
    type Output = NotificationResult;
    type Params = ();

    fn name() -> &'static str {
        "order_processing"
    }

    fn definition_key() -> &'static str {
        "quickstart.order_processing.v1"
    }

    fn on_error() -> OnError {
        OnError::Fail
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        // Root task — validates the order
        let validate =
            builder.task(TaskNode::<ValidatedOrder>::new("validate_order").node_id("validate"));

        // Fan-out: three parallel checks, all depend on validate
        let inventory = builder.task(
            TaskNode::<InventoryStatus>::new("check_inventory")
                .node_id("inventory")
                .waits_for(validate)
                .args_from("order", validate),
        );
        let cost = builder.task(
            TaskNode::<ShippingCost>::new("calculate_shipping_cost")
                .node_id("shipping_cost")
                .waits_for(validate)
                .args_from("order", validate),
        );
        let address = builder.task(
            TaskNode::<AddressValidation>::new("check_address")
                .node_id("address")
                .waits_for(validate)
                .args_from("order", validate),
        );

        // Fan-in: reserve waits for all three parallel checks
        let reserve = builder.task(
            TaskNode::<Reservation>::new("reserve_inventory")
                .node_id("reserve")
                .waits_for(inventory)
                .waits_for(cost)
                .waits_for(address)
                .args_from("inventory", inventory)
                .args_from("cost", cost)
                .args_from("address", address),
        );

        // Sequential: shipment then notification
        let shipment = builder.task(
            TaskNode::<Shipment>::new("create_shipment")
                .node_id("shipment")
                .waits_for(reserve)
                .args_from("reservation", reserve),
        );
        let notify = builder.task(
            TaskNode::<NotificationResult>::new("send_notification")
                .node_id("notify")
                .waits_for(shipment)
                .args_from("shipment", shipment),
        );

        Ok(WorkflowDefConfig::new().output(notify))
    }
}

/// Register the reusable order-processing workflow on the app.
pub fn register(app: &mut Horsies) -> Result<WorkflowFunction<NotificationResult>, HorsiesError> {
    app.register_workflow_definition::<OrderProcessingWorkflow>()
}
