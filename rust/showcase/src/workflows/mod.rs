//! Acme workflow definitions and scheduler-facing registration.

use std::collections::HashMap;

use horsies::{
    AnyNode, Horsies, HorsiesError, OnError, SubWorkflowNode, SuccessCase, SuccessPolicy, TaskNode,
    WorkflowFunction, WorkflowSpec, WorkflowSpecBuilder, WorkflowTemplate,
};
use serde_json::Value;

use crate::tasks::{JsonTask, TaskHandles};

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

/// Handles used to build workflow nodes through the public task API.
#[derive(Clone)]
pub struct WorkflowTasks {
    handles: TaskHandles,
}

/// Registered workflow entry points retained by the demo-owned command layer.
#[derive(Clone)]
pub struct RegisteredWorkflows {
    pub order_fulfillment: WorkflowTemplate<crate::domain::Order, Value>,
    pub static_specs: HashMap<String, WorkflowFunction<Value>>,
}

impl WorkflowTasks {
    pub fn new(handles: TaskHandles) -> Self {
        Self { handles }
    }

    pub fn node(
        &self,
        task_name: &str,
        node_id: &str,
        kwargs: serde_json::Value,
    ) -> Result<TaskNode<Value, Value>, HorsiesError> {
        let handle = self.handles.get(task_name).ok_or_else(|| {
            HorsiesError::new(format!("workflow task '{}' is not registered", task_name))
        })?;
        Ok(handle
            .node()
            .kwargs_json(serde_json::to_string(&kwargs).expect("JSON values serialize"))
            .node_id(node_id))
    }

    pub fn child(
        &self,
        workflow_name: &str,
        node_id: &str,
        kwargs: serde_json::Value,
    ) -> SubWorkflowNode<Value, Value> {
        SubWorkflowNode::<Value, Value>::typed(workflow_name)
            .kwargs_json(serde_json::to_string(&kwargs).expect("JSON values serialize"))
            .node_id(node_id)
            .queue(crate::tasks::QUEUE_FULFILLMENT)
    }

    pub fn named(&self, task_name: &str) -> Option<&JsonTask> {
        self.handles.get(task_name)
    }
}

pub(crate) fn builder(name: &str, definition_key: &str) -> WorkflowSpecBuilder {
    let mut builder = WorkflowSpecBuilder::new(name);
    builder
        .definition_key(definition_key)
        .on_error(OnError::Fail);
    builder
}

pub(crate) fn finish(
    builder: WorkflowSpecBuilder,
    output_node_id: Option<&str>,
    links: &[(String, String, String)],
    context_nodes: &[(String, Vec<String>)],
    policy: Option<SuccessPolicy>,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut spec = builder.build()?;
    if let Some(node_id) = output_node_id {
        spec.output_index = Some(find_node(&spec.tasks, node_id)?);
    }
    for (dependent, field, source) in links {
        let source_index = find_node(&spec.tasks, source)?;
        let dependent_index = find_node(&spec.tasks, dependent)?;
        let node = &mut spec.tasks[dependent_index];
        node.args_from.insert(field.clone(), source_index);
        if !node.dependencies.contains(&source_index) {
            node.dependencies.push(source_index);
        }
    }
    for (node_id, sources) in context_nodes {
        let index = find_node(&spec.tasks, node_id)?;
        spec.tasks[index].workflow_ctx_from = Some(sources.clone());
    }
    spec.success_policy = policy;
    Ok(spec)
}

pub(crate) fn find_node(nodes: &[AnyNode], node_id: &str) -> Result<usize, HorsiesError> {
    nodes
        .iter()
        .position(|node| node.node_id.as_deref() == Some(node_id))
        .ok_or_else(|| HorsiesError::new(format!("workflow node '{}' is missing", node_id)))
}

pub(crate) fn success_policy(
    required: &[&str],
    optional: &[&str],
    nodes: &[AnyNode],
) -> Result<SuccessPolicy, HorsiesError> {
    let required_indices = required
        .iter()
        .map(|id| find_node(nodes, id))
        .collect::<Result<Vec<_>, _>>()?;
    let optional_indices = optional
        .iter()
        .map(|id| find_node(nodes, id))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SuccessPolicy {
        cases: vec![SuccessCase {
            required_indices,
            name: None,
        }],
        optional_indices: (!optional_indices.is_empty()).then_some(optional_indices),
    })
}

/// Register representative workflow specs and dynamic builders for `check()`.
pub fn register_all(
    app: &mut Horsies,
    handles: TaskHandles,
) -> Result<RegisteredWorkflows, HorsiesError> {
    let tasks = WorkflowTasks::new(handles);

    let shipping_tasks = tasks.clone();
    let _shipping_template = app
        .register_parameterized_workflow::<shipping::ShippingParams, Value, _>(
            "shipping",
            "acme.shipping.v1",
            move |params| {
                shipping::build(
                    &shipping_tasks,
                    &params.order_id,
                    &params.courier,
                    params.express,
                )
            },
        )?;
    let shipping_check_tasks = tasks.clone();
    let mut shipping_builder =
        app.check_workflow_builder("shipping", move |params: &shipping::ShippingParams| {
            shipping::build(
                &shipping_check_tasks,
                &params.order_id,
                &params.courier,
                params.express,
            )
        })?;
    shipping_builder.case(shipping::ShippingParams {
        order_id: "CHECK-ORDER".to_owned(),
        courier: "atlas".to_owned(),
        express: false,
    });
    shipping_builder.register()?;
    let order_tasks = tasks.clone();
    let order_template = app.register_parameterized_workflow::<crate::domain::Order, Value, _>(
        "order_fulfillment",
        "acme.order_fulfillment.v1",
        move |order| order_fulfillment::build(&order_tasks, &order),
    )?;
    let mut static_specs = HashMap::new();
    let catalog_import = app.register_workflow_spec::<Value>(catalog_import::build(
        &tasks,
        "CHECK-IMPORT",
        crate::tuning::CATALOG_IMPORT_CHUNKS,
    )?)?;
    static_specs.insert(catalog_import.name().to_owned(), catalog_import);
    let customer_winback = app.register_workflow_spec::<Value>(customer_winback::build(
        &tasks,
        "CHECK-SEGMENT",
        crate::tuning::ABANDONED_CART_AGE_MINUTES,
    )?)?;
    static_specs.insert(customer_winback.name().to_owned(), customer_winback);
    let daily_report = app.register_workflow_spec::<Value>(daily_report::build(
        &tasks,
        "CHECK",
        crate::tuning::ABANDONED_CART_AGE_MINUTES,
    )?)?;
    static_specs.insert(daily_report.name().to_owned(), daily_report);
    let flash_sale = app.register_workflow_spec::<Value>(flash_sale::build(
        &tasks,
        "FLASH-CHECK",
        "ACME-SKU-0001",
    )?)?;
    static_specs.insert(flash_sale.name().to_owned(), flash_sale);
    let fraud_review = app.register_workflow_spec::<Value>(fraud_review::build(
        &tasks,
        "ACME-CHECK-0001",
        4_900,
    )?)?;
    static_specs.insert(fraud_review.name().to_owned(), fraud_review);
    let price_sync = app.register_workflow_spec::<Value>(price_sync::build(
        &tasks,
        "SYNC-CHECK",
        "ACME-SKU-0001",
    )?)?;
    static_specs.insert(price_sync.name().to_owned(), price_sync);
    let restock = app.register_workflow_spec::<Value>(restock::build(
        &tasks,
        crate::tuning::SUPPLIERS
            .iter()
            .map(|supplier| (*supplier).to_owned())
            .collect(),
    )?)?;
    static_specs.insert(restock.name().to_owned(), restock);
    let returns_review = app.register_workflow_spec::<Value>(returns_review::build(
        &tasks,
        "RET-CHECK",
        "ACME-CHECK-0001",
        "ACME-SKU-0001",
        1,
    )?)?;
    static_specs.insert(returns_review.name().to_owned(), returns_review);
    let seasonal_markdown = app.register_workflow_spec::<Value>(seasonal_markdown::build(
        &tasks,
        "MARKDOWN-CHECK",
        (1..=6)
            .map(|index| format!("ACME-SKU-{index:04}"))
            .collect(),
    )?)?;
    static_specs.insert(seasonal_markdown.name().to_owned(), seasonal_markdown);
    let warehouse_transfer = app.register_workflow_spec::<Value>(warehouse_transfer::build(
        &tasks,
        "ACME-SKU-0001",
        5,
    )?)?;
    static_specs.insert(warehouse_transfer.name().to_owned(), warehouse_transfer);

    let check_tasks = tasks.clone();
    let mut order_builder = app
        .check_workflow_builder("order_fulfillment", move |order: &crate::domain::Order| {
            order_fulfillment::build(&check_tasks, order)
        })?;
    order_builder.case(order_fulfillment::check_order());
    order_builder.register()?;

    Ok(RegisteredWorkflows {
        order_fulfillment: order_template,
        static_specs,
    })
}

pub fn build_order(
    handles: TaskHandles,
    order: crate::domain::Order,
) -> Result<WorkflowSpec, HorsiesError> {
    order_fulfillment::build(&WorkflowTasks::new(handles), &order)
}
