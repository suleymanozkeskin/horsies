use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, ItemFn};

mod codegen;
mod parse;
mod user_error_code;
mod workflow_input;

/// Define an async task.
///
/// ```ignore
/// #[horsies::task("validate_order", queue = "standard")]
/// async fn validate_order(order: Order) -> Result<ValidatedOrder, TaskError<OrderCode>> {
///     Ok(ValidatedOrder { ... })
/// }
/// ```
///
/// The function returns `Result<T, TaskError<E>>`: `T` is the output type
/// and `E` the code type of the task's own errors
/// (`#[derive(horsies::UserErrorCode)]`, or `std::convert::Infallible`).
/// Its parameters form the input, stored as Python stores keyword
/// arguments: no parameter, or one parameter written `()`, is `()` (a send
/// stores `{}`); any other parameters are a generated `Input` struct with
/// one field per parameter, keyed by the parameter's name. A single
/// parameter is wrapped whatever its type: `fn double(x: i64)` stores
/// `{"x": 2}`, and `fn validate_order(order: Order)` stores `{"order":
/// {...}}`. A parameter whose value is stored needs a name: `_` is refused
/// for it. A first parameter of type `horsies::TaskRuntime` is not input:
/// the body gets the runtime of the app.
///
/// Attributes after the name:
/// - `queue = "name"`: a queue of `Custom` queue mode.
/// - `execution = isolated | shared_async`: the class of the task; the
///   default is `isolated`. `shared_async` needs `timeout_ms`.
/// - `timeout_ms = 1000..=4294967295`: the deadline of a run.
/// - `cancel_grace_ms = 100..=10000`: `shared_async`: the time from the
///   cancel signal (at the deadline, or at an operator cancel) to the drop
///   of the body; default 1,000. `isolated`: the time from an operator
///   cancel to the kill of the child when the run has not ended; with none,
///   an operator cancel kills nothing.
/// - `retry_policy = <RetryPolicy>` with `auto_retry_for = ["CODE", ...]`:
///   both or neither. A code is a built-in code (`TASK_TIMEOUT`,
///   `WORKER_CRASHED`, ...) or a code of `E`; any other is a compile
///   error.
/// - `workflow_ctx`: the input gets the workflow context.
///
/// The attribute generates a module of the same name. It holds no state:
/// each send names the app that it goes through, and a workflow builder
/// resolves a node against its own app.
///
/// ```ignore
/// let validate = validate_order::register(&mut app)?;   // TaskFunction<validate_order::Input, ValidatedOrder, OrderCode>
/// let order_field = validate_order::params::order();    // InputField<validate_order::Input, Order>
/// let handle = validate_order::send(&app, order).await?;
/// let options = TaskSendOptions::new().delay(WholeSeconds::from_secs(30));
/// let later = validate_order::with_options(options).send(&app, order).await?;
/// let node = validate_order::node();                    // TaskNode<ValidatedOrder, validate_order::Input>
/// let function = validate_order::handle(&runtime)?;     // from a TaskRuntime
/// ```
///
/// A send through an app that does not register the task is
/// `VALIDATION_FAILED` and writes nothing.
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = parse_macro_input!(attr as parse::TaskAttrs);
    let func = parse_macro_input!(item as ItemFn);

    match codegen::generate_task(attrs, func, codegen::BodyKind::Async) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Define a blocking task: a `fn` that runs on a thread of its worker
/// child.
///
/// The same attributes and generated module as `#[horsies::task]`, with
/// `execution = isolated | shared_thread`. `shared_thread` needs
/// `timeout_ms` and takes no `cancel_grace_ms`: a thread cannot be stopped.
/// An `isolated` blocking task takes `cancel_grace_ms` as an async one
/// does.
///
/// ```ignore
/// #[horsies::blocking_task("resize", execution = shared_thread, timeout_ms = 60_000)]
/// fn resize(input: ResizeInput) -> Result<String, TaskError<ImageCode>> {
///     Ok(format!("{} at {}px", input.path, input.width))
/// }
/// ```
#[proc_macro_attribute]
pub fn blocking_task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = parse_macro_input!(attr as parse::TaskAttrs);
    let func = parse_macro_input!(item as ItemFn);

    match codegen::generate_task(attrs, func, codegen::BodyKind::Blocking) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive typed workflow field tokens for a named-field struct: the params
/// type of a workflow definition, which a sub-workflow node fills field by
/// field. A task's input fields have their tokens in the task's own module
/// (`task_name::params::<name>()`).
///
/// Generates one associated function `field_<name>()` per field that serde
/// reads, with the key that serde reads: `rename`, `rename_all` and raw
/// identifiers (`r#type` is `type`) are applied. A field with
/// `#[serde(skip)]` or `#[serde(skip_deserializing)]` gets no token. A
/// `#[serde(flatten)]` field is a compile error: its keys are those of its
/// own type.
///
/// ```ignore
/// #[derive(Serialize, Deserialize, horsies::WorkflowInput)]
/// struct RegionParams {
///     region: String,
/// }
///
/// RegionReport::node(SubWorkflowParams::NotGiven)?
///     .set(RegionParams::field_region(), "eu".to_owned())?;
/// ```
#[proc_macro_derive(WorkflowInput)]
pub fn derive_workflow_input(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match workflow_input::derive_workflow_input(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive the code type of a task's own errors: `horsies::UserErrorCode`.
///
/// The enum has unit variants only. Each variant's code is its name in
/// UPPER_SNAKE_CASE. A code with the name of a built-in or reserved code
/// (`TASK_TIMEOUT`, `WORKFLOW_STOPPED`, ...) is a compile error.
///
/// ```ignore
/// #[derive(Debug, Clone, PartialEq, horsies::UserErrorCode)]
/// enum ChargeCode {
///     CardDeclined, // CARD_DECLINED
///     RateLimited,  // RATE_LIMITED
/// }
/// ```
#[proc_macro_derive(UserErrorCode)]
pub fn derive_user_error_code(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match user_error_code::derive_user_error_code(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
