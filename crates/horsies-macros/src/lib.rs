use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, ItemFn};

mod code_catalog;
mod codegen;
mod manifest_schema;
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
/// arguments: none is `()`; one struct is the input itself, its fields the
/// keys; several parameters, or one that is not a struct (`fn double(x:
/// i64)`), are a generated `Input` struct keyed by the parameter names
/// (`{"x": 2}`). The attribute reads the type's name: a primitive, a string,
/// a sequence, a set, a map, an `Option`, a tuple, an array, a JSON value, a
/// UUID, a date or time, a `TaskOutcome` and a `WorkflowContext` are
/// wrapped; a path to any other type is taken as a struct. A first
/// parameter of type `horsies::TaskRuntime` is not input: the body gets the
/// runtime of the app.
///
/// Attributes after the name:
/// - `queue = "name"`: a queue of `Custom` queue mode.
/// - `execution = isolated | shared_async`: the class of the task; the
///   default is `isolated`. `shared_async` needs `timeout_ms`.
/// - `timeout_ms = 1000..=4294967295`: the deadline of a run.
/// - `cancel_grace_ms = 100..=10000`: `shared_async` only; the time from
///   the cancel signal at the deadline to the drop of the body. Default
///   1,000.
/// - `retry_policy = <RetryPolicy>` with `auto_retry_for = ["CODE", ...]`:
///   both or neither. A code is a built-in code (`TASK_TIMEOUT`,
///   `WORKER_CRASHED`, ...) or a code of `E`; any other is a compile
///   error.
/// - `workflow_ctx`: the input gets the workflow context.
///
/// The attribute generates a module of the same name:
///
/// ```ignore
/// let validate = validate_order::register(&mut app)?;   // TaskFunction<Order, ValidatedOrder, OrderCode>
/// let handle = validate_order::send(order).await?;      // after the first register in the process
/// let later = validate_order::schedule(WholeSeconds::from_secs(30), order).await?;
/// let function = validate_order::handle(&runtime)?;     // from a TaskRuntime
/// ```
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
/// `timeout_ms` and takes no `cancel_grace_ms`: a thread cannot be
/// cancelled.
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

/// Derive typed workflow field tokens for a named-field input struct.
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
/// struct ProcessInput {
///     data: String,
/// }
///
/// process_data::node()?.arg_from(ProcessInput::field_data(), fetch);
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

/// Derive the static catalog data of a closed code enum of horsies-core:
/// its members, their wire values and their docs. `horsies-codegen` renders
/// the Python enum classes from it.
///
/// ```ignore
/// /// Categorized broker operation failure codes.
/// #[derive(horsies_macros::CodeCatalog)]
/// #[code_catalog(module = "broker", base = "str_enum")]
/// pub enum BrokerErrorCode {
///     /// Schema initialization failed.
///     SchemaInitFailed,
/// }
/// ```
///
/// Emits `impl horsies_core::catalog::CodeEnum for BrokerErrorCode`.
///
/// A lifecycle status marks its terminal variants with
/// `#[code_catalog(terminal)]` and names their Python constant with
/// `#[code_catalog(terminal_states = "...")]` on the enum.
#[proc_macro_derive(CodeCatalog, attributes(code_catalog))]
pub fn derive_code_catalog(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match code_catalog::derive_code_catalog(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive the Python form of a manifest type, read by the Python code
/// generator.
///
/// A struct of named fields is a frozen dataclass; an enum with a serde
/// `tag` is one dataclass per variant and a union alias; an enum of unit
/// variants is a `Literal` alias. Each field type implements
/// `horsies_core::manifest_schema::ManifestValue`.
///
/// ```ignore
/// /// Where the session pool connects.
/// #[derive(serde::Deserialize, horsies_macros::ManifestSchema)]
/// #[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
/// pub enum SessionDatabase {
///     SameAsDatabaseUrl,
///     Separate { url: DatabaseUrl },
/// }
/// ```
///
/// Emits `impl horsies_core::manifest_schema::ManifestValue` and
/// `impl horsies_core::manifest_schema::ManifestItem` for the type.
#[proc_macro_derive(ManifestSchema)]
pub fn derive_manifest_schema(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match manifest_schema::derive_manifest_schema(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
