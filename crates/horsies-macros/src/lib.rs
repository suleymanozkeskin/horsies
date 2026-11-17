use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, ItemFn};

mod code_catalog;
mod codegen;
mod manifest_schema;
mod parse;
mod user_error_code;
mod workflow_input;

/// Attribute macro for defining an async horsies task.
///
/// # Example
///
/// ```ignore
/// #[horsies::task("validate_order", queue = "standard")]
/// async fn validate_order(order: Order) -> Result<ValidatedOrder, TaskError> {
///     Ok(ValidatedOrder { ... })
/// }
/// ```
///
/// Tasks that need to start a dynamic workflow at runtime may inject
/// `horsies::TaskRuntime` as the first parameter:
///
/// ```ignore
/// #[horsies::task("scrape_detail")]
/// async fn scrape_detail(
///     rt: horsies::TaskRuntime,
///     input: ScrapeInput,
/// ) -> Result<(), TaskError> {
///     if let Some(spec) = build_enrichment_spec(&input)? {
///         rt.start::<()>(spec).await?;
///     }
///     Ok(())
/// }
/// ```
///
/// Registered tasks also generate global helpers for dispatch from anywhere:
///
/// ```ignore
/// extract_attachment_text::send(input).await?;
/// extract_attachment_text::schedule(Duration::from_secs(30), input).await?;
/// // Explicit handle-based path (testing / advanced):
/// let extract = extract_attachment_text::handle(&rt)?;
/// ```
///
/// `TaskRuntime` can also retrieve typed app-provided state:
///
/// ```ignore
/// let settings = rt.state::<AppSettings>()?;
/// ```
///
/// Generates a companion `#[doc(hidden)]` module with a `register()`
/// function. Register via:
///
/// ```ignore
/// let task = validate_order::register(&mut app)?;
/// ```
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = parse_macro_input!(attr as parse::TaskAttrs);
    let func = parse_macro_input!(item as ItemFn);

    match codegen::generate_task(attrs, func, /* blocking */ false) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Attribute macro for defining a blocking (CPU-bound) horsies task.
///
/// Same as `#[horsies::task]` but generates a `blocking_task_fn!` registration
/// instead of `async_task_fn!`.
///
/// ```ignore
/// #[horsies::blocking_task("cpu_heavy", queue = "background")]
/// fn cpu_heavy(input: HeavyInput) -> Result<HeavyOutput, TaskError> {
///     Ok(HeavyOutput { ... })
/// }
/// ```
#[proc_macro_attribute]
pub fn blocking_task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = parse_macro_input!(attr as parse::TaskAttrs);
    let func = parse_macro_input!(item as ItemFn);

    match codegen::generate_task(attrs, func, /* blocking */ true) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive typed workflow field tokens for a named-field input struct.
///
/// Generates one associated function per field:
///
/// ```ignore
/// #[derive(Serialize, Deserialize, horsies::WorkflowInput)]
/// struct ProcessInput {
///     data: TaskResult<String>,
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
