use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{FnArg, ItemFn, Pat, ReturnType, Type};

use crate::parse::TaskAttrs;

/// Generate the original function + companion descriptor module.
pub fn generate_task(attrs: TaskAttrs, func: ItemFn, blocking: bool) -> syn::Result<TokenStream> {
    // --- Validate the function signature ---
    let signature = validate_signature(&func, blocking)?;

    // --- Extract types from signature ---
    let (args_type, output_type) = extract_types(&func, &signature)?;

    // --- Build the generated code ---
    let fn_name = &func.sig.ident;
    let vis = &func.vis;
    let task_name_str = &attrs.name;
    let task_name_value = task_name_str.value();

    // Task options body
    let task_options_body = build_task_options_body(&attrs);

    // Generated registration wrapper.
    let base_registered_task = build_registered_task(&signature, fn_name, &args_type, blocking);
    let registered_task = if attrs.workflow_ctx {
        quote! { { #base_registered_task }.with_workflow_ctx() }
    } else {
        base_registered_task
    };
    let runtime_capture = if signature.accepts_runtime() {
        quote! { let __horsies_runtime = app.task_runtime(); }
    } else {
        quote! {}
    };

    let module_input_items = generate_module_input_items(&signature, &args_type);
    let params_items = generate_params_items(&signature, &args_type);
    let send_sig = generate_send_signature(&signature, &args_type);
    let method_send_sig = generate_method_send_signature(&signature, &args_type);
    let send_build_args = generate_send_build_args(&signature);

    // Separate cfg attributes (applied to both fn and module) from
    // other attributes (applied to fn only).
    let cfg_attrs: Vec<_> = func
        .attrs
        .iter()
        .filter(|a| a.path().is_ident("cfg"))
        .collect();

    let preserved_attrs: Vec<_> = func.attrs.iter().collect();

    let func_block = &func.block;
    let func_sig = &func.sig;
    let helper_docs = format!(
        "Retrieve the runtime-bound handle for `{}` from `TaskRuntime`.",
        task_name_value
    );
    let send_docs = format!(
        "Enqueue `{}` immediately. Works from any call site after `register()` is called at startup.",
        task_name_value
    );
    let schedule_docs = format!(
        "Enqueue `{}` with a delay. Works from any call site after `register()` is called at startup.",
        task_name_value
    );
    let with_options_docs = format!(
        "Configure per-send options for `{}`. Use this for dynamic deadlines such as `good_until`.",
        task_name_value
    );
    let node_docs = format!(
        "Create a workflow node for `{}`. Inherits queue, priority, and task options from the registered task. Requires `register()` first.",
        task_name_value
    );
    let not_registered_msg = format!(
        "task '{}' is not registered — call {}::register(&mut app) at startup",
        task_name_value, task_name_value
    );

    // Build the queue setter chain
    let queue_chain = match &attrs.queue {
        Some(q) => quote! { let builder = builder.queue(#q); },
        None => quote! {},
    };

    // Build the task_options setter chain
    let opts_chain = if task_options_body.to_string() != "None" {
        quote! {
            let opts: Option<horsies::TaskOptions> = #task_options_body;
            let builder = if let Some(o) = opts { builder.task_options(o) } else { builder };
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        // Original function — all attributes preserved.
        #(#preserved_attrs)*
        #vis #func_sig #func_block

        /// Generated task registration module.
        /// cfg attributes are duplicated so the module is compiled out
        /// when the function is compiled out.
        #(#cfg_attrs)*
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        #vis mod #fn_name {
            use super::*;

            #module_input_items

            #params_items

            /// Per-send options bound to this task's generated send helpers.
            pub struct SendOptions {
                options: horsies::TaskSendOptions,
            }

            impl SendOptions {
                #[doc = #send_docs]
                pub async fn send(&self #method_send_sig) -> horsies::TaskSendResult<horsies::TaskHandle<#output_type>> {
                    let __args: #args_type = #send_build_args;
                    __HANDLE
                        .get()
                        .ok_or_else(|| horsies::TaskSendError {
                            code: horsies::TaskSendErrorCode::ValidationFailed,
                            message: #not_registered_msg.to_owned(),
                            retryable: false,
                            task_id: None,
                            payload: None,
                        })?
                        .send_with_options(self.options, __args)
                        .await
                }

                #[doc = #schedule_docs]
                pub async fn schedule(
                    &self,
                    delay: std::time::Duration
                    #method_send_sig
                ) -> horsies::TaskSendResult<horsies::TaskHandle<#output_type>> {
                    let __args: #args_type = #send_build_args;
                    __HANDLE
                        .get()
                        .ok_or_else(|| horsies::TaskSendError {
                            code: horsies::TaskSendErrorCode::ValidationFailed,
                            message: #not_registered_msg.to_owned(),
                            retryable: false,
                            task_id: None,
                            payload: None,
                        })?
                        .schedule_with_options(self.options, delay, __args)
                        .await
                }
            }

            static __HANDLE: std::sync::OnceLock<
                horsies::TaskFunction<#args_type, #output_type>,
            > = std::sync::OnceLock::new();

            /// Register this task on the app and return a typed `TaskFunction`.
            ///
            /// Also populates the global handle so that [`send`] and [`schedule`]
            /// work from any call site without `TaskRuntime`.
            pub fn register(
                app: &mut horsies::Horsies,
            ) -> Result<
                horsies::TaskFunction<#args_type, #output_type>,
                horsies::HorsiesError,
            > {
                #runtime_capture
                let builder = app.task::<#args_type, #output_type>(
                    #task_name_str,
                    #registered_task,
                )?;
                #queue_chain
                #opts_chain
                let handle = builder.register()?;
                let _ = __HANDLE.set(handle.clone());
                Ok(handle)
            }

            #[doc = #helper_docs]
            pub fn handle(
                rt: &horsies::TaskRuntime,
            ) -> Result<horsies::TaskFunction<#args_type, #output_type>, horsies::TaskError> {
                rt.task_handle::<#args_type, #output_type>(#task_name_str)
            }

            #[doc = #send_docs]
            pub async fn send(#send_sig) -> horsies::TaskSendResult<horsies::TaskHandle<#output_type>> {
                let __args: #args_type = #send_build_args;
                __HANDLE
                    .get()
                    .ok_or_else(|| horsies::TaskSendError {
                        code: horsies::TaskSendErrorCode::ValidationFailed,
                        message: #not_registered_msg.to_owned(),
                        retryable: false,
                        task_id: None,
                        payload: None,
                    })?
                    .send(__args)
                    .await
            }

            #[doc = #schedule_docs]
            pub async fn schedule(
                delay: std::time::Duration,
                #send_sig
            ) -> horsies::TaskSendResult<horsies::TaskHandle<#output_type>> {
                let __args: #args_type = #send_build_args;
                __HANDLE
                    .get()
                    .ok_or_else(|| horsies::TaskSendError {
                        code: horsies::TaskSendErrorCode::ValidationFailed,
                        message: #not_registered_msg.to_owned(),
                        retryable: false,
                        task_id: None,
                        payload: None,
                    })?
                    .schedule(delay, __args)
                    .await
            }

            #[doc = #with_options_docs]
            pub fn with_options(options: horsies::TaskSendOptions) -> SendOptions {
                SendOptions { options }
            }

            #[doc = #node_docs]
            pub fn node() -> Result<horsies::TaskNode<#output_type, #args_type>, horsies::HorsiesError> {
                __HANDLE
                    .get()
                    .ok_or_else(|| horsies::HorsiesError::new(
                        #not_registered_msg.to_owned(),
                    ))
                    .map(|h| h.node())
            }
        }
    })
}

#[derive(Clone)]
struct ParamSpec {
    ident: syn::Ident,
    ty: Type,
}

#[derive(Clone)]
enum InputShape {
    Unit,
    Direct(ParamSpec),
    Wrapped(Vec<ParamSpec>),
}

#[derive(Clone)]
struct SignatureShape {
    has_runtime: bool,
    input: InputShape,
}

impl SignatureShape {
    fn accepts_runtime(&self) -> bool {
        self.has_runtime
    }
}

/// Validate the function signature matches task requirements.
fn validate_signature(func: &ItemFn, blocking: bool) -> syn::Result<SignatureShape> {
    let sig = &func.sig;

    // Reject methods (self receiver).
    for arg in &sig.inputs {
        if let FnArg::Receiver(r) = arg {
            return Err(syn::Error::new_spanned(
                r,
                "task functions must be free functions, not methods",
            ));
        }
    }

    // Reject generics.
    if !sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &sig.generics,
            "generic task functions are not supported",
        ));
    }

    // Reject lifetime parameters.
    if sig.generics.lt_token.is_some() {
        return Err(syn::Error::new_spanned(
            &sig.generics,
            "task functions with lifetime parameters are not supported",
        ));
    }

    // Task signatures may be either:
    // - `()`
    // - `(TaskRuntime)`
    // - `(arg)`
    // - `(TaskRuntime, arg)`
    // - `(a, b, c, ...)`
    // - `(TaskRuntime, a, b, c, ...)`
    let typed_args: Vec<_> = sig
        .inputs
        .iter()
        .filter_map(|a| match a {
            FnArg::Typed(t) => Some(t.clone()),
            _ => None,
        })
        .collect();

    let (has_runtime, user_args) = match typed_args.as_slice() {
        [] => (false, Vec::new()),
        [arg] if is_task_runtime_type(&arg.ty) => (true, Vec::new()),
        [arg] => (false, vec![arg.clone()]),
        [first, rest @ ..] if is_task_runtime_type(&first.ty) => (true, rest.to_vec()),
        _ => (false, typed_args),
    };

    if let Some(arg) = user_args
        .iter()
        .skip(usize::from(has_runtime))
        .find(|arg| is_task_runtime_type(&arg.ty))
    {
        return Err(syn::Error::new_spanned(
            arg,
            "task functions may inject TaskRuntime only as the first argument",
        ));
    }

    let params = user_args
        .iter()
        .map(param_spec_from_arg)
        .collect::<syn::Result<Vec<_>>>()?;

    let input = match params.as_slice() {
        [] => InputShape::Unit,
        [param] if !should_wrap_single_param(&param.ty) => InputShape::Direct(param.clone()),
        _ => InputShape::Wrapped(params),
    };

    let signature = SignatureShape { has_runtime, input };

    // Async check: #[task] requires async, #[blocking_task] requires non-async.
    if !blocking && sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            sig.fn_token,
            "#[horsies::task] requires an async function; use #[horsies::blocking_task] for sync functions",
        ));
    }
    if blocking && sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            sig.asyncness,
            "#[horsies::blocking_task] requires a non-async function; use #[horsies::task] for async functions",
        ));
    }

    // Must have an explicit return type.
    if matches!(sig.output, ReturnType::Default) {
        return Err(syn::Error::new_spanned(
            sig.fn_token,
            "task functions must have an explicit return type: -> Result<T, TaskError>",
        ));
    }

    // Return type must be Result<T, TaskError>.
    if let ReturnType::Type(_, ty) = &sig.output {
        if !looks_like_result(ty) {
            return Err(syn::Error::new_spanned(
                ty,
                "task functions must return Result<T, TaskError>",
            ));
        }
        // Validate the error type is one of the accepted TaskError paths.
        // Syntactic check for supported forms; final type correctness is
        // enforced by Rust compilation of the generated code.
        validate_error_type(ty)?;
    }

    Ok(signature)
}

/// Extract the args type and output type from the function signature.
fn extract_types(func: &ItemFn, signature: &SignatureShape) -> syn::Result<(Type, Type)> {
    let args_type = match signature {
        SignatureShape {
            input: InputShape::Unit,
            ..
        } => syn::parse_quote!(()),
        SignatureShape {
            input: InputShape::Direct(param),
            ..
        } => param.ty.clone(),
        SignatureShape {
            input: InputShape::Wrapped(_),
            ..
        } => syn::parse_quote!(Input),
    };

    // Output type: extract T from Result<T, TaskError>.
    let output_type = match &func.sig.output {
        ReturnType::Type(_, ty) => extract_result_ok_type(ty)?,
        _ => unreachable!("validated: explicit return type"),
    };

    Ok((args_type, output_type))
}

fn is_task_runtime_type(ty: &Type) -> bool {
    let Type::Path(tp) = ty else {
        return false;
    };
    tp.path
        .segments
        .last()
        .map(|segment| segment.ident == "TaskRuntime")
        .unwrap_or(false)
}

fn build_registered_task(
    signature: &SignatureShape,
    fn_name: &syn::Ident,
    args_type: &Type,
    blocking: bool,
) -> TokenStream {
    let call_expr = build_call_expr(signature, fn_name, blocking);

    if blocking {
        let wrapper_runtime_field = if signature.accepts_runtime() {
            quote! { runtime: horsies::TaskRuntime, }
        } else {
            quote! {}
        };
        let wrapper_runtime_init = if signature.accepts_runtime() {
            quote! { runtime: __horsies_runtime.clone(), }
        } else {
            quote! {}
        };
        let wrapper_runtime_clone = if signature.accepts_runtime() {
            quote! { let runtime = self.runtime.clone(); }
        } else {
            quote! {}
        };
        quote! {{
            struct __BlockingTaskWrapper {
                #wrapper_runtime_field
            }

            impl horsies::core::task::fn_trait::BlockingTaskFn for __BlockingTaskWrapper {
                fn execute(&self, args: &[u8]) -> horsies::core::task::fn_trait::RawTaskResult {
                    #wrapper_runtime_clone
                    let deserialized: #args_type = match horsies::core::task::macros::decode_task_input(args) {
                        Ok(v) => v,
                        Err(task_error) => {
                            return horsies::TaskResult::Err(task_error);
                        }
                    };

                    #call_expr
                }
            }

            horsies::core::task::fn_trait::RegisteredTask::Blocking {
                task: ::std::sync::Arc::new(__BlockingTaskWrapper {
                    #wrapper_runtime_init
                }),
                meta: horsies::core::task::fn_trait::TaskMeta::for_input::<#args_type>(),
            }
        }}
    } else {
        let wrapper_runtime_field = if signature.accepts_runtime() {
            quote! { runtime: horsies::TaskRuntime, }
        } else {
            quote! {}
        };
        let wrapper_runtime_init = if signature.accepts_runtime() {
            quote! { runtime: __horsies_runtime.clone(), }
        } else {
            quote! {}
        };
        let wrapper_runtime_clone = if signature.accepts_runtime() {
            quote! { let runtime = self.runtime.clone(); }
        } else {
            quote! {}
        };
        quote! {{
            struct __AsyncTaskWrapper {
                #wrapper_runtime_field
            }

            impl horsies::core::task::fn_trait::AsyncTaskFn for __AsyncTaskWrapper {
                fn execute(
                    &self,
                    args: &[u8],
                ) -> ::std::pin::Pin<
                    Box<
                        dyn ::std::future::Future<
                                Output = horsies::core::task::fn_trait::RawTaskResult,
                            > + Send
                            + '_,
                    >,
                > {
                    let args = args.to_vec();
                    Box::pin(async move {
                        #wrapper_runtime_clone
                        let deserialized: #args_type = match horsies::core::task::macros::decode_task_input(&args) {
                            Ok(v) => v,
                            Err(task_error) => {
                                return horsies::TaskResult::Err(task_error);
                            }
                        };

                        #call_expr
                    })
                }
            }

            horsies::core::task::fn_trait::RegisteredTask::Async {
                task: ::std::sync::Arc::new(__AsyncTaskWrapper {
                    #wrapper_runtime_init
                }),
                meta: horsies::core::task::fn_trait::TaskMeta::for_input::<#args_type>(),
            }
        }}
    }
}

fn build_call_expr(
    signature: &SignatureShape,
    fn_name: &syn::Ident,
    blocking: bool,
) -> TokenStream {
    let call = match &signature.input {
        InputShape::Unit => {
            if signature.has_runtime {
                quote! { super::#fn_name(runtime) }
            } else {
                quote! { super::#fn_name() }
            }
        }
        InputShape::Direct(_param) => {
            if signature.has_runtime {
                quote! { super::#fn_name(runtime, deserialized) }
            } else {
                quote! { super::#fn_name(deserialized) }
            }
        }
        InputShape::Wrapped(params) => {
            let names: Vec<_> = params.iter().map(|p| &p.ident).collect();
            let destructure = quote! {
                let Input { #(#names),* } = deserialized;
            };
            let invoke = if signature.has_runtime {
                quote! { super::#fn_name(runtime, #(#names),*) }
            } else {
                quote! { super::#fn_name(#(#names),*) }
            };
            return if blocking {
                quote! {{
                    #destructure
                    match #invoke {
                        Ok(value) => horsies::core::task::macros::encode_validated_task_output(&value),
                        Err(task_error) => horsies::TaskResult::Err(task_error),
                    }
                }}
            } else {
                quote! {{
                    #destructure
                    match #invoke.await {
                        Ok(value) => horsies::core::task::macros::encode_validated_task_output(&value),
                        Err(task_error) => horsies::TaskResult::Err(task_error),
                    }
                }}
            };
        }
    };

    if blocking {
        quote! {
            match #call {
                Ok(value) => horsies::core::task::macros::encode_validated_task_output(&value),
                Err(task_error) => horsies::TaskResult::Err(task_error),
            }
        }
    } else {
        quote! {
            match #call.await {
                Ok(value) => horsies::core::task::macros::encode_validated_task_output(&value),
                Err(task_error) => horsies::TaskResult::Err(task_error),
            }
        }
    }
}

fn generate_module_input_items(signature: &SignatureShape, _args_type: &Type) -> TokenStream {
    match &signature.input {
        InputShape::Wrapped(params) => {
            let fields = params.iter().map(|param| {
                let ident = &param.ident;
                let ty = &param.ty;
                quote! { pub #ident: #ty, }
            });
            quote! {
                #[derive(serde::Serialize, serde::Deserialize)]
                pub struct Input {
                    #(#fields)*
                }
            }
        }
        _ => quote! {},
    }
}

fn generate_params_items(signature: &SignatureShape, _args_type: &Type) -> TokenStream {
    match &signature.input {
        InputShape::Unit => quote! {},
        InputShape::Direct(_param) => quote! {},
        InputShape::Wrapped(params) => {
            let field_fns = params.iter().map(|param| {
                let ident = &param.ident;
                let ty = &param.ty;
                let field_name = ident.to_string();
                quote! {
                    pub fn #ident() -> horsies::InputField<Input, #ty> {
                        horsies::__private::input_field(#field_name)
                    }
                }
            });
            quote! {
                pub mod params {
                    use super::*;
                    #(#field_fns)*
                }
            }
        }
    }
}

fn generate_send_signature(signature: &SignatureShape, args_type: &Type) -> TokenStream {
    match &signature.input {
        InputShape::Unit => quote! {},
        InputShape::Direct(param) => {
            let ident = &param.ident;
            quote! { #ident: #args_type }
        }
        InputShape::Wrapped(params) => {
            let args = params.iter().map(|param| {
                let ident = &param.ident;
                let ty = &param.ty;
                quote! { #ident: #ty }
            });
            quote! { #(#args),* }
        }
    }
}

fn generate_method_send_signature(signature: &SignatureShape, args_type: &Type) -> TokenStream {
    match &signature.input {
        InputShape::Unit => quote! {},
        _ => {
            let send_sig = generate_send_signature(signature, args_type);
            quote! { , #send_sig }
        }
    }
}

fn generate_send_build_args(signature: &SignatureShape) -> TokenStream {
    match &signature.input {
        InputShape::Unit => quote! { () },
        InputShape::Direct(param) => {
            let ident = &param.ident;
            quote! { #ident }
        }
        InputShape::Wrapped(params) => {
            let names = params.iter().map(|param| &param.ident);
            quote! { Input { #(#names),* } }
        }
    }
}

fn param_spec_from_arg(arg: &syn::PatType) -> syn::Result<ParamSpec> {
    match arg.pat.as_ref() {
        Pat::Ident(pat_ident) => Ok(ParamSpec {
            ident: pat_ident.ident.clone(),
            ty: (*arg.ty).clone(),
        }),
        Pat::Wild(_) => Ok(ParamSpec {
            ident: syn::Ident::new("input", arg.pat.span()),
            ty: (*arg.ty).clone(),
        }),
        _ => Err(syn::Error::new_spanned(
            &arg.pat,
            "task function parameters must be plain identifiers",
        )),
    }
}

fn should_wrap_single_param(ty: &Type) -> bool {
    is_task_result_type(ty)
}

fn is_task_result_type(ty: &Type) -> bool {
    let Type::Path(tp) = ty else {
        return false;
    };
    tp.path
        .segments
        .last()
        .map(|segment| segment.ident == "TaskResult")
        .unwrap_or(false)
}

/// Check if a type looks like `Result<...>`.
fn looks_like_result(ty: &Type) -> bool {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            return seg.ident == "Result";
        }
    }
    false
}

/// Check whether the error type in `Result<T, E>` is a recognized TaskError path.
///
/// Accepted forms:
/// - `TaskError`
/// - `horsies::TaskError`
///
/// Returns `Ok(())` if accepted or if the type is too complex to analyze
/// (defers to compiler). Returns `Err` with a span-targeted diagnostic
/// if the type is a simple path that doesn't match any accepted form.
fn validate_error_type(ty: &Type) -> syn::Result<()> {
    let err_type = match extract_result_second_arg(ty) {
        Some(t) => t,
        None => return Ok(()), // complex type — defer to compiler
    };

    let Type::Path(err_tp) = &err_type else {
        // Not a path type (tuple, reference, etc.) — reject.
        return Err(syn::Error::new_spanned(
            &err_type,
            "task function error type must be TaskError",
        ));
    };

    let segments: Vec<_> = err_tp
        .path
        .segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect();

    let accepted = matches!(
        segments
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .as_slice(),
        ["TaskError"] | ["horsies", "TaskError"]
    );

    if !accepted {
        let path_str = segments.join("::");
        return Err(syn::Error::new_spanned(
            &err_tp.path,
            format!(
                "task function error type must be TaskError, found `{}`; \
                 accepted forms: TaskError, horsies::TaskError",
                path_str,
            ),
        ));
    }

    Ok(())
}

/// Extract the second generic argument from `Result<T, E>`.
fn extract_result_second_arg(ty: &Type) -> Option<Type> {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            if seg.ident == "Result" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(err_type)) = args.args.iter().nth(1) {
                        return Some(err_type.clone());
                    }
                }
            }
        }
    }
    None
}

/// Extract `T` from `Result<T, E>`.
fn extract_result_ok_type(ty: &Type) -> syn::Result<Type> {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            if seg.ident == "Result" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(ok_type)) = args.args.first() {
                        return Ok(ok_type.clone());
                    }
                }
            }
        }
    }
    Err(syn::Error::new_spanned(
        ty,
        "could not extract T from Result<T, TaskError>; expected Result<T, TaskError>",
    ))
}

/// Build the body of `fn task_options(&self)` from parsed attributes.
fn build_task_options_body(attrs: &TaskAttrs) -> TokenStream {
    let has_retry = attrs.retry_policy.is_some();
    let has_auto_retry = attrs.auto_retry_for.is_some();
    let has_timeout = attrs.timeout_ms.is_some();

    if !has_retry && !has_auto_retry && !has_timeout {
        return quote! { None };
    }

    let retry_policy_expr = match &attrs.retry_policy {
        Some(expr) => quote! { Some(#expr) },
        None => quote! { None },
    };

    let auto_retry_for_expr = match &attrs.auto_retry_for {
        Some(arr) => {
            let elems: Vec<_> = arr.elems.iter().collect();
            quote! {
                Some(vec![#(horsies::TaskErrorCode::User(#elems.to_owned())),*])
            }
        }
        None => quote! { None },
    };

    let timeout_ms_expr = match &attrs.timeout_ms {
        // Validated as u32 >= 1000 at parse time; re-emit as a u32-suffixed
        // literal so TaskOptions.timeout_ms (Option<u32>) types without a cast.
        Some(lit) => {
            let value = lit
                .base10_parse::<u32>()
                .expect("timeout_ms validated as u32 at parse time");
            let suffixed = proc_macro2::Literal::u32_suffixed(value);
            quote! { Some(#suffixed) }
        }
        None => quote! { None },
    };

    // task_name and queue_name are normalized by register(), so we use
    // placeholder values here — the builder overwrites them.
    quote! {
        Some(horsies::TaskOptions {
            task_name: String::new(),
            queue_name: None,
            retry_policy: #retry_policy_expr,
            good_until: None,
            auto_retry_for: #auto_retry_for_expr,
            timeout_ms: #timeout_ms_expr,
        })
    }
}
