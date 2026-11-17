//! The expansion of `#[horsies::task]` and `#[horsies::blocking_task]`: the
//! function as written, the compile-time checks of its retry codes, and a
//! module of the same name with `register`, the send helpers, `handle`,
//! `node` and the input field tokens.
//!
//! Every path in the expansion starts at `::horsies`, so the user's crate
//! needs no other dependency for it.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned, ToTokens};
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{Attribute, FnArg, GenericArgument, ItemFn, Pat, PathArguments, ReturnType, Type};

use crate::parse::{ExecutionClass, Retry, TaskAttrs};

/// Which attribute expands: `#[task]` takes an `async fn`, `#[blocking_task]`
/// a `fn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyKind {
    Async,
    Blocking,
}

/// One parameter of the task's input.
#[derive(Clone)]
struct Param {
    ident: syn::Ident,
    ty: Type,
}

/// How the parameters after an optional `TaskRuntime` form the input.
#[derive(Clone)]
enum InputShape {
    /// No parameter: the input is `()`, sent as `null`.
    Unit,
    /// One parameter: its type is the input.
    Direct(Param),
    /// Several parameters, or one that a workflow fills from a dependency
    /// result: a generated `Input` struct with one field per parameter.
    Wrapped(Vec<Param>),
}

#[derive(Clone)]
struct Signature {
    takes_runtime: bool,
    input: InputShape,
}

/// The types of the task: input `A`, output `T`, code type `E`.
struct TaskTypes {
    input: Type,
    output: Type,
    code: Type,
}

/// Expand the task attribute on `func`.
pub fn generate_task(attrs: TaskAttrs, func: ItemFn, kind: BodyKind) -> syn::Result<TokenStream> {
    let signature = validate_signature(&func, kind)?;
    check_execution_fits(&attrs, kind)?;
    let types = task_types(&func, &signature)?;
    let cfg_attrs: Vec<&Attribute> = func
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("cfg"))
        .collect();
    let retry_checks = retry_code_checks(&attrs, &types.code, &cfg_attrs);
    let module = task_module(&attrs, &func, &signature, &types, kind);
    Ok(quote! {
        #func
        #retry_checks
        #(#cfg_attrs)*
        #module
    })
}

fn task_module(
    attrs: &TaskAttrs,
    func: &ItemFn,
    signature: &Signature,
    types: &TaskTypes,
    kind: BodyKind,
) -> TokenStream {
    let fn_name = &func.sig.ident;
    let vis = &func.vis;
    let task_name = &attrs.name;
    let TaskTypes {
        input,
        output,
        code,
    } = types;
    let function_type = quote! { ::horsies::TaskFunction<#input, #output, #code> };
    let handle_type = quote! { ::horsies::TaskHandle<#output, #code> };
    let send_result = quote! { ::std::result::Result<#handle_type, ::horsies::TaskSendError> };
    let input_items = input_struct(signature);
    let params_items = params_module(signature);
    let send_params = send_params(signature);
    let build_input = build_input(signature);
    let registered = registered_task(signature, fn_name, input, kind);
    let workflow_ctx = match attrs.workflow_ctx {
        true => quote! { .with_workflow_ctx() },
        false => quote! {},
    };
    let runtime_capture = match signature.takes_runtime {
        true => quote! { let __horsies_runtime = app.task_runtime(); },
        false => quote! {},
    };
    let options = task_options(attrs);
    quote! {
        /// The registration and the send helpers of the task.
        #[doc(hidden)]
        // A private task gets a private module, whose unused helpers are no
        // defect of the user's code.
        #[allow(non_camel_case_types, dead_code)]
        #vis mod #fn_name {
            use super::*;

            #input_items

            #params_items

            const TASK_NAME: &str = #task_name;

            /// The function that the first `register` in this process
            /// returned; the send helpers send through it.
            static FUNCTION: ::std::sync::OnceLock<#function_type> =
                ::std::sync::OnceLock::new();

            /// Register the task on `app` and return its typed function.
            /// The first registration in a process also serves `send`,
            /// `schedule`, `with_options` and `node` of this module.
            pub fn register(
                app: &mut ::horsies::Horsies,
            ) -> ::std::result::Result<#function_type, ::horsies::HorsiesError> {
                ::horsies::__private::refuse_foreign_user_codes::<#code>(TASK_NAME)?;
                #runtime_capture
                let task = #registered #workflow_ctx .with_task_options(#options);
                app.register(TASK_NAME, task)?;
                let function =
                    <#function_type>::__registered(app, TASK_NAME)?;
                let _first_registration =
                    FUNCTION.set(::std::clone::Clone::clone(&function));
                ::std::result::Result::Ok(function)
            }

            fn registered() -> ::std::result::Result<
                &'static #function_type,
                ::horsies::TaskSendError,
            > {
                FUNCTION
                    .get()
                    .ok_or_else(|| ::horsies::__private::send_before_register(TASK_NAME))
            }

            /// The typed function of the task in the app of `runtime`.
            pub fn handle(
                runtime: &::horsies::TaskRuntime,
            ) -> ::std::result::Result<#function_type, ::horsies::TaskFunctionNotFound> {
                runtime.task_handle::<#input, #output, #code>(TASK_NAME)
            }

            /// Send the task now.
            pub async fn send(#send_params) -> #send_result {
                let input: #input = #build_input;
                registered()?.send(input).await
            }

            /// Send the task to run after `delay`.
            pub async fn schedule(
                delay: ::horsies::WholeSeconds,
                #send_params
            ) -> #send_result {
                let input: #input = #build_input;
                registered()?.schedule(delay, input).await
            }

            /// The send helpers with per-send options.
            pub fn with_options(options: ::horsies::TaskSendOptions) -> SendOptions {
                SendOptions { options }
            }

            /// Per-send options of this task's send helpers.
            pub struct SendOptions {
                options: ::horsies::TaskSendOptions,
            }

            impl SendOptions {
                /// Send the task now with these options.
                pub async fn send(&self, #send_params) -> #send_result {
                    let input: #input = #build_input;
                    registered()?
                        .with_options(::std::clone::Clone::clone(&self.options))
                        .send(input)
                        .await
                }

                /// Send the task to run after `delay` with these options.
                pub async fn schedule(
                    &self,
                    delay: ::horsies::WholeSeconds,
                    #send_params
                ) -> #send_result {
                    let input: #input = #build_input;
                    registered()?
                        .with_options(::std::clone::Clone::clone(&self.options))
                        .schedule(delay, input)
                        .await
                }
            }

            /// A workflow node of the task, with the queue, priority and
            /// options of its registration.
            pub fn node() -> ::std::result::Result<
                ::horsies::TaskNode<#output, #input>,
                ::horsies::HorsiesError,
            > {
                FUNCTION
                    .get()
                    .map(|function| function.node())
                    .ok_or_else(|| ::horsies::__private::node_before_register(TASK_NAME))
            }
        }
    }
}

/// The `TaskOptions` of the registration: the attribute values, with the
/// default class `ISOLATED` and the default grace of a `SHARED_ASYNC` task.
fn task_options(attrs: &TaskAttrs) -> TokenStream {
    let queue_name = match &attrs.queue {
        Some(queue) => {
            quote! { ::std::option::Option::Some(::std::string::String::from(#queue)) }
        }
        None => quote! { ::std::option::Option::None },
    };
    let (retry_policy, auto_retry_for) = match &attrs.retry {
        Retry::NoRetry => (
            quote! { ::std::option::Option::None },
            quote! { ::std::option::Option::None },
        ),
        Retry::Policy { policy, codes } => {
            let codes = codes.iter().map(|entry| &entry.code);
            (
                quote! { ::std::option::Option::Some(#policy) },
                quote! {
                    ::std::option::Option::Some(::std::vec![
                        #(::horsies::__private::retry_code(#codes)),*
                    ])
                },
            )
        }
    };
    let timeout_ms = match &attrs.timeout_ms {
        Some(timeout) => {
            let value = proc_macro2::Literal::u32_suffixed(timeout.value);
            quote! { ::std::option::Option::Some(#value) }
        }
        None => quote! { ::std::option::Option::None },
    };
    let (execution, cancel_grace_ms) = match (attrs.execution.class, &attrs.cancel_grace_ms) {
        (ExecutionClass::Isolated, _) => (
            quote! { ::horsies::Execution::Isolated },
            quote! { ::std::option::Option::None },
        ),
        (ExecutionClass::SharedThread, _) => (
            quote! { ::horsies::Execution::SharedThread },
            quote! { ::std::option::Option::None },
        ),
        (ExecutionClass::SharedAsync, Some(grace)) => {
            let value = proc_macro2::Literal::u32_suffixed(grace.value);
            (
                quote! { ::horsies::Execution::SharedAsync },
                quote! {
                    ::std::option::Option::Some(::horsies::__private::cancel_grace(#value))
                },
            )
        }
        (ExecutionClass::SharedAsync, None) => (
            quote! { ::horsies::Execution::SharedAsync },
            quote! {
                ::std::option::Option::Some(::horsies::__private::DEFAULT_CANCEL_GRACE)
            },
        ),
    };
    quote! {
        ::horsies::TaskOptions {
            task_name: ::std::string::String::from(TASK_NAME),
            queue_name: #queue_name,
            good_until: ::std::option::Option::None,
            auto_retry_for: #auto_retry_for,
            retry_policy: #retry_policy,
            timeout_ms: #timeout_ms,
            execution: #execution,
            cancel_grace_ms: #cancel_grace_ms,
        }
    }
}

/// One `const` assertion per `auto_retry_for` code: a built-in code, or a
/// code of the task's code type `E`. The assertions sit next to the
/// function, so `E` resolves as in its signature.
fn retry_code_checks(attrs: &TaskAttrs, code: &Type, cfg_attrs: &[&Attribute]) -> TokenStream {
    let Retry::Policy { codes, .. } = &attrs.retry else {
        return quote! {};
    };
    let code_type_text = code.to_token_stream().to_string().replace(' ', "");
    let checks = codes.iter().map(|entry| {
        let literal = &entry.code;
        // `assert!` reads its message as a format string with no argument.
        let message = format!(
            "auto_retry_for of task '{}': {} is neither a built-in code nor a code of {}",
            attrs.name.value(),
            literal.value(),
            code_type_text
        )
        .replace('{', "{{")
        .replace('}', "}}");
        quote_spanned! {literal.span()=>
            #(#cfg_attrs)*
            const _: () = ::core::assert!(
                ::horsies::__private::is_retry_code(
                    #literal,
                    <#code as ::horsies::UserErrorCode>::CODES,
                ),
                #message
            );
        }
    });
    quote! { #(#checks)* }
}

/// The `RegisteredTask` of the function: a body struct that decodes the
/// input, calls the function and stores what it returned.
fn registered_task(
    signature: &Signature,
    fn_name: &syn::Ident,
    input: &Type,
    kind: BodyKind,
) -> TokenStream {
    let (runtime_field, runtime_init, runtime_clone) = match signature.takes_runtime {
        true => (
            quote! { runtime: ::horsies::TaskRuntime, },
            quote! { runtime: ::std::clone::Clone::clone(&__horsies_runtime), },
            quote! { let runtime = ::std::clone::Clone::clone(&self.runtime); },
        ),
        false => (quote! {}, quote! {}, quote! {}),
    };
    let decode = decode_statement(signature, input);
    let call = call_expression(signature, fn_name);
    let meta = quote! {
        ::horsies::__private::TaskMeta::for_input::<#input>()
            .in_module(::horsies::__private::parent_module(::std::module_path!()))
    };
    let validate = quote! {
        fn validate_input(
            &self,
            input: &[u8],
        ) -> ::std::result::Result<(), ::horsies::__private::StoredTaskError> {
            ::horsies::__private::check_input::<#input>(input)
        }
    };
    match kind {
        BodyKind::Async => quote! {{
            struct __HorsiesBody {
                #runtime_field
            }

            impl ::horsies::__private::AsyncTaskFn for __HorsiesBody {
                fn execute(
                    &self,
                    input: &[u8],
                ) -> ::std::pin::Pin<
                    ::std::boxed::Box<
                        dyn ::std::future::Future<
                                Output = ::horsies::__private::RawTaskResult,
                            > + ::std::marker::Send
                            + '_,
                    >,
                > {
                    let input = input.to_vec();
                    #runtime_clone
                    ::std::boxed::Box::pin(async move {
                        #decode
                        ::horsies::__private::body_result(#call.await)
                    })
                }

                #validate
            }

            ::horsies::__private::RegisteredTask::Async {
                task: ::std::sync::Arc::new(__HorsiesBody { #runtime_init }),
                meta: #meta,
            }
        }},
        BodyKind::Blocking => quote! {{
            struct __HorsiesBody {
                #runtime_field
            }

            impl ::horsies::__private::BlockingTaskFn for __HorsiesBody {
                fn execute(&self, input: &[u8]) -> ::horsies::__private::RawTaskResult {
                    #runtime_clone
                    #decode
                    ::horsies::__private::body_result(#call)
                }

                #validate
            }

            ::horsies::__private::RegisteredTask::Blocking {
                task: ::std::sync::Arc::new(__HorsiesBody { #runtime_init }),
                meta: #meta,
            }
        }},
    }
}

/// The decode of `input` (a byte slice or a vector in scope) into the
/// bindings that the call takes. A refusal returns the stored error.
fn decode_statement(signature: &Signature, input_type: &Type) -> TokenStream {
    let pattern = match &signature.input {
        InputShape::Unit => quote! { () },
        InputShape::Direct(_) => quote! { decoded },
        InputShape::Wrapped(params) => {
            let names = params.iter().map(|param| &param.ident);
            quote! { Input { #(#names),* } }
        }
    };
    quote! {
        let #pattern = match ::horsies::__private::decode_input::<#input_type>(&input) {
            ::std::result::Result::Ok(decoded) => decoded,
            ::std::result::Result::Err(refused) => {
                return ::horsies::__private::TaskResult::Err(refused);
            }
        };
    }
}

/// `super::f(...)` with the runtime and the decoded input.
fn call_expression(signature: &Signature, fn_name: &syn::Ident) -> TokenStream {
    let runtime = match signature.takes_runtime {
        true => vec![quote! { runtime }],
        false => vec![],
    };
    let input: Vec<TokenStream> = match &signature.input {
        InputShape::Unit => vec![],
        InputShape::Direct(_) => vec![quote! { decoded }],
        InputShape::Wrapped(params) => params
            .iter()
            .map(|param| param.ident.to_token_stream())
            .collect(),
    };
    let arguments = runtime.into_iter().chain(input);
    quote! { super::#fn_name(#(#arguments),*) }
}

/// The `Input` struct of a wrapped input: one public field per parameter,
/// unknown keys refused.
fn input_struct(signature: &Signature) -> TokenStream {
    let InputShape::Wrapped(params) = &signature.input else {
        return quote! {};
    };
    let fields = params.iter().map(|param| {
        let ident = &param.ident;
        let ty = &param.ty;
        quote! { pub #ident: #ty, }
    });
    quote! {
        /// The input of the task: one field per parameter.
        #[derive(
            ::horsies::__private::serde::Serialize,
            ::horsies::__private::serde::Deserialize,
        )]
        #[serde(crate = "::horsies::__private::serde", deny_unknown_fields)]
        pub struct Input {
            #(#fields)*
        }
    }
}

/// `params::<name>()`: the field token of each parameter of a wrapped
/// input. The key is the name that serde reads: a raw identifier without
/// its `r#`.
fn params_module(signature: &Signature) -> TokenStream {
    let InputShape::Wrapped(params) = &signature.input else {
        return quote! {};
    };
    let tokens = params.iter().map(|param| {
        let ident = &param.ident;
        let ty = &param.ty;
        let key = ident.unraw().to_string();
        quote! {
            pub fn #ident() -> ::horsies::InputField<Input, #ty> {
                ::horsies::__private::input_field(#key)
            }
        }
    });
    quote! {
        /// The field tokens of the task's input, for workflow nodes.
        pub mod params {
            use super::*;
            #(#tokens)*
        }
    }
}

/// The parameters of the send helpers: the task's input parameters.
fn send_params(signature: &Signature) -> TokenStream {
    let params: Vec<&Param> = match &signature.input {
        InputShape::Unit => vec![],
        InputShape::Direct(param) => vec![param],
        InputShape::Wrapped(params) => params.iter().collect(),
    };
    let params = params.iter().map(|param| {
        let ident = &param.ident;
        let ty = &param.ty;
        quote! { #ident: #ty }
    });
    quote! { #(#params),* }
}

/// The input value from the send helper's parameters.
fn build_input(signature: &Signature) -> TokenStream {
    match &signature.input {
        InputShape::Unit => quote! { () },
        InputShape::Direct(param) => param.ident.to_token_stream(),
        InputShape::Wrapped(params) => {
            let names = params.iter().map(|param| &param.ident);
            quote! { Input { #(#names),* } }
        }
    }
}

/// The class must fit the function: an `async fn` runs `isolated` or
/// `shared_async`; a `fn` runs `isolated` or `shared_thread`.
fn check_execution_fits(attrs: &TaskAttrs, kind: BodyKind) -> syn::Result<()> {
    match (kind, attrs.execution.class) {
        (BodyKind::Async, ExecutionClass::SharedThread) => Err(syn::Error::new(
            attrs.execution.span,
            "execution = shared_thread runs a blocking fn on a seat thread; an async fn runs \
             isolated or shared_async",
        )),
        (BodyKind::Blocking, ExecutionClass::SharedAsync) => Err(syn::Error::new(
            attrs.execution.span,
            "execution = shared_async runs an async fn on a shared runtime; a blocking fn runs \
             isolated or shared_thread",
        )),
        (BodyKind::Async, ExecutionClass::Isolated | ExecutionClass::SharedAsync)
        | (BodyKind::Blocking, ExecutionClass::Isolated | ExecutionClass::SharedThread) => Ok(()),
    }
}

/// The shape of the signature. Refuses a method, generics, lifetimes, an
/// `async fn` under `#[blocking_task]` and a `fn` under `#[task]`, a missing
/// return type, a `TaskRuntime` that is not the first parameter, and a
/// parameter that is not a plain name.
fn validate_signature(func: &ItemFn, kind: BodyKind) -> syn::Result<Signature> {
    let sig = &func.sig;
    if let Some(receiver) = sig.inputs.iter().find_map(|argument| match argument {
        FnArg::Receiver(receiver) => Some(receiver),
        FnArg::Typed(_) => None,
    }) {
        return Err(syn::Error::new_spanned(
            receiver,
            "task functions must be free functions, not methods",
        ));
    }
    if sig.generics.lt_token.is_some() || !sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &sig.generics,
            "task functions take no generic or lifetime parameters: the input and output types \
             are the wire contract",
        ));
    }
    match (kind, sig.asyncness.is_some()) {
        (BodyKind::Async, false) => {
            return Err(syn::Error::new_spanned(
                sig.fn_token,
                "#[horsies::task] takes an async fn; use #[horsies::blocking_task] for a fn",
            ))
        }
        (BodyKind::Blocking, true) => {
            return Err(syn::Error::new_spanned(
                sig.asyncness,
                "#[horsies::blocking_task] takes a fn; use #[horsies::task] for an async fn",
            ))
        }
        (BodyKind::Async, true) | (BodyKind::Blocking, false) => {}
    }
    if matches!(sig.output, ReturnType::Default) {
        return Err(syn::Error::new_spanned(
            sig.fn_token,
            "task functions return Result<T, TaskError<E>>",
        ));
    }
    let typed: Vec<&syn::PatType> = sig
        .inputs
        .iter()
        .filter_map(|argument| match argument {
            FnArg::Typed(typed) => Some(typed),
            FnArg::Receiver(_) => None,
        })
        .collect();
    let (takes_runtime, user_params) = match typed.split_first() {
        Some((first, rest)) if is_task_runtime(&first.ty) => (true, rest.to_vec()),
        Some(_) | None => (false, typed),
    };
    if let Some(misplaced) = user_params.iter().find(|param| is_task_runtime(&param.ty)) {
        return Err(syn::Error::new_spanned(
            misplaced,
            "TaskRuntime is the first parameter of a task function, or absent",
        ));
    }
    let params = user_params
        .iter()
        .map(|param| param_of(param))
        .collect::<syn::Result<Vec<_>>>()?;
    let input = match params.as_slice() {
        [] => InputShape::Unit,
        [param] if !is_dependency_result(&param.ty) => InputShape::Direct(param.clone()),
        _ => InputShape::Wrapped(params),
    };
    Ok(Signature {
        takes_runtime,
        input,
    })
}

fn param_of(param: &syn::PatType) -> syn::Result<Param> {
    match param.pat.as_ref() {
        Pat::Ident(pattern) => Ok(Param {
            ident: pattern.ident.clone(),
            ty: (*param.ty).clone(),
        }),
        Pat::Wild(_) => Ok(Param {
            ident: syn::Ident::new("input", param.pat.span()),
            ty: (*param.ty).clone(),
        }),
        other => Err(syn::Error::new_spanned(
            other,
            "task function parameters are plain names",
        )),
    }
}

fn is_task_runtime(ty: &Type) -> bool {
    last_segment_is(ty, "TaskRuntime")
}

/// A single parameter that a workflow fills from a dependency result is
/// wrapped, so that the node can bind it by name.
fn is_dependency_result(ty: &Type) -> bool {
    last_segment_is(ty, "TaskResult")
}

fn last_segment_is(ty: &Type, name: &str) -> bool {
    match ty {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == name),
        _ => false,
    }
}

/// The input, output and code types of the task.
fn task_types(func: &ItemFn, signature: &Signature) -> syn::Result<TaskTypes> {
    let input: Type = match &signature.input {
        InputShape::Unit => syn::parse_quote!(()),
        InputShape::Direct(param) => param.ty.clone(),
        InputShape::Wrapped(_) => syn::parse_quote!(Input),
    };
    let ReturnType::Type(_, returned) = &func.sig.output else {
        unreachable!("validate_signature refuses a function without a return type")
    };
    let (output, error) = result_types(returned)?;
    let code = task_error_code(&error)?;
    Ok(TaskTypes {
        input,
        output,
        code,
    })
}

/// `T` and the error type of `Result<T, Error>`.
fn result_types(ty: &Type) -> syn::Result<(Type, Type)> {
    let refusal = || {
        syn::Error::new_spanned(
            ty,
            "task functions return Result<T, TaskError<E>>, with E the code type of the \
             task's own errors",
        )
    };
    let Type::Path(path) = ty else {
        return Err(refusal());
    };
    let Some(segment) = path.path.segments.last() else {
        return Err(refusal());
    };
    if segment.ident != "Result" {
        return Err(refusal());
    }
    match generic_types(&segment.arguments).as_slice() {
        [output, error] => Ok(((*output).clone(), (*error).clone())),
        _ => Err(refusal()),
    }
}

/// `E` of `TaskError<E>` or `horsies::TaskError<E>`.
fn task_error_code(error: &Type) -> syn::Result<Type> {
    let Type::Path(path) = error else {
        return Err(syn::Error::new_spanned(
            error,
            "the error type of a task function is TaskError<E>",
        ));
    };
    let names: Vec<String> = path
        .path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect();
    let is_task_error = matches!(
        names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice(),
        ["TaskError"] | ["horsies", "TaskError"]
    );
    if !is_task_error {
        return Err(syn::Error::new_spanned(
            &path.path,
            format!(
                "the error type of a task function is TaskError<E>, found `{}`; accepted forms: \
                 TaskError<E>, horsies::TaskError<E>",
                names.join("::")
            ),
        ));
    }
    let Some(segment) = path.path.segments.last() else {
        unreachable!("a path that matched TaskError has a last segment")
    };
    match generic_types(&segment.arguments).as_slice() {
        [code] => Ok((*code).clone()),
        _ => Err(syn::Error::new_spanned(
            &path.path,
            "name the code type of the task's own errors: TaskError<YourCode>, or \
             TaskError<std::convert::Infallible> for a task that has none",
        )),
    }
}

fn generic_types(arguments: &PathArguments) -> Vec<&Type> {
    match arguments {
        PathArguments::AngleBracketed(arguments) => arguments
            .args
            .iter()
            .filter_map(|argument| match argument {
                GenericArgument::Type(ty) => Some(ty),
                _ => None,
            })
            .collect(),
        PathArguments::None | PathArguments::Parenthesized(_) => vec![],
    }
}
