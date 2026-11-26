//! The expansion of `#[horsies::task]` and `#[horsies::blocking_task]`: the
//! function as written, the compile-time checks of its retry codes, and a
//! module of the same name with `register`, the send helpers, `handle`,
//! `node` and the input field tokens.
//!
//! The module holds no state. Each send helper takes the app that it sends
//! through and finds the task's typed function in that app; `node()` names
//! the task, and the workflow builder of an app resolves it against that
//! app. So two apps of one process share no registration.
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
    /// No parameter, or one parameter of type `()`: the input is `()`, sent
    /// as `{}` (no keyword arguments, as Python stores them), as a workflow
    /// node gives it.
    Unit(UnitParameter),
    /// One or more parameters, any one that is not a single `()`: a
    /// generated `Input` struct with one field per parameter, keyed by the
    /// parameter's name, as Python stores each keyword argument. A single
    /// struct parameter is wrapped too: `fn f(order: Order)` stores
    /// `{"order": {...}}`.
    Wrapped(Vec<Param>),
}

/// The parameter of a task whose input is `()`.
#[derive(Clone)]
enum UnitParameter {
    /// The function takes no input parameter.
    Absent,
    /// The function takes one parameter of type `()`; the body passes `()`.
    Given(Box<Param>),
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
    // The app parameter of the send helpers resolves at the definition site
    // of the expansion, so a task parameter named `app` binds apart from it.
    let app = syn::Ident::new("app", proc_macro2::Span::mixed_site());
    let input_items = input_struct(signature);
    let params_items = params_module(signature);
    let send_params = send_params(signature);
    let build_input = build_input(signature);
    let registered = registered_task(signature, fn_name, input, code, kind);
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

            /// Register the task on `app` and return its typed function.
            /// The send helpers and the workflow builders of `app` find the
            /// task after this.
            pub fn register(
                app: &mut ::horsies::Horsies,
            ) -> ::std::result::Result<#function_type, ::horsies::HorsiesError> {
                #runtime_capture
                let task = #registered #workflow_ctx .with_task_options(#options);
                app.register(TASK_NAME, task)?;
                <#function_type>::__registered(app, TASK_NAME)
            }

            /// The typed function of the task in the app of `runtime`.
            pub fn handle(
                runtime: &::horsies::TaskRuntime,
            ) -> ::std::result::Result<#function_type, ::horsies::TaskFunctionNotFound> {
                runtime.task_handle::<#input, #output, #code>(TASK_NAME)
            }

            /// Send the task now through `app`, the app that registered
            /// it.
            ///
            /// Errors: the send errors; `VALIDATION_FAILED` when `app` does
            /// not register the task, and nothing is written.
            pub async fn send(#app: &::horsies::Horsies, #send_params) -> #send_result {
                let input: #input = #build_input;
                ::horsies::__private::send_function::<#input, #output, #code>(#app, TASK_NAME)?
                    .send(input)
                    .await
            }

            /// The send helpers with per-send options: a delay, a deadline,
            /// an idempotency key, a retention.
            pub fn with_options(options: ::horsies::TaskSendOptions) -> SendOptions {
                SendOptions { options }
            }

            /// Per-send options of this task's send helpers. A send takes
            /// them; clone them for one more send.
            #[derive(::std::clone::Clone, ::std::fmt::Debug)]
            pub struct SendOptions {
                options: ::horsies::TaskSendOptions,
            }

            impl SendOptions {
                /// Send the task through `app` with these options: now, or
                /// after their delay. The send owns the options, so its
                /// future borrows only `app`:
                /// `name::with_options(options).send(&app, ..)` is one
                /// expression, also when the future is kept in a binding.
                ///
                /// Errors: as `send`.
                pub async fn send(
                    self,
                    #app: &::horsies::Horsies,
                    #send_params
                ) -> #send_result {
                    let input: #input = #build_input;
                    ::horsies::__private::send_function::<#input, #output, #code>(#app, TASK_NAME)?
                        .send_with_options(self.options, input)
                        .await
                }
            }

            /// A workflow node of the task with an empty input `{}`. The
            /// workflow builder that takes it resolves the queue, the
            /// priority and the options of the task against its own app.
            pub fn node() -> ::horsies::TaskNode<#output, #input> {
                ::horsies::__private::task_node::<#output, #input>(TASK_NAME)
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
        (ExecutionClass::Isolated, Some(grace)) => {
            let value = proc_macro2::Literal::u32_suffixed(grace.value);
            (
                quote! { ::horsies::Execution::Isolated },
                quote! {
                    ::std::option::Option::Some(::horsies::__private::cancel_grace(#value))
                },
            )
        }
        (ExecutionClass::Isolated, None) => (
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
/// input, calls the function and stores what it returned. Its meta declares
/// the codes of the code type `code` (`UserErrorCode::CODES`); the
/// registration checks them.
fn registered_task(
    signature: &Signature,
    fn_name: &syn::Ident,
    input: &Type,
    code: &Type,
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
            .with_error_codes(<#code as ::horsies::UserErrorCode>::CODES)
    };
    let validate_call = match &signature.input {
        InputShape::Unit(_) => quote! { ::horsies::__private::decode_unit_input(input) },
        InputShape::Wrapped(_) => quote! {
            ::horsies::__private::check_input::<#input>(input)
        },
    };
    let validate = quote! {
        fn validate_input(
            &self,
            input: &[u8],
        ) -> ::std::result::Result<(), ::horsies::__private::StoredTaskError> {
            #validate_call
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
/// bindings that the call takes. A refusal returns the stored error. A task
/// whose input is `()` reads `{}` (a send, a workflow node) and `null`.
fn decode_statement(signature: &Signature, input_type: &Type) -> TokenStream {
    let pattern = match &signature.input {
        InputShape::Unit(_) => quote! { () },
        InputShape::Wrapped(params) => {
            let names = params.iter().map(|param| &param.ident);
            quote! { Input { #(#names),* } }
        }
    };
    let decode = match &signature.input {
        InputShape::Unit(_) => quote! { ::horsies::__private::decode_unit_input(&input) },
        InputShape::Wrapped(_) => {
            quote! { ::horsies::__private::decode_input::<#input_type>(&input) }
        }
    };
    quote! {
        let #pattern = match #decode {
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
        InputShape::Unit(UnitParameter::Absent) => vec![],
        InputShape::Unit(UnitParameter::Given(_)) => vec![quote! { () }],
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
        InputShape::Unit(UnitParameter::Absent) => vec![],
        InputShape::Unit(UnitParameter::Given(param)) => vec![&**param],
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
        InputShape::Unit(UnitParameter::Absent) => quote! { () },
        InputShape::Unit(UnitParameter::Given(param)) => param.ident.to_token_stream(),
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
/// return type, a `TaskRuntime` that is not the first parameter, a
/// parameter that is not a plain name, and a `_` parameter whose value is
/// stored (any but a single `()`).
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
    let input = input_shape(&user_params)?;
    Ok(Signature {
        takes_runtime,
        input,
    })
}

/// The input of the parameters after an optional `TaskRuntime`, as Python
/// stores keyword arguments: no parameter, or one of type `()`, is `()`;
/// any other parameters are a generated `Input` struct keyed by their
/// names, a single struct included. The rule reads only the syntax of `()`,
/// never a type's name.
fn input_shape(params: &[&syn::PatType]) -> syn::Result<InputShape> {
    match params {
        [] => Ok(InputShape::Unit(UnitParameter::Absent)),
        [single] if is_unit_type(&single.ty) => Ok(InputShape::Unit(UnitParameter::Given(
            Box::new(unit_param(single)?),
        ))),
        _ => params
            .iter()
            .map(|param| stored_param(param))
            .collect::<syn::Result<Vec<_>>>()
            .map(InputShape::Wrapped),
    }
}

/// The one `()` parameter: a plain name, or `_`. Its value is not stored,
/// so `_` needs no key; the send helpers name it `input`.
fn unit_param(param: &syn::PatType) -> syn::Result<Param> {
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

/// A parameter whose value the input stores under its name.
fn stored_param(param: &syn::PatType) -> syn::Result<Param> {
    match param.pat.as_ref() {
        Pat::Ident(pattern) => Ok(Param {
            ident: pattern.ident.clone(),
            ty: (*param.ty).clone(),
        }),
        Pat::Wild(_) => Err(syn::Error::new_spanned(
            &param.pat,
            "the input stores each task parameter under its name; name this parameter (`_` is \
             allowed only for a single `()` parameter)",
        )),
        other => Err(syn::Error::new_spanned(
            other,
            "task function parameters are plain names",
        )),
    }
}

fn is_task_runtime(ty: &Type) -> bool {
    last_segment_is(ty, "TaskRuntime")
}

/// The deepest nesting of parentheses around `()` that [`is_unit_type`]
/// reads through; a deeper type is not taken as `()`.
const MAX_PAREN_DEPTH: usize = 8;

/// Whether `ty` is written as `()`, inside at most [`MAX_PAREN_DEPTH`]
/// parentheses. A type alias of `()` is not seen as `()`: the attribute
/// reads syntax, not definitions, so such a parameter is stored under its
/// name (`{"name": null}`).
fn is_unit_type(ty: &Type) -> bool {
    let mut current = ty;
    for _level in 0..=MAX_PAREN_DEPTH {
        match current {
            Type::Paren(inner) => current = inner.elem.as_ref(),
            Type::Group(inner) => current = inner.elem.as_ref(),
            Type::Tuple(tuple) => return tuple.elems.is_empty(),
            _ => return false,
        }
    }
    false
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
        InputShape::Unit(_) => syn::parse_quote!(()),
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

#[cfg(test)]
mod tests {
    use super::{input_shape, InputShape, UnitParameter};

    /// The parameters of `func`, as the attribute reads them.
    fn shape_of(func: syn::ItemFn) -> syn::Result<InputShape> {
        let params: Vec<syn::PatType> = func
            .sig
            .inputs
            .into_iter()
            .filter_map(|argument| match argument {
                syn::FnArg::Typed(typed) => Some(typed),
                syn::FnArg::Receiver(_) => None,
            })
            .collect();
        let borrowed: Vec<&syn::PatType> = params.iter().collect();
        input_shape(&borrowed)
    }

    /// The field names of a wrapped input; `None` for a `()` input.
    fn wrapped_names(shape: &InputShape) -> Option<Vec<String>> {
        match shape {
            InputShape::Unit(_) => None,
            InputShape::Wrapped(params) => {
                Some(params.iter().map(|param| param.ident.to_string()).collect())
            }
        }
    }

    #[test]
    fn no_parameter_and_a_single_unit_parameter_are_the_unit_input() {
        let none = shape_of(syn::parse_quote!(
            fn f() {}
        ))
        .expect("no parameter");
        assert!(matches!(none, InputShape::Unit(UnitParameter::Absent)));
        let unit_cases: [syn::ItemFn; 3] = [
            syn::parse_quote!(
                fn f(_: ()) {}
            ),
            syn::parse_quote!(
                fn f(nothing: ()) {}
            ),
            syn::parse_quote!(
                fn f(nothing: (())) {}
            ),
        ];
        for func in unit_cases {
            let shape = shape_of(func).expect("one () parameter");
            assert!(matches!(shape, InputShape::Unit(UnitParameter::Given(_))));
        }
    }

    #[test]
    fn every_other_single_parameter_is_stored_under_its_name() {
        let single_cases: [syn::ItemFn; 8] = [
            syn::parse_quote!(
                fn f(order: Order) {}
            ),
            syn::parse_quote!(
                fn f(order: crate::orders::Order) {}
            ),
            syn::parse_quote!(
                fn f(order: Box<Order>) {}
            ),
            syn::parse_quote!(
                fn f(order: OrderKind) {}
            ),
            syn::parse_quote!(
                fn f(order: i64) {}
            ),
            syn::parse_quote!(
                fn f(order: Option<Order>) {}
            ),
            syn::parse_quote!(
                fn f(order: TaskOutcome<i64, Infallible>) {}
            ),
            syn::parse_quote!(
                fn f(order: Box<()>) {}
            ),
        ];
        for func in single_cases {
            let shape = shape_of(func).expect("one parameter");
            assert_eq!(wrapped_names(&shape), Some(vec!["order".to_owned()]));
        }
    }

    #[test]
    fn several_parameters_are_each_stored_under_their_names() {
        let shape = shape_of(syn::parse_quote!(
            fn f(order: Order, nothing: (), count: u32) {}
        ))
        .expect("three parameters");
        assert_eq!(
            wrapped_names(&shape),
            Some(vec![
                "order".to_owned(),
                "nothing".to_owned(),
                "count".to_owned()
            ])
        );
    }

    #[test]
    fn a_wildcard_parameter_whose_value_is_stored_is_refused() {
        let refused: [syn::ItemFn; 2] = [
            syn::parse_quote!(
                fn f(_: Order) {}
            ),
            syn::parse_quote!(
                fn f(count: u32, _: ()) {}
            ),
        ];
        for func in refused {
            let error = match shape_of(func) {
                Ok(_) => panic!("a stored `_` parameter must be refused"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("under its name"), "{error}");
        }
    }

    #[test]
    fn unit_inside_parentheses_past_the_depth_bound_is_stored() {
        let deep = shape_of(syn::parse_quote!(
            fn f(nothing: ((((((((((()))))))))))) {}
        ))
        .expect("one parameter");
        assert_eq!(wrapped_names(&deep), Some(vec!["nothing".to_owned()]));
    }
}
