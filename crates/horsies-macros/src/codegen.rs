use proc_macro2::TokenStream;
use quote::quote;
use syn::{FnArg, ItemFn, ReturnType, Type};

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

    // Task options body
    let task_options_body = build_task_options_body(&attrs);

    // Macro call: async_task_fn! or blocking_task_fn!
    let registered_task = if signature.accepts_runtime() {
        build_runtime_registered_task(&signature, fn_name, &args_type, blocking)
    } else if blocking {
        quote! { horsies::blocking_task_fn!(super::#fn_name, #args_type) }
    } else {
        quote! { horsies::async_task_fn!(super::#fn_name, #args_type) }
    };
    let runtime_capture = if signature.accepts_runtime() {
        quote! { let __horsies_runtime = app.task_runtime(); }
    } else {
        quote! {}
    };

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

            /// Register this task on the app and return a typed `TaskFunction`.
            ///
            /// This is the only public entry point. The macro generates direct
            /// builder calls — no intermediate descriptor trait involved.
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
                builder.register()
            }
        }
    })
}

#[derive(Clone)]
enum SignatureShape {
    ArgsOnly,
    RuntimeOnly,
    RuntimeAndArgs,
}

impl SignatureShape {
    fn accepts_runtime(&self) -> bool {
        matches!(self, Self::RuntimeOnly | Self::RuntimeAndArgs)
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

    // Task signatures may be either `(args)`, `(TaskRuntime)`, or
    // `(TaskRuntime, args)`.
    let typed_args: Vec<_> = sig
        .inputs
        .iter()
        .filter_map(|a| match a {
            FnArg::Typed(t) => Some(t),
            _ => None,
        })
        .collect();

    let signature = match typed_args.as_slice() {
        [arg] if is_task_runtime_type(&arg.ty) => SignatureShape::RuntimeOnly,
        [arg] if !is_task_runtime_type(&arg.ty) => SignatureShape::ArgsOnly,
        [runtime, _args] if is_task_runtime_type(&runtime.ty) => SignatureShape::RuntimeAndArgs,
        [_first, second] => {
            let message = if is_task_runtime_type(&second.ty) {
                "task functions may inject TaskRuntime only as the first argument"
            } else {
                "task functions may accept either (args) or (TaskRuntime, args)"
            };
            return Err(syn::Error::new_spanned(&sig.inputs, message));
        }
        _ => {
            return Err(syn::Error::new_spanned(
                &sig.inputs,
                "task functions must accept either one argument (args) or two arguments (TaskRuntime, args)",
            ));
        }
    };

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
    let sig = &func.sig;

    let typed_args: Vec<_> = sig
        .inputs
        .iter()
        .filter_map(|a| match a {
            FnArg::Typed(t) => Some(t.clone()),
            _ => None,
        })
        .collect();

    let args_type = match signature {
        SignatureShape::ArgsOnly => (*typed_args[0].ty).clone(),
        SignatureShape::RuntimeOnly => syn::parse_quote!(()),
        SignatureShape::RuntimeAndArgs => (*typed_args[1].ty).clone(),
    };

    // Output type: extract T from Result<T, TaskError>.
    let output_type = match &sig.output {
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

fn build_runtime_registered_task(
    signature: &SignatureShape,
    fn_name: &syn::Ident,
    args_type: &Type,
    blocking: bool,
) -> TokenStream {
    let call_expr = match signature {
        SignatureShape::RuntimeOnly => {
            if blocking {
                quote! { super::#fn_name(runtime) }
            } else {
                quote! { super::#fn_name(runtime).await }
            }
        }
        SignatureShape::RuntimeAndArgs => {
            if blocking {
                quote! { super::#fn_name(runtime, deserialized) }
            } else {
                quote! { super::#fn_name(runtime, deserialized).await }
            }
        }
        SignatureShape::ArgsOnly => {
            unreachable!("runtime wrapper only built for runtime signatures")
        }
    };

    if blocking {
        quote! {{
            struct __BlockingTaskWrapper {
                runtime: horsies::TaskRuntime,
            }

            impl horsies::core::task::fn_trait::BlockingTaskFn for __BlockingTaskWrapper {
                fn execute(&self, args: &[u8]) -> horsies::core::task::fn_trait::RawTaskResult {
                    let runtime = self.runtime.clone();
                    let deserialized: #args_type = match horsies::core::task::macros::decode_task_input(args) {
                        Ok(v) => v,
                        Err(task_error) => {
                            return horsies::TaskResult::Err(task_error);
                        }
                    };

                    match #call_expr {
                        Ok(value) => horsies::core::task::macros::encode_validated_task_output(&value),
                        Err(task_error) => horsies::TaskResult::Err(task_error),
                    }
                }
            }

            horsies::core::task::fn_trait::RegisteredTask::Blocking {
                task: ::std::sync::Arc::new(__BlockingTaskWrapper {
                    runtime: __horsies_runtime.clone(),
                }),
                meta: horsies::core::task::fn_trait::TaskMeta::default(),
            }
        }}
    } else {
        quote! {{
            struct __AsyncTaskWrapper {
                runtime: horsies::TaskRuntime,
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
                    let runtime = self.runtime.clone();
                    Box::pin(async move {
                        let deserialized: #args_type = match horsies::core::task::macros::decode_task_input(&args) {
                            Ok(v) => v,
                            Err(task_error) => {
                                return horsies::TaskResult::Err(task_error);
                            }
                        };

                        match #call_expr {
                            Ok(value) => horsies::core::task::macros::encode_validated_task_output(&value),
                            Err(task_error) => horsies::TaskResult::Err(task_error),
                        }
                    })
                }
            }

            horsies::core::task::fn_trait::RegisteredTask::Async {
                task: ::std::sync::Arc::new(__AsyncTaskWrapper {
                    runtime: __horsies_runtime.clone(),
                }),
                meta: horsies::core::task::fn_trait::TaskMeta::default(),
            }
        }}
    }
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
    let has_good_until = attrs.good_until.is_some();
    let has_auto_retry = attrs.auto_retry_for.is_some();

    if !has_retry && !has_good_until && !has_auto_retry {
        return quote! { None };
    }

    let retry_policy_expr = match &attrs.retry_policy {
        Some(expr) => quote! { Some(#expr) },
        None => quote! { None },
    };

    let good_until_expr = match &attrs.good_until {
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

    // task_name and queue_name are normalized by register(), so we use
    // placeholder values here — the builder overwrites them.
    quote! {
        Some(horsies::TaskOptions {
            task_name: String::new(),
            queue_name: None,
            retry_policy: #retry_policy_expr,
            good_until: #good_until_expr,
            auto_retry_for: #auto_retry_for_expr,
        })
    }
}
