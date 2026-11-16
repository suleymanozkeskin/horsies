use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::{Attribute, Data, DeriveInput, Expr, ExprLit, Fields, Lit, Meta, Token};

pub fn derive_workflow_input(input: DeriveInput) -> syn::Result<TokenStream> {
    let ident = input.ident;
    let generics = input.generics;
    let struct_attrs = input.attrs;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            Fields::Unnamed(fields) => {
                return Err(syn::Error::new_spanned(
                    fields,
                    "WorkflowInput can only be derived for named-field structs",
                ))
            }
            Fields::Unit => {
                return Err(syn::Error::new_spanned(
                    ident,
                    "WorkflowInput can only be derived for named-field structs",
                ))
            }
        },
        _other => {
            return Err(syn::Error::new_spanned(
                ident,
                "WorkflowInput can only be derived for named-field structs",
            ))
        }
    };

    // The wire key each field is (de)serialized under must match the input
    // struct's serde attributes, else `set`/`arg_from` write kwargs under the
    // Rust field name while deserialization expects the renamed key (C13).
    let rename_all = serde_string(&struct_attrs, "rename_all");

    let field_fns = fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_ty = &field.ty;
        let fn_ident = format_ident!("field_{}", field_ident);
        // Field-level `rename` wins; otherwise apply a struct-level `rename_all`
        // rule to the Rust field name; otherwise use the field name verbatim.
        let field_name = match serde_string(&field.attrs, "rename") {
            Some(name) => name,
            None => match &rename_all {
                Some(rule) => apply_rename_rule(rule, &field_ident.to_string()),
                None => field_ident.to_string(),
            },
        };
        quote! {
            pub fn #fn_ident() -> horsies::InputField<Self, #field_ty> {
                horsies::__private::input_field(#field_name)
            }
        }
    });

    Ok(quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            #(#field_fns)*
        }
    })
}

/// Read the deserialization-relevant string for `key` from a struct's or field's
/// `#[serde(...)]` attributes. Handles both `key = "..."` and the split
/// `key(deserialize = "...")` forms; a `serialize`-only rename does not change
/// the wire key the struct deserializes from, so it is intentionally ignored.
fn serde_string(attrs: &[Attribute], key: &str) -> Option<String> {
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let Meta::List(list) = &attr.meta else {
            continue;
        };
        let Ok(metas) = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        else {
            continue;
        };
        for meta in &metas {
            if let Some(value) = extract_serde_string(meta, key) {
                return Some(value);
            }
        }
    }
    None
}

fn extract_serde_string(meta: &Meta, key: &str) -> Option<String> {
    match meta {
        Meta::NameValue(nv) if nv.path.is_ident(key) => str_lit(&nv.value),
        Meta::List(list) if list.path.is_ident(key) => {
            let inner = list
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .ok()?;
            inner.iter().find_map(|m| match m {
                Meta::NameValue(nv) if nv.path.is_ident("deserialize") => str_lit(&nv.value),
                _ => None,
            })
        }
        _ => None,
    }
}

fn str_lit(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => Some(s.value()),
        _ => None,
    }
}

/// Apply a serde `rename_all` rule to a struct field name, matching serde's
/// `RenameRule::apply_to_field` semantics (field names are already snake_case).
fn apply_rename_rule(rule: &str, field: &str) -> String {
    match rule {
        "lowercase" | "snake_case" => field.to_owned(),
        "UPPERCASE" | "SCREAMING_SNAKE_CASE" => field.to_ascii_uppercase(),
        "PascalCase" => pascal_case(field),
        "camelCase" => {
            let pascal = pascal_case(field);
            let mut chars = pascal.chars();
            match chars.next() {
                Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        }
        "kebab-case" => field.replace('_', "-"),
        "SCREAMING-KEBAB-CASE" => field.to_ascii_uppercase().replace('_', "-"),
        // Unknown rule: leave the field name unchanged rather than guess.
        _ => field.to_owned(),
    }
}

fn pascal_case(field: &str) -> String {
    let mut out = String::new();
    let mut capitalize = true;
    for ch in field.chars() {
        if ch == '_' {
            capitalize = true;
        } else if capitalize {
            out.push(ch.to_ascii_uppercase());
            capitalize = false;
        } else {
            out.push(ch);
        }
    }
    out
}
