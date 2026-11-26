//! `#[derive(CustomErrorCode)]`: the code type of a task's own errors.
//!
//! The enum has unit variants only. Each variant's code is its name in
//! UPPER_SNAKE_CASE. The derive refuses at compile time: a struct or a
//! union, generics, a variant with fields, two variants with one code, and
//! (through a `const` assertion over the core's catalogs) a code with the
//! name of a built-in or reserved code.

use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::ext::IdentExt;
use syn::{Data, DeriveInput, Fields, Ident};

/// One variant and its code.
struct CodeVariant {
    ident: Ident,
    code: String,
    span: Span,
}

pub fn derive_custom_error_code(input: DeriveInput) -> syn::Result<TokenStream> {
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "CustomErrorCode can only be derived for an enum of unit variants",
        ));
    };
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "CustomErrorCode cannot be derived for a generic enum",
        ));
    }
    let variants = data
        .variants
        .iter()
        .map(code_variant)
        .collect::<syn::Result<Vec<_>>>()?;
    refuse_repeated_codes(&variants)?;
    Ok(render(&input.ident, &variants))
}

fn code_variant(variant: &syn::Variant) -> syn::Result<CodeVariant> {
    match &variant.fields {
        Fields::Unit => Ok(CodeVariant {
            ident: variant.ident.clone(),
            code: upper_snake_case(&variant.ident.unraw().to_string()),
            span: variant.ident.span(),
        }),
        Fields::Named(_) | Fields::Unnamed(_) => Err(syn::Error::new_spanned(
            variant,
            "a CustomErrorCode variant has no fields: its code is its name",
        )),
    }
}

fn refuse_repeated_codes(variants: &[CodeVariant]) -> syn::Result<()> {
    for (index, variant) in variants.iter().enumerate() {
        if let Some(earlier) = variants[..index]
            .iter()
            .find(|earlier| earlier.code == variant.code)
        {
            return Err(syn::Error::new(
                variant.span,
                format!(
                    "variants `{}` and `{}` have the same code {}",
                    earlier.ident, variant.ident, variant.code
                ),
            ));
        }
    }
    Ok(())
}

fn render(ident: &Ident, variants: &[CodeVariant]) -> TokenStream {
    let idents: Vec<&Ident> = variants.iter().map(|variant| &variant.ident).collect();
    let codes: Vec<&str> = variants
        .iter()
        .map(|variant| variant.code.as_str())
        .collect();
    let checks = variants.iter().map(|variant| {
        let code = &variant.code;
        let message = format!(
            "custom error code {code} of variant `{}` is the name of a built-in or reserved \
             code; rename the variant",
            variant.ident
        );
        quote_spanned! {variant.span=>
            const _: () = ::core::assert!(::horsies::__private::is_free_custom_code(#code), #message);
        }
    });
    let as_str_body = match variants.is_empty() {
        true => quote! { match *self {} },
        false => quote! { match self { #(Self::#idents => #codes,)* } },
    };
    let from_code_body = match variants.is_empty() {
        true => quote! {
            let _ = text;
            ::core::option::Option::None
        },
        false => quote! {
            match text {
                #(#codes => ::core::option::Option::Some(Self::#idents),)*
                _ => ::core::option::Option::None,
            }
        },
    };
    quote! {
        #(#checks)*

        impl ::horsies::CustomErrorCode for #ident {
            const CODES: &'static [&'static str] = &[#(#codes),*];

            fn as_str(&self) -> &'static str {
                #as_str_body
            }

            fn from_code(text: &str) -> ::core::option::Option<Self> {
                #from_code_body
            }
        }
    }
}

/// `RateLimited` is `RATE_LIMITED`; `HTTPError` is `HTTP_ERROR`;
/// `Http2Failed` is `HTTP2_FAILED`. A word starts at an upper-case letter
/// that follows a lower-case letter or a digit, and at the last upper-case
/// letter of a run that a lower-case letter follows.
pub(crate) fn upper_snake_case(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut code = String::with_capacity(name.len() + 4);
    for (index, &character) in chars.iter().enumerate() {
        let starts_word = match (
            index.checked_sub(1).map(|previous| chars[previous]),
            chars.get(index + 1),
        ) {
            (None, _) => false,
            (Some(previous), next) => {
                character.is_uppercase()
                    && (previous.is_lowercase()
                        || previous.is_ascii_digit()
                        || (previous.is_uppercase()
                            && next.is_some_and(|next| next.is_lowercase())))
            }
        };
        if starts_word {
            code.push('_');
        }
        code.extend(character.to_uppercase());
    }
    code
}

#[cfg(test)]
mod tests {
    use super::upper_snake_case;

    #[test]
    fn variant_names_become_upper_snake_case() {
        for (name, code) in [
            ("RateLimited", "RATE_LIMITED"),
            ("Declined", "DECLINED"),
            ("HTTPError", "HTTP_ERROR"),
            ("Http2Failed", "HTTP2_FAILED"),
            ("IOTimeout", "IO_TIMEOUT"),
            ("A", "A"),
            ("Stage3B", "STAGE3_B"),
        ] {
            assert_eq!(upper_snake_case(name), code, "{name}");
        }
    }
}
