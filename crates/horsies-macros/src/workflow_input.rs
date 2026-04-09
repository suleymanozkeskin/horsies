use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields};

pub fn derive_workflow_input(input: DeriveInput) -> syn::Result<TokenStream> {
    let ident = input.ident;
    let generics = input.generics;
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

    let field_fns = fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_ty = &field.ty;
        let fn_ident = format_ident!("field_{}", field_ident);
        let field_name = field_ident.to_string();
        quote! {
            pub fn #fn_ident() -> horsies::InputField<Self, #field_ty> {
                horsies::InputField::new(#field_name)
            }
        }
    });

    Ok(quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            #(#field_fns)*
        }
    })
}
