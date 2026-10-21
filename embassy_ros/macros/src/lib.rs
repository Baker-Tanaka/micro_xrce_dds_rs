//! `#[embassy_ros::node]` attribute macro.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{
    parse::Parser, parse_macro_input, punctuated::Punctuated, spanned::Spanned, Expr, ExprLit,
    FnArg, ItemFn, Lit, Meta, Token,
};

enum Kind {
    Publisher,
    Subscription,
}

/// Turns
///
/// ```ignore
/// #[embassy_ros::node(name = "imu", pool_size = 1)]
/// async fn node(node: Node, device: Imu, #[publisher(topic = &DATA)] p: Publisher<Imu>) { .. }
/// ```
///
/// into `fn node(&NodeSpawner, Imu) -> Result<(), StartupError>` which
/// registers the node and its endpoints (local only) and spawns the task.
/// The first parameter must be the `Node`; at most one un-attributed
/// resources parameter may follow (omitted => `()`); every other parameter
/// needs `#[publisher(topic = EXPR)]` or `#[subscription(topic = EXPR)]`
/// where `EXPR: &'static Topic<M, N>`.
#[proc_macro_attribute]
pub fn node(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = match Punctuated::<Meta, Token![,]>::parse_terminated.parse(args) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };
    let item = parse_macro_input!(item as ItemFn);
    match expand(args, item) {
        Ok(t) => t.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(args: Punctuated<Meta, Token![,]>, item: ItemFn) -> syn::Result<TokenStream2> {
    let mut name: Option<syn::LitStr> = None;
    let mut pool_size: Option<Expr> = None;
    for meta in args {
        match &meta {
            Meta::NameValue(nv) if nv.path.is_ident("name") => match &nv.value {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(s), ..
                }) => name = Some(s.clone()),
                other => {
                    return Err(syn::Error::new(
                        other.span(),
                        "`name` must be a string literal",
                    ))
                }
            },
            Meta::NameValue(nv) if nv.path.is_ident("pool_size") => {
                pool_size = Some(nv.value.clone())
            }
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    "expected `name = \"..\"` or `pool_size = N`",
                ))
            }
        }
    }
    let name = name.ok_or_else(|| syn::Error::new(Span::call_site(), "missing `name = \"..\"`"))?;

    let sig = &item.sig;
    if sig.asyncness.is_none() {
        return Err(syn::Error::new(
            sig.span(),
            "node functions must be `async fn`",
        ));
    }
    if !sig.generics.params.is_empty() || sig.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            sig.generics.span(),
            "node functions cannot be generic",
        ));
    }
    if let syn::ReturnType::Type(_, ty) = &sig.output {
        return Err(syn::Error::new(
            ty.span(),
            "node functions must return `()`",
        ));
    }

    let vis = item.vis.clone();
    let fn_name = sig.ident.clone();
    let mut task_params = Vec::new();
    let mut bindings = Vec::new();
    let mut call_args = Vec::new();
    let mut endpoint_setup = Vec::new();
    let mut resources_ty: Option<syn::Type> = None;

    let inputs: Vec<FnArg> = item.sig.inputs.iter().cloned().collect();
    for (i, input) in inputs.into_iter().enumerate() {
        let FnArg::Typed(mut pt) = input else {
            return Err(syn::Error::new(input.span(), "`self` is not allowed"));
        };
        let mut kind = None;
        let mut topic: Option<Expr> = None;
        let mut rest = Vec::new();
        for attr in pt.attrs.drain(..) {
            let k = if attr.path().is_ident("publisher") {
                Kind::Publisher
            } else if attr.path().is_ident("subscription") {
                Kind::Subscription
            } else {
                rest.push(attr);
                continue;
            };
            if kind.is_some() {
                return Err(syn::Error::new(attr.span(), "duplicate endpoint attribute"));
            }
            attr.parse_nested_meta(|m| {
                if m.path.is_ident("topic") {
                    topic = Some(m.value()?.parse()?);
                    Ok(())
                } else {
                    Err(m.error("expected `topic = <&'static Topic>`"))
                }
            })?;
            if topic.is_none() {
                return Err(syn::Error::new(attr.span(), "missing `topic = ...`"));
            }
            kind = Some(k);
        }
        pt.attrs = rest;
        let arg = format_ident!("__embassy_ros_arg{}", i);
        let pat = &pt.pat;
        let ty = &pt.ty;
        task_params.push(quote!(#arg: #ty));
        bindings.push(quote!(let #pat = #arg;));
        if i == 0 {
            if kind.is_some() {
                return Err(syn::Error::new(
                    pt.span(),
                    "the first parameter must be the `Node`",
                ));
            }
            call_args.push(quote!(__embassy_ros_node));
            continue;
        }
        match kind {
            None => {
                if resources_ty.is_some() {
                    return Err(syn::Error::new(
                        pt.span(),
                        "only one resources parameter is allowed; annotate endpoints with #[publisher]/#[subscription]",
                    ));
                }
                resources_ty = Some((*pt.ty).clone());
                call_args.push(quote!(__embassy_ros_resources));
            }
            Some(k) => {
                let topic = topic.unwrap();
                let method = match k {
                    Kind::Publisher => quote!(publisher),
                    Kind::Subscription => quote!(subscription),
                };
                endpoint_setup.push(quote! {
                    let #arg = __embassy_ros_builder.#method(#topic)?;
                });
                call_args.push(quote!(#arg));
            }
        }
    }
    if task_params.is_empty() {
        return Err(syn::Error::new(
            sig.span(),
            "the first parameter must be `node: Node`",
        ));
    }
    let resources_ty = resources_ty.unwrap_or_else(|| syn::parse_quote!(()));
    let body = &item.block;
    let attrs = &item.attrs;
    let pool = pool_size.map(|p| quote!(, pool_size = #p));

    Ok(quote! {
        #(#attrs)*
        #vis fn #fn_name(
            __embassy_ros_spawner: &::embassy_ros::NodeSpawner,
            __embassy_ros_resources: #resources_ty,
        ) -> ::core::result::Result<(), ::embassy_ros::StartupError> {
            #[::embassy_ros::__private::embassy_executor::task(
                embassy_executor = ::embassy_ros::__private::embassy_executor #pool
            )]
            async fn __embassy_ros_node_task(#(#task_params),*) {
                #(#bindings)*
                #body
            }

            let _ = &__embassy_ros_resources;
            let mut __embassy_ros_builder = __embassy_ros_spawner.__create_node(#name)?;
            #(#endpoint_setup)*
            let __embassy_ros_node = __embassy_ros_builder.finish();
            __embassy_ros_spawner.__spawn(#name, __embassy_ros_node_task(#(#call_args),*))
        }
    })
}
