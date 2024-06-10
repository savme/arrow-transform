use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{
    parse_macro_input, punctuated::Punctuated, Expr, FnArg, Ident, ItemFn, Lit, Meta, PatType,
    TypePath,
};

/// cloudquery_transform is an entrypoint for a transformation function.
///
/// For example, to create a transform that modifies all tables:
/// ```
/// #[cloudquery_transform]
/// pub fn modify_all(record: RecordBatch) -> RecordBatch {
///     arrow::record_batch::RecordBatch::new_empty(record.schema())
/// }
/// ```
///
/// Or to create a transform for a subset of tables:
/// ```
/// #[cloudquery_transform(table = "my_table_prefix_*")]
/// pub fn modify_some(record: RecordBatch) -> RecordBatch {
///     arrow::record_batch::RecordBatch::new_empty(record.schema())
/// }
/// ```
#[proc_macro_attribute]
pub fn cloudquery_transform(attr: TokenStream, item: TokenStream) -> TokenStream {
    let parsed = {
        let input = item.clone();
        parse_macro_input!(input as ItemFn)
    };

    let args = parse_macro_input!(attr with Punctuated::<Meta, syn::Token![,]>::parse_terminated);
    let table_arg = args
        .iter()
        .find(|m| m.path().is_ident("table"))
        .map(|v| v.require_name_value().map(|v| v.value.to_owned()).ok())
        .and_then(|v| v);

    let table_filter = match table_arg {
        Some(Expr::Lit(l, ..)) => match l.lit {
            Lit::Str(v) => v.token().to_string(),
            _ => "*".to_owned(),
        },
        _ => "*".to_owned(),
    };

    if parsed.sig.inputs.len() != 1 {
        panic!(
            "cloudquery_transform expects a function with the following signature:
         fn(arrow::record_batch::RecordBatch) -> arrow::record_batch::RecordBatch"
        )
    }

    let is_valid = match &parsed.sig.inputs.iter().next() {
        Some(FnArg::Typed(PatType { ty, .. })) => match &**ty {
            syn::Type::Path(TypePath { path, .. })
                if path
                    .segments
                    .last()
                    .is_some_and(|v| v.ident == "RecordBatch") =>
            {
                true
            }
            _ => false,
        },
        _ => false,
    };

    if !is_valid {
        panic!(
            "cloudquery_transform expects a function with the following signature:
    fn(arrow::record_batch::RecordBatch) -> arrow::record_batch::RecordBatch"
        )
    }

    let original = {
        let i = item.clone();
        parse_macro_input!(i as ItemFn)
    };

    let fn_name = parsed.sig.ident;
    let wn = Ident::new(&format!("_wrapper_{}", fn_name), Span::call_site());
    let export_name = format!("_cqtransform_{}@@{}", table_filter, fn_name);
    let fn_name_literal = fn_name.to_string();

    let expanded = quote! {
        #[cfg_attr(all(target_arch = "wasm32"), export_name = #export_name)]
        pub fn #wn(ptr: u32, size: u32) -> u64 {
            use cloudquery_sdk::arrow::ipc::reader::StreamReader;
            use cloudquery_sdk::arrow::ipc::writer::StreamWriter;
            use cloudquery_sdk::bytes::{Buf, BufMut};
            use cloudquery_sdk::log;

            let v = Box::new(unsafe { Vec::from_raw_parts(ptr as *mut u8, size as usize, size as usize) })
                .leak();

            log(&format!("{}: new arrow batch: {} (buffer size)", #fn_name_literal, v.len()));

            let st = StreamReader::try_new_unbuffered(v.reader(), None);
            if let Some(err) = st.as_ref().err() {
                log(&err.to_string());
                unimplemented!();
            }

            let st = st.unwrap();
            let schema = &st.schema();

            let mut output_bytes = Vec::<u8>::default().writer();
            {
                let mut writer = StreamWriter::try_new(&mut output_bytes, schema).unwrap();
                st.into_iter()
                    .flat_map(|batch| batch)
                    .map(|rec| #fn_name(rec))
                    .for_each(|rec| {
                        writer.write(&rec).unwrap();
                    });

                writer.finish().unwrap();
            }

            let finished_writer = output_bytes.get_mut();
            let out_ptr = Box::into_raw(finished_writer.clone().into_boxed_slice()) as *mut u8;
            return ((out_ptr as u64) << 32) | finished_writer.len() as u64;
        }

        #original
    };

    TokenStream::from(expanded)
}
