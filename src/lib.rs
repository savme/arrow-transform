use std::{mem::MaybeUninit, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, RecordBatch, StringArray, StringBuilder},
    datatypes::DataType,
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use bytes::{Buf, BufMut};

#[link(wasm_import_module = "env")]
extern "C" {
    #[link_name = "log"]
    fn _log(ptr: u32, size: u32);

}

unsafe fn log(message: &String) {
    let (ptr, len) = (message.as_ptr() as u32, message.len() as u32);
    _log(ptr, len);
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "cloudquery_transform")]
pub unsafe extern "C" fn cloudquery_transform(ptr: u32, size: u32) -> u64 {
    let v = Box::new(Vec::from_raw_parts(
        ptr as *mut u8,
        size as usize,
        size as usize,
    ))
    .leak();
    log(&format!("new arrow batch: {} (buffer size)", v.len()));

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

        st.into_iter().flat_map(|batch| batch).for_each(|rec| {
            let schema = rec.schema();

            let new_columns = rec
                .columns()
                .iter()
                .enumerate()
                .map(|(idx, c)| {
                    let column_name = schema.field(idx).name();
                    log(&format!(
                        "column {} is of type {:?}",
                        column_name,
                        c.data_type()
                    ));

                    if c.data_type() == &DataType::Utf8 {
                        let string_array = c.as_any().downcast_ref::<StringArray>().unwrap();
                        let mut builder = StringBuilder::new();

                        for i in 0..string_array.len() {
                            let value = string_array.value(i);
                            builder.append_value(value.chars().rev().collect::<String>());
                        }

                        return Arc::new(builder.finish()) as ArrayRef;
                    }

                    return c.clone();
                })
                .collect();

            let new_records = RecordBatch::try_new(schema, new_columns).unwrap();
            writer.write(&new_records).unwrap();
        });

        writer.finish().unwrap();
    }

    let finished_writer = output_bytes.get_mut();
    let out_ptr = Box::into_raw(finished_writer.clone().into_boxed_slice()) as *mut u8;
    log(&format!("buffer length: {}", finished_writer.len()));

    return ((out_ptr as u64) << 32) | finished_writer.len() as u64;
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "allocate")]
#[no_mangle]
pub extern "C" fn allocate(size: usize) -> *mut u8 {
    let vec: Vec<MaybeUninit<u8>> = Vec::with_capacity(size);
    Box::into_raw(vec.into_boxed_slice()) as *mut u8
}
