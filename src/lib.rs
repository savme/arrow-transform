use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, RecordBatch, StringArray, StringBuilder},
    datatypes::DataType,
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use bytes::{Buf, BufMut};

use cloudquery_sdk::cloudquery_transform;
use cloudquery_sdk::log;

#[cloudquery_transform]
pub fn reverse_all_strings(
    record: arrow::record_batch::RecordBatch,
) -> arrow::record_batch::RecordBatch {
    let schema = record.schema();

    let new_columns = record
        .columns()
        .iter()
        .map(|c| match c.data_type() {
            DataType::Utf8 => {
                let string_array = c.as_any().downcast_ref::<StringArray>().unwrap();
                let mut builder = StringBuilder::new();

                for i in 0..string_array.len() {
                    let value = string_array.value(i);
                    builder.append_value(value.chars().rev().collect::<String>());
                }

                return Arc::new(builder.finish()) as ArrayRef;
            }
            _ => c.clone(),
        })
        .collect();

    RecordBatch::try_new(schema, new_columns).unwrap()
}

#[cloudquery_transform(table = "aws_s3_bucket_loggings")]
fn uppercase(record: RecordBatch) -> RecordBatch {
    let schema = record.schema();

    let new_columns = record
        .columns()
        .iter()
        .map(|c| match c.data_type() {
            DataType::Utf8 => {
                let string_array = c.as_any().downcast_ref::<StringArray>().unwrap();
                let mut builder = StringBuilder::new();

                for i in 0..string_array.len() {
                    let value = string_array.value(i);
                    builder.append_value(
                        value
                            .chars()
                            .map(|c| c.to_ascii_uppercase())
                            .collect::<String>(),
                    );
                }

                return Arc::new(builder.finish()) as ArrayRef;
            }
            _ => c.clone(),
        })
        .collect();

    RecordBatch::try_new(schema, new_columns).unwrap()
}
