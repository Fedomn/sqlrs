use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures::stream::BoxStream;

use super::{TableFunction, TableFunctionBindInput, TableFunctionInput};
use crate::catalog_v2::{Catalog, CatalogEntry, DEFAULT_SCHEMA};
use crate::execution::SchemaUtil;
use crate::function::{BuiltinFunctions, FunctionData, FunctionError, FunctionResult};
use crate::main_entry::ClientContext;
use crate::types_v2::{LogicalType, ScalarValue};

pub struct SqlrsTablesFunc;

#[derive(new, Debug, Clone)]
pub struct SqlrsTablesData {
    pub(crate) entries: Vec<CatalogEntry>,
    pub(crate) return_types: Vec<LogicalType>,
    pub(crate) return_names: Vec<String>,
}

impl SqlrsTablesFunc {
    fn generate_sqlrs_tables_names() -> Vec<String> {
        vec![
            "schema_name".to_string(),
            "schema_oid".to_string(),
            "table_name".to_string(),
            "table_oid".to_string(),
        ]
    }

    fn generate_sqlrs_tables_types() -> Vec<LogicalType> {
        vec![
            LogicalType::Varchar,
            LogicalType::Integer,
            LogicalType::Varchar,
            LogicalType::Integer,
        ]
    }

    fn bind_func(
        context: Arc<ClientContext>,
        _input: TableFunctionBindInput,
        return_types: &mut Vec<LogicalType>,
        return_names: &mut Vec<String>,
    ) -> FunctionResult<Option<FunctionData>> {
        let entries = Catalog::scan_entries(context, DEFAULT_SCHEMA.to_string(), &|entry| {
            matches!(entry, CatalogEntry::TableCatalogEntry(_))
        })?;
        let data = SqlrsTablesData::new(
            entries,
            Self::generate_sqlrs_tables_types(),
            Self::generate_sqlrs_tables_names(),
        );
        return_types.extend(data.return_types.clone());
        return_names.extend(data.return_names.clone());
        Ok(Some(FunctionData::SqlrsTablesData(Box::new(data))))
    }

    fn tables_func(
        _context: Arc<ClientContext>,
        input: &TableFunctionInput,
    ) -> FunctionResult<BoxStream<'static, FunctionResult<RecordBatch>>> {
        if let Some(FunctionData::SqlrsTablesData(data)) = &input.bind_data {
            let schema = SchemaUtil::new_schema_ref(&data.return_names, &data.return_types);
            let mut schema_names = ScalarValue::new_builder(&LogicalType::Varchar)?;
            let mut schema_oids = ScalarValue::new_builder(&LogicalType::Integer)?;
            let mut table_names = ScalarValue::new_builder(&LogicalType::Varchar)?;
            let mut table_oids = ScalarValue::new_builder(&LogicalType::Integer)?;
            for entry in data.entries.iter() {
                if let CatalogEntry::TableCatalogEntry(table) = entry {
                    ScalarValue::append_for_builder(
                        &ScalarValue::Utf8(Some(table.schema_base.name.clone())),
                        &mut schema_names,
                    )?;
                    ScalarValue::append_for_builder(
                        &ScalarValue::Int32(Some(table.schema_base.oid as i32)),
                        &mut schema_oids,
                    )?;
                    ScalarValue::append_for_builder(
                        &ScalarValue::Utf8(Some(table.base.name.clone())),
                        &mut table_names,
                    )?;
                    ScalarValue::append_for_builder(
                        &ScalarValue::Int32(Some(table.base.oid as i32)),
                        &mut table_oids,
                    )?;
                }
            }
            let cols = vec![
                schema_names.finish(),
                schema_oids.finish(),
                table_names.finish(),
                table_oids.finish(),
            ];
            let batch = RecordBatch::try_new(schema, cols)?;
            let stream = Box::pin(async_stream::try_stream! {
                yield batch;
            });
            Ok(stream)
        } else {
            Err(FunctionError::InternalError(
                "unexpected global state type".to_string(),
            ))
        }
    }

    pub fn register_function(set: &mut BuiltinFunctions) -> Result<(), FunctionError> {
        set.add_table_functions(TableFunction::new(
            "sqlrs_tables".to_string(),
            Some(Self::bind_func),
            Self::tables_func,
        ))?;
        Ok(())
    }
}
