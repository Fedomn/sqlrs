use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;
use itertools::Itertools;

use super::{TableFunction, TableFunctionBindInput, TableFunctionInput};
use crate::catalog_v2::{Catalog, CatalogEntry, DEFAULT_SCHEMA};
use crate::execution::SchemaUtil;
use crate::function::{BuiltinFunctions, FunctionData, FunctionError};
use crate::main_entry::ClientContext;
use crate::types_v2::{LogicalType, ScalarValue};

pub struct SqlrsColumnsFunc;

#[derive(new, Debug, Clone)]
pub struct SqlrsColumnsData {
    pub(crate) entries: Vec<CatalogEntry>,
    pub(crate) return_types: Vec<LogicalType>,
    pub(crate) return_names: Vec<String>,
    pub(crate) current_cursor: usize,
}

impl SqlrsColumnsFunc {
    fn generate_sqlrs_tables_names() -> Vec<String> {
        vec![
            "table_name".to_string(),
            "column_names".to_string(),
            "column_types".to_string(),
        ]
    }

    fn generate_sqlrs_tables_types() -> Vec<LogicalType> {
        vec![
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ]
    }

    fn bind_func(
        context: Arc<ClientContext>,
        _input: TableFunctionBindInput,
        return_types: &mut Vec<LogicalType>,
        return_names: &mut Vec<String>,
    ) -> Result<Option<FunctionData>, FunctionError> {
        let entries = Catalog::scan_entries(context, DEFAULT_SCHEMA.to_string(), &|entry| {
            matches!(entry, CatalogEntry::TableCatalogEntry(_))
        })?;
        let data = SqlrsColumnsData::new(
            entries,
            Self::generate_sqlrs_tables_types(),
            Self::generate_sqlrs_tables_names(),
            0,
        );
        return_types.extend(data.return_types.clone());
        return_names.extend(data.return_names.clone());
        Ok(Some(FunctionData::SqlrsColumnsData(Box::new(data))))
    }

    fn tables_func(
        _context: Arc<ClientContext>,
        input: &mut TableFunctionInput,
    ) -> Result<Option<RecordBatch>, FunctionError> {
        if input.bind_data.is_none() {
            return Ok(None);
        }

        let bind_data = input.bind_data.as_mut().unwrap();
        if let FunctionData::SqlrsColumnsData(data) = bind_data {
            if data.current_cursor >= data.entries.len() {
                return Ok(None);
            }

            let schema = SchemaUtil::new_schema_ref(&data.return_names, &data.return_types);
            let mut table_name = ScalarValue::new_builder(&LogicalType::Varchar)?;
            let mut column_names = ScalarValue::new_builder(&LogicalType::Varchar)?;
            let mut column_types = ScalarValue::new_builder(&LogicalType::Varchar)?;
            for entry in data.entries.iter() {
                if let CatalogEntry::TableCatalogEntry(table) = entry {
                    ScalarValue::append_for_builder(
                        &ScalarValue::Utf8(Some(table.base.name.clone())),
                        &mut table_name,
                    )?;

                    let names = table.columns.iter().map(|col| col.name.clone()).join(", ");
                    let names = format!("[{}]", names);
                    let types = table.columns.iter().map(|col| col.ty.clone()).join(", ");
                    let types = format!("[{}]", types);
                    ScalarValue::append_for_builder(
                        &ScalarValue::Utf8(Some(names)),
                        &mut column_names,
                    )?;
                    ScalarValue::append_for_builder(
                        &ScalarValue::Utf8(Some(types)),
                        &mut column_types,
                    )?;
                }
            }
            let cols = vec![
                table_name.finish(),
                column_names.finish(),
                column_types.finish(),
            ];
            data.current_cursor += data.entries.len();
            let batch = RecordBatch::try_new(schema, cols)?;
            Ok(Some(batch))
        } else {
            Err(FunctionError::InternalError(
                "unexpected global state type".to_string(),
            ))
        }
    }

    pub fn register_function(set: &mut BuiltinFunctions) -> Result<(), FunctionError> {
        set.add_table_functions(TableFunction::new(
            "sqlrs_columns".to_string(),
            Some(Self::bind_func),
            Self::tables_func,
        ))?;
        Ok(())
    }
}
