use derive_new::new;

use super::BoundTableRef;
use crate::catalog_v2::{Catalog, CatalogEntry};
use crate::function::TableFunctionBindInput;
use crate::planner_v2::{
    BindError, Binder, LogicalGet, LogicalOperator, LogicalOperatorBase, SqlparserResolver,
};

/// Represents a reference to a table-producing function call
#[derive(new, Debug)]
pub struct BoundTableFunction {
    pub(crate) get: LogicalOperator,
}

impl Binder {
    pub fn bind_table_function(
        &mut self,
        table: sqlparser::ast::TableFactor,
    ) -> Result<BoundTableRef, BindError> {
        match table {
            sqlparser::ast::TableFactor::Table {
                name, alias, args, ..
            } => {
                let (schema, table_function_name) =
                    SqlparserResolver::object_name_to_schema_table(&name)?;
                let alias = alias
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| table_function_name.clone());

                let function = Catalog::get_table_function(
                    self.clone_client_context(),
                    schema,
                    table_function_name,
                )?;

                let table_func = function.functions[0].clone();
                let mut return_types = vec![];
                let mut return_names = vec![];
                let bind_data = if let Some(bind_func) = table_func.bind {
                    bind_func(
                        self.clone_client_context(),
                        TableFunctionBindInput::new(None, args),
                        &mut return_types,
                        &mut return_names,
                    )?
                } else {
                    None
                };

                let table_index = self.generate_table_index();
                let logical_get = LogicalGet::new(
                    LogicalOperatorBase::default(),
                    table_index,
                    table_func,
                    bind_data,
                    return_types.clone(),
                    return_names.clone(),
                );
                let plan = LogicalOperator::LogicalGet(logical_get);
                // now add the table function to the bind context so its columns can be bound
                self.bind_context.add_table_function(
                    alias,
                    table_index,
                    return_types,
                    return_names,
                    CatalogEntry::TableFunctionCatalogEntry(function),
                );
                let bound_ref =
                    BoundTableRef::BoundTableFunction(Box::new(BoundTableFunction::new(plan)));
                Ok(bound_ref)
            }
            other => Err(BindError::Internal(format!(
                "unexpected table type: {}, only bind TableFactor::Table",
                other
            ))),
        }
    }
}
