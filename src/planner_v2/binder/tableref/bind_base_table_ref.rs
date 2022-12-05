use derive_new::new;

use super::BoundTableRef;
use crate::catalog_v2::{Catalog, CatalogEntry, TableCatalogEntry};
use crate::planner_v2::{
    BindError, Binder, LogicalGet, LogicalOperator, LogicalOperatorBase, SqlparserResolver,
};

/// Represents a TableReference to a base table in the schema
#[derive(new, Debug)]
pub struct BoundBaseTableRef {
    #[allow(dead_code)]
    pub(crate) table: TableCatalogEntry,
    pub(crate) get: LogicalOperator,
}

impl Binder {
    pub fn bind_base_table_ref(
        &mut self,
        table: sqlparser::ast::TableFactor,
    ) -> Result<BoundTableRef, BindError> {
        match table {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                let table_index = self.generate_table_index();
                let (schema, table) = SqlparserResolver::object_name_to_schema_table(&name)?;
                let alias = alias
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| table.clone());
                let table = Catalog::get_table(self.clone_client_context(), schema, table)?;

                let mut return_names = vec![];
                let mut return_types = vec![];
                for col in table.columns.iter() {
                    return_names.push(col.name.clone());
                    return_types.push(col.ty.clone());
                }
                let logical_get = LogicalGet::new(
                    LogicalOperatorBase::default(),
                    table_index,
                    table.clone(),
                    return_types.clone(),
                    return_names.clone(),
                );
                let get = LogicalOperator::LogicalGet(logical_get);
                self.bind_context.add_base_table(
                    alias,
                    table_index,
                    return_types,
                    return_names,
                    CatalogEntry::TableCatalogEntry(table.clone()),
                );
                let bound_tabel_ref =
                    BoundTableRef::BoundBaseTableRef(Box::new(BoundBaseTableRef::new(table, get)));
                Ok(bound_tabel_ref)
            }
            other => Err(BindError::Internal(format!(
                "unexpected table type: {}, only bind TableFactor::Table",
                other
            ))),
        }
    }
}
