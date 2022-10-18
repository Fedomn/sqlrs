use crate::binder::{Binder, BoundAlias, BoundExpr, BoundSelect, TableSchema};
use crate::catalog::{ColumnCatalog, TableCatalog, TableId};

#[derive(Clone, Debug, PartialEq)]
pub struct BoundSubquery {
    pub query: Box<BoundSelect>,
    /// subquery always has a alias, if not, we will generate a alias number
    pub alias: TableId,
}

impl BoundSubquery {
    pub fn new(query: Box<BoundSelect>, alias: TableId) -> Self {
        Self { query, alias }
    }

    fn get_output_columns(&self) -> Vec<ColumnCatalog> {
        self.query
            .select_list
            .iter()
            .map(|expr| expr.output_column_catalog_for_alias_table(self.alias.clone()))
            .collect::<Vec<_>>()
    }

    pub fn gen_table_catalog_for_outside_reference(&self) -> TableCatalog {
        let subquery_output_columns = self.get_output_columns();
        TableCatalog::new_from_columns(self.alias.clone(), subquery_output_columns)
    }

    pub fn schema(&self) -> TableSchema {
        TableSchema::new_from_columns(self.get_output_columns())
    }

    pub fn bind_alias_to_all_columns(&mut self) {
        let table_catalog = self.gen_table_catalog_for_outside_reference();
        let column_catalog = table_catalog.get_all_columns();
        let new_subquery_select_list_with_alias = self
            .query
            .select_list
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                let column_catalog = column_catalog[idx].clone();
                BoundExpr::Alias(BoundAlias {
                    expr: Box::new(expr.clone()),
                    column_id: column_catalog.column_id,
                    table_id: column_catalog.table_id,
                })
            })
            .collect::<Vec<_>>();
        self.query.select_list = new_subquery_select_list_with_alias;
    }
}

impl Binder {
    pub fn gen_subquery_table_id(&mut self) -> String {
        let id = format!("subquery_{}", self.context.subquery_base_index);
        self.context.subquery_base_index += 1;
        id
    }
}
