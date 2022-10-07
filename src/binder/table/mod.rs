mod join;

pub use join::*;
use sqlparser::ast::{TableFactor, TableWithJoins};

use super::{BindError, Binder, BoundSelect};
use crate::catalog::{ColumnCatalog, ColumnId, TableCatalog, TableId};

pub static DEFAULT_DATABASE_NAME: &str = "postgres";
pub static DEFAULT_SCHEMA_NAME: &str = "postgres";

#[derive(Debug, Clone, PartialEq)]
pub enum BoundTableRef {
    Table(TableCatalog),
    Join(Join),
    Subquery(Box<BoundSelect>),
}

impl BoundTableRef {
    pub fn schema(&self) -> TableSchema {
        match self {
            BoundTableRef::Table(catalog) => TableSchema::new(catalog.clone()),
            BoundTableRef::Join(join) => {
                TableSchema::new_from_join(&join.left.schema(), &join.right.schema())
            }
            BoundTableRef::Subquery(subquery) => subquery.from_table.clone().unwrap().schema(),
        }
    }
}

/// used for extract_join_keys method to reorder join keys
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub columns: Vec<(TableId, ColumnId)>,
}

impl TableSchema {
    pub fn new(table_catalog: TableCatalog) -> Self {
        Self {
            columns: table_catalog
                .get_all_columns()
                .into_iter()
                .map(|c| (c.table_id, c.column_id))
                .collect(),
        }
    }

    pub fn new_from_join(left: &TableSchema, right: &TableSchema) -> Self {
        let mut left_cols = left.columns.clone();
        let right_cols = right.columns.clone();
        left_cols.extend(right_cols);
        Self { columns: left_cols }
    }

    pub fn contains_key(&self, col: &ColumnCatalog) -> bool {
        self.columns
            .iter()
            .any(|(t_id, c_id)| *t_id == col.table_id && *c_id == col.column_id)
    }
}

impl Binder {
    pub fn bind_table_with_joins(
        &mut self,
        table_with_joins: &TableWithJoins,
    ) -> Result<BoundTableRef, BindError> {
        let left = self.bind_table_ref(&table_with_joins.relation)?;
        if table_with_joins.joins.is_empty() {
            return Ok(left);
        }

        let mut new_left = left;
        // use left-deep to construct multiple joins
        // join ordering refer to: https://www.cockroachlabs.com/blog/join-ordering-pt1/
        for join in &table_with_joins.joins {
            let right = self.bind_table_ref(&join.relation)?;
            let (join_type, join_condition) =
                self.bind_join_operator(&new_left.schema(), &right.schema(), &join.join_operator)?;
            new_left = BoundTableRef::Join(Join {
                left: Box::new(new_left),
                right: Box::new(right),
                join_type,
                join_condition,
            });
        }
        Ok(new_left)
    }

    pub fn bind_table_ref(&mut self, table: &TableFactor) -> Result<BoundTableRef, BindError> {
        match table {
            TableFactor::Table { name, alias, .. } => {
                // ObjectName internal items: db.schema.table
                let (_database, _schema, table) = match name.0.as_slice() {
                    [table] => (
                        DEFAULT_DATABASE_NAME,
                        DEFAULT_SCHEMA_NAME,
                        table.value.as_str(),
                    ),
                    [schema, table] => (
                        DEFAULT_DATABASE_NAME,
                        schema.value.as_str(),
                        table.value.as_str(),
                    ),
                    [db, schema, table] => (
                        db.value.as_str(),
                        schema.value.as_str(),
                        table.value.as_str(),
                    ),
                    _ => return Err(BindError::InvalidTable(name.to_string())),
                };

                let table_name = table.to_string();
                let table_catalog = self
                    .catalog
                    .get_table_by_name(table)
                    .ok_or_else(|| BindError::InvalidTable(table_name.clone()))?;
                if let Some(alias) = alias {
                    let table_alias = alias.to_string().to_lowercase();
                    self.context
                        .tables
                        .insert(table_alias, table_catalog.clone());
                } else {
                    self.context
                        .tables
                        .insert(table_name, table_catalog.clone());
                }
                Ok(BoundTableRef::Table(table_catalog))
            }
            TableFactor::Derived {
                lateral: _,
                subquery,
                alias,
            } => {
                let table = self.bind_select(subquery)?;
                if let Some(alias) = alias {
                    todo!("alias for subquery {}", alias)
                }
                Ok(BoundTableRef::Subquery(Box::new(table)))
            }
            _other => panic!("unsupported table factor: {:?}", _other),
        }
    }
}
