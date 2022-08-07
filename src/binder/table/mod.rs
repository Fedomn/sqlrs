mod join;

pub use join::*;
use sqlparser::ast::{TableFactor, TableWithJoins};

use super::{BindError, Binder};
use crate::catalog::TableCatalog;

pub static DEFAULT_DATABASE_NAME: &str = "postgres";
pub static DEFAULT_SCHEMA_NAME: &str = "postgres";

#[derive(Debug, Clone, PartialEq)]
pub enum BoundTableRef {
    Table(TableCatalog),
    Join(Join),
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
            let (join_type, join_condition) = self.bind_join_operator(&join.join_operator)?;
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
            TableFactor::Table { name, alias: _, .. } => {
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
                self.context
                    .tables
                    .insert(table_name, table_catalog.clone());

                Ok(BoundTableRef::Table(table_catalog))
            }
            _ => panic!("unsupported table factor"),
        }
    }
}
