mod join;

use join::*;
use sqlparser::ast::{TableFactor, TableWithJoins};

use super::{BindError, Binder};
use crate::catalog::TableCatalog;

pub static DEFAULT_DATABASE_NAME: &str = "postgres";
pub static DEFAULT_SCHEMA_NAME: &str = "postgres";

#[derive(Debug, Clone)]
pub enum BoundTableRef {
    Table {
        table_catalog: TableCatalog,
    },
    Join {
        relation: Box<BoundTableRef>,
        joins: Vec<Join>,
    },
}

impl Binder {
    pub fn bind_table_with_joins(
        &mut self,
        table_with_joins: &TableWithJoins,
    ) -> Result<BoundTableRef, BindError> {
        let relation = self.bind_table_ref(&table_with_joins.relation)?;
        let mut joins = vec![];
        for join in &table_with_joins.joins {
            let join_table = self.bind_table_ref(&join.relation)?;
            let (join_type, join_condition) = self.bind_join_operator(&join.join_operator)?;
            let join = Join {
                left: Box::new(relation.clone()),
                right: Box::new(join_table),
                join_type,
                join_condition,
            };
            joins.push(join);
        }
        if joins.is_empty() {
            Ok(relation)
        } else {
            Ok(BoundTableRef::Join {
                relation: Box::new(relation),
                joins,
            })
        }
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

                Ok(BoundTableRef::Table { table_catalog })
            }
            _ => panic!("unsupported table factor"),
        }
    }
}
