mod join;
mod subquery;

use std::fmt::{self};

pub use join::*;
use sqlparser::ast::{TableFactor, TableWithJoins};
pub use subquery::*;

use super::{BindError, Binder, BinderContext};
use crate::catalog::{ColumnCatalog, ColumnId, TableCatalog, TableId};

pub static DEFAULT_DATABASE_NAME: &str = "postgres";
pub static DEFAULT_SCHEMA_NAME: &str = "postgres";
pub static EMPTY_DATABASE_ID: &str = "empty-database-id";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BoundTableRef {
    Table(BoundSimpleTable),
    Join(Join),
    Subquery(BoundSubqueryRef),
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BoundSimpleTable {
    pub catalog: TableCatalog,
    pub alias: Option<TableId>,
}

impl BoundSimpleTable {
    pub fn new(catalog: TableCatalog, alias: Option<TableId>) -> Self {
        Self { catalog, alias }
    }

    pub fn table_id(&self) -> TableId {
        self.alias
            .clone()
            .unwrap_or_else(|| self.catalog.id.clone())
    }

    pub fn schema(&self) -> TableSchema {
        let table_id = self.table_id();
        let columns = self
            .catalog
            .get_all_columns()
            .into_iter()
            .map(|c| (table_id.clone(), c.column_id))
            .collect();
        TableSchema { columns }
    }
}

impl BoundTableRef {
    pub fn schema(&self) -> TableSchema {
        match self {
            BoundTableRef::Table(table) => table.schema(),
            BoundTableRef::Join(join) => {
                TableSchema::new_from_join(&join.left.schema(), &join.right.schema())
            }
            BoundTableRef::Subquery(subquery) => subquery.schema(),
        }
    }

    /// Bound table id, if table alias exists, use alias as id
    pub fn bound_table_id(&self) -> TableId {
        match self {
            BoundTableRef::Table(table) => table.table_id(),
            BoundTableRef::Join(join) => join.left.bound_table_id(),
            BoundTableRef::Subquery(subquery) => subquery.alias.clone(),
        }
    }
}

/// used for extract_join_keys method to reorder join keys
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub columns: Vec<(TableId, ColumnId)>,
}

impl TableSchema {
    pub fn new_from_columns(columns: Vec<ColumnCatalog>) -> Self {
        Self {
            columns: columns
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
                let mut table_catalog = self
                    .catalog
                    .get_table_by_name(table)
                    .ok_or_else(|| BindError::InvalidTable(table_name.clone()))?;
                let mut table_alias = None;
                if let Some(alias) = alias {
                    // add alias table in table catalog for later column binding
                    // such as: select sum(t.a) as c1 from t1 as t
                    let table_alias_str = alias.to_string().to_lowercase();
                    // we only change column's table_id to table_alias, keep original real table_id
                    // for storage layer lookup corresponding file
                    table_catalog =
                        table_catalog.clone_with_new_column_table_id(table_alias_str.clone());
                    self.context
                        .tables
                        .insert(table_alias_str.clone(), table_catalog.clone());
                    table_alias = Some(table_alias_str);
                } else {
                    self.context
                        .tables
                        .insert(table_name, table_catalog.clone());
                }
                Ok(BoundTableRef::Table(BoundSimpleTable::new(
                    table_catalog,
                    table_alias,
                )))
            }
            TableFactor::Derived {
                lateral: _,
                subquery,
                alias,
            } => {
                // handle subquery as source
                // such as: (select max(b) as v1 from t1) in following sql
                // select a, t2.v1 as max_b from t1 cross join (select max(b) as v1 from t1) t2;
                let mut upper_context = self.context.clone();
                // generate a new context for nested query as parent context
                self.context = BinderContext::new();
                let query = self.bind_select(subquery)?;
                upper_context.parent = Some(Box::new(self.context.clone()));
                // reset context to upper context
                self.context = upper_context;

                let alias = alias
                    .clone()
                    .map(|a| a.to_string().to_lowercase())
                    .ok_or(BindError::SubqueryMustHaveAlias)?;
                let mut subquery = BoundSubqueryRef::new(Box::new(query), alias.clone());

                // add subquery output columns into context
                let subquery_catalog = subquery.gen_table_catalog_for_outside_reference();
                self.context.tables.insert(alias, subquery_catalog);

                // add BoundAlias for all subquery columns
                subquery.bind_alias_to_all_columns();

                Ok(BoundTableRef::Subquery(subquery))
            }
            _other => panic!("unsupported table factor: {:?}", _other),
        }
    }
}

impl fmt::Debug for BoundSimpleTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let alias = if let Some(alias) = &self.alias {
            format!(" as {}", alias)
        } else {
            "".to_string()
        };
        write!(f, r#"{:?}{}"#, self.catalog, alias)
    }
}
