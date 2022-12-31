use itertools::Itertools;
use sqlparser::ast::Statement;

use super::BoundStatement;
use crate::catalog_v2::ColumnDefinition;
use crate::common::{CreateInfoBase, CreateTableInfo};
use crate::planner_v2::{
    BindError, Binder, LogicalCreateTable, LogicalOperator, SqlparserResolver,
};
use crate::types_v2::LogicalType;

impl Binder {
    pub fn bind_create_table(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::CreateTable {
                name,
                columns,
                query,
                ..
            } => {
                let (schema, table) = SqlparserResolver::object_name_to_schema_table(name)?;
                let (column_definitions, query) = if let Some(query) = query {
                    // create table columns baesd on query names and types
                    let select = self.bind_query(query)?;
                    let cols = select
                        .names
                        .into_iter()
                        .zip_eq(select.types.into_iter())
                        .map(|(name, ty)| ColumnDefinition::new(name, ty))
                        .collect::<Vec<_>>();
                    (cols, Some(Box::new(select.plan)))
                } else {
                    // create table columns based on input column_def
                    let cols = columns
                        .iter()
                        .map(SqlparserResolver::column_def_to_column_definition)
                        .try_collect()?;
                    (cols, None)
                };
                let bound_info =
                    BoundCreateTableInfo::new(schema, table, column_definitions, query);
                let plan = LogicalOperator::LogicalCreateTable(LogicalCreateTable::new(bound_info));
                Ok(BoundStatement::new(
                    plan,
                    vec![LogicalType::Varchar],
                    vec!["success".to_string()],
                ))
            }
            _ => Err(BindError::UnsupportedStmt(format!("{:?}", stmt))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BoundCreateTableInfo {
    pub(crate) base: CreateTableInfo,
    /// CREATE TABLE from QUERY
    pub(crate) query: Option<Box<LogicalOperator>>,
}

impl BoundCreateTableInfo {
    pub fn new(
        schema: String,
        table: String,
        column_definitions: Vec<ColumnDefinition>,
        query: Option<Box<LogicalOperator>>,
    ) -> Self {
        let base = CreateInfoBase::new(schema);
        let create_table_info = CreateTableInfo::new(base, table, column_definitions);
        Self {
            base: create_table_info,
            query,
        }
    }

    pub fn new_create_as_query(base: CreateTableInfo, query: LogicalOperator) -> Self {
        Self {
            base,
            query: Some(Box::new(query)),
        }
    }
}
