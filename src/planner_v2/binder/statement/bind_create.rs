use sqlparser::ast::Statement;

use super::{BoundStatement, CreateTableInfo};
use crate::catalog_v2::ColumnDefinition;
use crate::planner_v2::{
    BindError, Binder, CreateInfoBase, LogicalCreateTable, LogicalOperator, SqlparserResolver,
};
use crate::types_v2::LogicalType;

impl Binder {
    pub fn bind_create_table(&self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => {
                let (schema, table) = SqlparserResolver::object_name_to_schema_table(name)?;
                let column_definitions = columns
                    .iter()
                    .map(SqlparserResolver::column_def_to_column_definition)
                    .try_collect()?;
                let bound_info = BoundCreateTableInfo::new(schema, table, column_definitions);
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
}

impl BoundCreateTableInfo {
    pub fn new(schema: String, table: String, column_definitions: Vec<ColumnDefinition>) -> Self {
        let base = CreateInfoBase::new(schema);
        let create_table_info = CreateTableInfo::new(base, table, column_definitions);
        Self {
            base: create_table_info,
        }
    }
}
