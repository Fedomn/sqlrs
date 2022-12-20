mod bind_create;
mod bind_explain;
mod bind_insert;
mod bind_select;
mod bind_show_tables;

pub use bind_create::*;
pub use bind_insert::*;
pub use bind_select::*;
pub use bind_show_tables::*;
use derive_new::new;
use sqlparser::ast::Statement;

use super::{BindError, Binder};
use crate::planner_v2::LogicalOperator;
use crate::types_v2::LogicalType;

#[derive(new, Debug)]
pub struct BoundStatement {
    pub(crate) plan: LogicalOperator,
    pub(crate) types: Vec<LogicalType>,
    pub(crate) names: Vec<String>,
}

impl Binder {
    pub fn bind(&mut self, statement: &Statement) -> Result<BoundStatement, BindError> {
        match statement {
            Statement::CreateTable { .. } => self.bind_create_table(statement),
            Statement::Insert { .. } => self.bind_insert(statement),
            Statement::Query { .. } => self.bind_select(statement),
            Statement::Explain { .. } => self.bind_explain(statement),
            Statement::ShowTables { .. } => self.bind_show_tables(statement),
            _ => Err(BindError::UnsupportedStmt(format!("{:?}", statement))),
        }
    }
}
