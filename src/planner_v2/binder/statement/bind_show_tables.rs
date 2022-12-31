use sqlparser::ast::Statement;

use super::BoundStatement;
use crate::planner_v2::{BindError, Binder, SqlparserQueryBuilder, SqlparserSelectBuilder};

impl Binder {
    pub fn bind_show_tables(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::ShowTables { .. } => {
                let select = SqlparserSelectBuilder::default()
                    .projection_cols(vec!["schema_name", "table_name"])
                    .from_table_function("sqlrs_tables")
                    .build();
                let query = SqlparserQueryBuilder::new_from_select(select).build();
                self.bind_query(&query)
            }
            _ => Err(BindError::UnsupportedStmt(format!("{:?}", stmt))),
        }
    }
}
