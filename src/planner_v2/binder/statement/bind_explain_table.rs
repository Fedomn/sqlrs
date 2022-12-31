use sqlparser::ast::Statement;

use super::BoundStatement;
use crate::planner_v2::{
    BindError, Binder, SqlparserQueryBuilder, SqlparserResolver, SqlparserSelectBuilder,
};

impl Binder {
    pub fn bind_explain_table(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::ExplainTable {
                describe_alias,
                table_name,
                ..
            } => {
                if !*describe_alias {
                    return Err(BindError::UnsupportedStmt(
                        "Only support describe table statement".to_string(),
                    ));
                }
                let (_, table_name) = SqlparserResolver::object_name_to_schema_table(table_name)?;
                let select = SqlparserSelectBuilder::default()
                    .projection_wildcard()
                    .from_table_function("sqlrs_columns")
                    .selection_col_eq_string("table_name", table_name.as_str())
                    .build();
                let query = SqlparserQueryBuilder::new_from_select(select).build();
                self.bind_query(&query)
            }
            _ => Err(BindError::UnsupportedStmt(format!("{:?}", stmt))),
        }
    }
}
