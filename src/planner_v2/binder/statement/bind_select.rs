use sqlparser::ast::{Query, Statement};

use super::BoundStatement;
use crate::planner_v2::{BindError, Binder};

impl Binder {
    pub fn bind_query_stmt(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::Query(query) => self.bind_query(query),
            _ => Err(BindError::UnsupportedStmt(format!("{:?}", stmt))),
        }
    }

    pub fn bind_query(&mut self, query: &Query) -> Result<BoundStatement, BindError> {
        let mut node = self.bind_query_body(&query.body)?;
        if let Some(limit_modifier) = self.bind_limit_modifier(query)? {
            node.modifiers.push(limit_modifier);
        }
        self.create_plan_for_select_node(node)
    }
}
