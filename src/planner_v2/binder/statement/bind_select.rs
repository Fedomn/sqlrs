use sqlparser::ast::Statement;

use super::BoundStatement;
use crate::planner_v2::{BindError, Binder};

impl Binder {
    pub fn bind_select(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::Query(query) => {
                let mut node = self.bind_select_node(query)?;
                if let Some(limit_modifier) = self.bind_limit_modifier(query)? {
                    node.modifiers.push(limit_modifier);
                }
                self.create_plan_for_select_node(node)
            }
            _ => Err(BindError::UnsupportedStmt(format!("{:?}", stmt))),
        }
    }
}
