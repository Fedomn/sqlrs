use sqlparser::ast::Statement;

use super::BoundStatement;
use crate::planner_v2::{
    BindError, Binder, ExplainType, LogicalExplain, LogicalOperator, LogicalOperatorBase,
};
use crate::types_v2::LogicalType;
use crate::util::tree_render::TreeRender;

impl Binder {
    pub fn bind_explain(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::Explain {
                statement, analyze, ..
            } => {
                let bound_stmt = self.bind(statement)?;
                let explain_type = if *analyze {
                    ExplainType::ANALYZE
                } else {
                    ExplainType::STANDARD
                };

                let types = vec![LogicalType::Varchar, LogicalType::Varchar];
                let names = vec!["explain_type".to_string(), "explain_value".to_string()];
                let logical_plan_string = TreeRender::logical_plan_tree(&bound_stmt.plan);
                let base = LogicalOperatorBase::new(vec![bound_stmt.plan], vec![], vec![]);
                let logical_explain = LogicalExplain::new(base, explain_type, logical_plan_string);
                let new_plan = LogicalOperator::LogicalExplain(logical_explain);
                Ok(BoundStatement::new(new_plan, types, names))
            }
            _ => Err(BindError::UnsupportedStmt(format!("{:?}", stmt))),
        }
    }
}
