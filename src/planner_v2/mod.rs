mod binder;
mod constants;
mod errors;
mod expression_binder;
mod expression_iterator;
mod function_binder;
mod logical_operator_visitor;
mod operator;

use std::sync::Arc;

pub use binder::*;
pub use constants::*;
pub use errors::*;
pub use expression_binder::*;
pub use expression_iterator::*;
pub use function_binder::*;
use log::debug;
pub use logical_operator_visitor::*;
pub use operator::*;
use sqlparser::ast::Statement;

use crate::main_entry::ClientContext;
use crate::types_v2::LogicalType;
use crate::util::tree_render::TreeRender;

static LOGGING_TARGET: &str = "sqlrs::planner";

pub struct Planner {
    binder: Binder,
    #[allow(dead_code)]
    client_context: Arc<ClientContext>,
    pub(crate) plan: Option<LogicalOperator>,
    pub(crate) types: Option<Vec<LogicalType>>,
    pub(crate) names: Option<Vec<String>>,
}

impl Planner {
    pub fn new(client_context: Arc<ClientContext>) -> Self {
        Self {
            binder: Binder::new(client_context.clone()),
            client_context,
            plan: None,
            types: None,
            names: None,
        }
    }

    pub fn create_plan(&mut self, statement: &Statement) -> Result<(), PlannerError> {
        debug!(
            target: LOGGING_TARGET,
            "Planner raw statement: {:?}", statement
        );

        let bound_statement = self.binder.bind(statement)?;

        debug!(
            target: LOGGING_TARGET,
            r#"Planner bound_statement:
names: {:?}
types: {:?}
plan:
{}"#,
            bound_statement.names,
            bound_statement.types,
            TreeRender::logical_plan_tree(&bound_statement.plan),
        );
        self.plan = Some(bound_statement.plan);
        self.names = Some(bound_statement.names);
        self.types = Some(bound_statement.types);
        Ok(())
    }
}
