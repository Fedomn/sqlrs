mod binder;
mod constants;
mod errors;
mod expression_binder;
mod expression_iterator;
mod logical_operator_visitor;
mod operator;

use std::sync::Arc;

pub use binder::*;
pub use constants::*;
pub use errors::*;
pub use expression_binder::*;
pub use expression_iterator::*;
pub use logical_operator_visitor::*;
pub use operator::*;
use sqlparser::ast::Statement;

use crate::main_entry::ClientContext;
use crate::types_v2::LogicalType;

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
        let bound_statement = self.binder.bind(statement)?;
        self.plan = Some(bound_statement.plan);
        self.names = Some(bound_statement.names);
        self.types = Some(bound_statement.types);
        // println!(
        //     "created_plan: {:#?}\nnames: {:?}\ntypes: {:?}",
        //     self.plan, self.names, self.types
        // );
        Ok(())
    }
}
