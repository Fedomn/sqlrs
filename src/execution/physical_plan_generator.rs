use std::sync::Arc;

use derive_new::new;

use super::{ColumnBindingResolver, PhysicalOperator};
use crate::main_entry::ClientContext;
use crate::planner_v2::{LogicalOperator, LogicalOperatorVisitor};

#[derive(new)]
pub struct PhysicalPlanGenerator {
    pub(crate) _client_context: Arc<ClientContext>,
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_plan(&self, mut op: LogicalOperator) -> PhysicalOperator {
        // first resolve column references
        let mut resolver = ColumnBindingResolver::default();
        resolver.visit_operator(&mut op);

        // now resolve types of all the operators
        op.resolve_operator_types();

        // then create the main physical plan
        self.create_plan_internal(op)
    }

    pub(crate) fn create_plan_internal(&self, op: LogicalOperator) -> PhysicalOperator {
        match op {
            LogicalOperator::LogicalCreateTable(op) => self.create_physical_create_table(op),
            LogicalOperator::LogicalExpressionGet(op) => self.create_physical_expression_scan(op),
            LogicalOperator::LogicalInsert(op) => self.create_physical_insert(op),
            LogicalOperator::LogicalGet(op) => self.create_physical_table_scan(op),
            LogicalOperator::LogicalProjection(op) => self.create_physical_projection(op),
            LogicalOperator::LogicalDummyScan(op) => self.create_physical_dummy_scan(op),
        }
    }
}
