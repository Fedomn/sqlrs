use std::sync::Arc;

use derive_new::new;
use log::debug;

use super::{ColumnBindingResolver, PhysicalOperator, PhysicalOperatorBase};
use crate::execution::LOGGING_TARGET;
use crate::main_entry::ClientContext;
use crate::planner_v2::{LogicalOperator, LogicalOperatorBase, LogicalOperatorVisitor};
use crate::util::tree_render::TreeRender;

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
        let plan = self.create_plan_internal(op);
        debug!(
            target: LOGGING_TARGET,
            "Physical Plan:\n{}",
            TreeRender::physical_plan_tree(&plan),
        );
        plan
    }

    pub(crate) fn create_plan_internal(&self, op: LogicalOperator) -> PhysicalOperator {
        match op {
            LogicalOperator::LogicalCreateTable(op) => self.create_physical_create_table(op),
            LogicalOperator::LogicalExpressionGet(op) => self.create_physical_expression_scan(op),
            LogicalOperator::LogicalInsert(op) => self.create_physical_insert(op),
            LogicalOperator::LogicalGet(op) => self.create_physical_table_scan(op),
            LogicalOperator::LogicalProjection(op) => self.create_physical_projection(op),
            LogicalOperator::LogicalDummyScan(op) => self.create_physical_dummy_scan(op),
            LogicalOperator::LogicalExplain(op) => self.create_physical_explain(op),
            LogicalOperator::LogicalFilter(op) => self.create_physical_filter(op),
            LogicalOperator::LogicalLimit(op) => self.create_physical_limit(op),
        }
    }

    pub(crate) fn create_physical_operator_base(
        &self,
        base: LogicalOperatorBase,
    ) -> PhysicalOperatorBase {
        let children = base
            .children
            .into_iter()
            .map(|op| self.create_plan_internal(op))
            .collect::<Vec<_>>();
        PhysicalOperatorBase::new(children, base.expressioins)
    }
}
