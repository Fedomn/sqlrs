use super::BoundExpressionListRef;
use crate::planner_v2::{
    BindError, Binder, LogicalDummyScan, LogicalExpressionGet, LogicalOperator, LogicalOperatorBase,
};

impl Binder {
    pub fn create_plan_for_expression_list_ref(
        &mut self,
        bound_ref: BoundExpressionListRef,
    ) -> Result<LogicalOperator, BindError> {
        let table_idx = bound_ref.bind_index;
        let base = LogicalOperatorBase::new(
            vec![LogicalOperator::LogicalDummyScan(LogicalDummyScan::new(
                self.generate_table_index(),
            ))],
            vec![],
            vec![],
        );
        let plan = LogicalExpressionGet::new(base, table_idx, bound_ref.types, bound_ref.values);
        Ok(LogicalOperator::LogicalExpressionGet(plan))
    }
}
