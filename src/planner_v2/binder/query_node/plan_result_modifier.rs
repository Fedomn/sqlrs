use super::BoundResultModifier;
use crate::planner_v2::{BindError, Binder, LogicalLimit, LogicalOperator, LogicalOperatorBase};

impl Binder {
    pub fn plan_for_result_modifiers(
        &mut self,
        result_modifiers: Vec<BoundResultModifier>,
        root: LogicalOperator,
    ) -> Result<LogicalOperator, BindError> {
        let mut root_op = root;
        for modifier in result_modifiers.into_iter() {
            match modifier {
                BoundResultModifier::BoundLimitModifier(limit) => {
                    let mut op = LogicalOperator::LogicalLimit(LogicalLimit::new(
                        LogicalOperatorBase::default(),
                        limit.limit_value,
                        limit.offsert_value,
                        limit.limit,
                        limit.offset,
                    ));
                    op.add_child(root_op);
                    root_op = op;
                }
            }
        }
        Ok(root_op)
    }
}
