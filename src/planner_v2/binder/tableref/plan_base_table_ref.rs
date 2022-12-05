use super::BoundBaseTableRef;
use crate::planner_v2::{BindError, Binder, LogicalOperator};

impl Binder {
    pub fn create_plan_for_base_tabel_ref(
        &mut self,
        bound_ref: BoundBaseTableRef,
    ) -> Result<LogicalOperator, BindError> {
        Ok(bound_ref.get)
    }
}
