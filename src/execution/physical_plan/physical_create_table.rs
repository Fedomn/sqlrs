use derive_new::new;

use super::{PhysicalInsert, PhysicalOperator, PhysicalOperatorBase};
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::{BoundCreateTableInfo, LogicalCreateTable};

#[derive(new, Clone)]
pub struct PhysicalCreateTable {
    #[new(default)]
    pub(crate) base: PhysicalOperatorBase,
    pub(crate) info: BoundCreateTableInfo,
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_create_table(&self, op: LogicalCreateTable) -> PhysicalOperator {
        if let Some(query) = op.info.query.clone() {
            // create table as select
            let query_plan = self.create_plan(*query);
            let base = PhysicalOperatorBase::new(vec![query_plan], vec![]);
            PhysicalOperator::PhysicalInsert(Box::new(PhysicalInsert::new_create_table_as(
                base, op.info,
            )))
        } else {
            // create table
            PhysicalOperator::PhysicalCreateTable(PhysicalCreateTable::new(op.info))
        }
    }
}
