use std::sync::Arc;

use super::util::find_aggregate_exprs;
use super::*;
use crate::binder::BoundSelect;
use crate::optimizer::*;

impl Planner {
    pub fn plan_select(&self, stmt: BoundSelect) -> Result<PlanRef, LogicalPlanError> {
        let mut plan: PlanRef;

        if let Some(table_ref) = stmt.from_table {
            // plan table_ref into LogicalTableScan
            plan = Arc::new(LogicalTableScan::new(
                table_ref.table_catalog.id.clone(),
                table_ref.table_catalog.get_all_columns(),
            ));
        } else {
            todo!("need logical values")
        }

        if let Some(expr) = stmt.where_clause {
            plan = Arc::new(LogicalFilter::new(expr, plan));
        }

        let agg = find_aggregate_exprs(stmt.select_list.as_slice());

        if !agg.is_empty() || !stmt.group_by.is_empty() {
            plan = Arc::new(LogicalAgg::new(agg, stmt.group_by, plan));
        }

        if !stmt.select_list.is_empty() {
            plan = Arc::new(LogicalProject::new(stmt.select_list, plan));
        }

        Ok(plan)
    }
}
